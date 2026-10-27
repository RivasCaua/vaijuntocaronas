package main

import (
	"fmt"
	"net"
	"time"

	"vaijunto/pkg/protocolo"
)

// baselineNo guarda os fatos de dominio reais criados em um no antes da injecao de falha,
// junto das credenciais necessarias para reautenticar e reconferir o mesmo estado mais tarde.
type baselineNo struct {
	NodoID          string
	CaronaID        string
	Origem, Destino string
	AssentosTotais  int
	VagasRestantes  int
	MotoristaLogin  string
	MotoristaSenha  string
	PassageiroLogin string
	PassageiroSenha string
	ReservaID       string
}

// rotaBaseline define uma rota fixa e valida do catalogo regional para cada empresa do consorcio,
// encadeada de forma que nodo-salvador e nodo-feira compartilham a cidade "Amelia Rodrigues",
// permitindo montar itinerarios combinados cross-no realistas na auditoria do 2PC (FASE 6).
func rotaBaseline(nodoID string) (origem, destino string) {
	switch nodoID {
	case "nodo-salvador":
		return "Salvador", "Amélia Rodrigues"
	case "nodo-feira":
		return "Amélia Rodrigues", "Feira de Santana"
	case "nodo-reconcavo":
		return "Cachoeira", "Santo Antônio de Jesus"
	default:
		return "Salvador", "Feira de Santana"
	}
}

// gerarLinhaDeBase cadastra um motorista e um passageiro reais no no informado, publica uma
// carona e efetiva uma reserva de verdade via TCP, deixando o WAL daquele processo com entradas
// reais de CADASTRAR_USUARIO, PUBLICAR_CARONA e RESERVAR para a auditoria de recuperacao.
func gerarLinhaDeBase(spec nodeSpec, data string) (*baselineNo, error) {
	endereco := fmt.Sprintf("127.0.0.1:%d", spec.PortaTCP)
	conn, err := net.DialTimeout("tcp", endereco, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("falha ao conectar em %s: %w", endereco, err)
	}
	defer conn.Close()

	scanner := protocolo.CriarScanner(conn)
	origem, destino := rotaBaseline(spec.ID)
	agora := time.Now().UnixNano()

	bl := &baselineNo{
		NodoID:          spec.ID,
		Origem:          origem,
		Destino:         destino,
		AssentosTotais:  3,
		MotoristaLogin:  fmt.Sprintf("mot_base_%s_%d", spec.ID, agora),
		MotoristaSenha:  "123",
		PassageiroLogin: fmt.Sprintf("pass_base_%s_%d", spec.ID, agora),
		PassageiroSenha: "123",
	}

	// 1. Cadastra e autentica o motorista
	if _, err := enviarRecv(conn, scanner, protocolo.Requisicao{
		Acao: protocolo.AcaoCadastrar, Usuario: bl.MotoristaLogin, Senha: bl.MotoristaSenha,
		Nome: "Motorista Baseline " + spec.ID, Papel: protocolo.PapelMotorista,
	}); err != nil {
		return nil, fmt.Errorf("cadastro de motorista falhou: %w", err)
	}

	respAuthMot, err := enviarRecv(conn, scanner, protocolo.Requisicao{
		Acao: protocolo.AcaoAutenticar, Usuario: bl.MotoristaLogin, Senha: bl.MotoristaSenha,
	})
	if err != nil {
		return nil, fmt.Errorf("autenticacao de motorista falhou: %w", err)
	}
	tokenMot := respAuthMot.Token

	// 2. Publica a carona real (gera registro PUBLICAR_CARONA no WAL)
	respCarona, err := enviarRecv(conn, scanner, protocolo.Requisicao{
		Acao: protocolo.AcaoPublicarCarona, Token: tokenMot, Rota: []string{origem, destino},
		Data: data, Horario: "08:00", HorarioChegada: "09:00", Assentos: bl.AssentosTotais, PrecoPorTrecho: 20.0,
	})
	if err != nil {
		return nil, fmt.Errorf("publicacao de carona falhou: %w", err)
	}
	bl.CaronaID = respCarona.CaronaID

	// 3. Cadastra e autentica o passageiro
	if _, err := enviarRecv(conn, scanner, protocolo.Requisicao{
		Acao: protocolo.AcaoCadastrar, Usuario: bl.PassageiroLogin, Senha: bl.PassageiroSenha,
		Nome: "Passageiro Baseline " + spec.ID, Papel: protocolo.PapelPassageiro,
	}); err != nil {
		return nil, fmt.Errorf("cadastro de passageiro falhou: %w", err)
	}

	respAuthPass, err := enviarRecv(conn, scanner, protocolo.Requisicao{
		Acao: protocolo.AcaoAutenticar, Usuario: bl.PassageiroLogin, Senha: bl.PassageiroSenha,
	})
	if err != nil {
		return nil, fmt.Errorf("autenticacao de passageiro falhou: %w", err)
	}
	tokenPass := respAuthPass.Token

	// 4. Busca e reserva de verdade (gera registro RESERVAR no WAL)
	respBusca, err := enviarRecv(conn, scanner, protocolo.Requisicao{
		Acao: protocolo.AcaoBuscarItinerarios, Origem: origem, Destino: destino, Data: data,
	})
	if err != nil || len(respBusca.Itinerarios) == 0 {
		return nil, fmt.Errorf("busca de itinerarios da linha de base falhou ou retornou vazia: %v", err)
	}

	respReserva, err := enviarRecv(conn, scanner, protocolo.Requisicao{
		Acao: protocolo.AcaoReservar, Token: tokenPass, Trechos: respBusca.Itinerarios[0].Trechos,
	})
	if err != nil {
		return nil, fmt.Errorf("reserva da linha de base falhou: %w", err)
	}
	bl.ReservaID = respReserva.ReservaID

	// 5. Confirma as vagas restantes do ponto de vista do motorista (prova do debito de inventario)
	vagas, err := obterVagasTrecho(conn, scanner, tokenMot, bl.CaronaID, origem, destino)
	if err != nil {
		return nil, fmt.Errorf("falha ao consultar vagas pos-reserva: %w", err)
	}
	bl.VagasRestantes = vagas

	return bl, nil
}

// obterVagasTrecho consulta, via LISTAR_CARONAS, quantas vagas livres restam em um sub-trecho especifico.
func obterVagasTrecho(conn net.Conn, scanner *scannerT, token, caronaID, origem, destino string) (int, error) {
	resp, err := enviarRecv(conn, scanner, protocolo.Requisicao{Acao: protocolo.AcaoListarCaronas, Token: token})
	if err != nil {
		return 0, err
	}

	chave := origem + "->" + destino
	for _, c := range resp.Caronas {
		if c.CaronaID != caronaID {
			continue
		}
		for _, t := range c.Trechos {
			if t.Trecho == chave {
				return t.VagasLivres, nil
			}
		}
	}
	return 0, fmt.Errorf("carona %s ou trecho %s nao encontrado na resposta de LISTAR_CARONAS", caronaID, chave)
}

// verificarIntegridadePosRecuperacao reconecta ao no recem-reiniciado e confirma, usando as MESMAS
// credenciais da linha de base, que o usuario, a carona e a reserva sobreviveram exatamente como
// estavam no instante da queda - nem perdidos, nem reaplicados em duplicidade pelo replay do WAL.
func verificarIntegridadePosRecuperacao(spec nodeSpec, bl *baselineNo) (bool, []string) {
	var detalhes []string
	tudoOK := true

	endereco := fmt.Sprintf("127.0.0.1:%d", spec.PortaTCP)
	conn, err := net.DialTimeout("tcp", endereco, 5*time.Second)
	if err != nil {
		return false, []string{fmt.Sprintf("FALHA: nao foi possivel reconectar ao no recuperado: %v", err)}
	}
	defer conn.Close()
	scanner := protocolo.CriarScanner(conn)

	// 1. O usuario passageiro sobrevive ao replay do WAL (CADASTRAR_USUARIO reaplicado)?
	respAuthPass, err := enviarRecv(conn, scanner, protocolo.Requisicao{
		Acao: protocolo.AcaoAutenticar, Usuario: bl.PassageiroLogin, Senha: bl.PassageiroSenha,
	})
	if err != nil {
		return false, []string{fmt.Sprintf("FALHA: passageiro pre-queda nao conseguiu reautenticar apos recuperacao: %v", err)}
	}
	detalhes = append(detalhes, "PASS: usuario passageiro reconstruido via replay do WAL e reautenticado com sucesso")

	// 2. A reserva pre-queda continua ATIVA e com o mesmo ID (nem perdida, nem duplicada)?
	respReservas, err := enviarRecv(conn, scanner, protocolo.Requisicao{
		Acao: protocolo.AcaoListarReservas, Token: respAuthPass.Token,
	})
	if err != nil {
		tudoOK = false
		detalhes = append(detalhes, fmt.Sprintf("FALHA: nao foi possivel listar reservas apos recuperacao: %v", err))
	} else {
		encontrada := false
		for _, r := range respReservas.Reservas {
			if r.ReservaID == bl.ReservaID {
				encontrada = true
				if r.Status != "ATIVA" {
					tudoOK = false
					detalhes = append(detalhes, fmt.Sprintf("FALHA: reserva %s recuperada com status inesperado '%s' (esperado ATIVA)", bl.ReservaID, r.Status))
				} else {
					detalhes = append(detalhes, fmt.Sprintf("PASS: reserva pre-queda %s presente e ATIVA apos recuperacao via WAL", bl.ReservaID))
				}
				break
			}
		}
		if !encontrada {
			tudoOK = false
			detalhes = append(detalhes, fmt.Sprintf("FALHA: reserva pre-queda %s nao foi reconstruida pelo replay do WAL (dado perdido)", bl.ReservaID))
		}
	}

	// 3. O motorista sobrevive e o inventario de vagas bate EXATAMENTE com o valor pre-queda?
	respAuthMot, err := enviarRecv(conn, scanner, protocolo.Requisicao{
		Acao: protocolo.AcaoAutenticar, Usuario: bl.MotoristaLogin, Senha: bl.MotoristaSenha,
	})
	if err != nil {
		tudoOK = false
		detalhes = append(detalhes, fmt.Sprintf("FALHA: motorista pre-queda nao conseguiu reautenticar apos recuperacao: %v", err))
		return tudoOK, detalhes
	}

	vagas, err := obterVagasTrecho(conn, scanner, respAuthMot.Token, bl.CaronaID, bl.Origem, bl.Destino)
	if err != nil {
		tudoOK = false
		detalhes = append(detalhes, fmt.Sprintf("FALHA: nao foi possivel consultar vagas pos-recuperacao: %v", err))
	} else if vagas != bl.VagasRestantes {
		tudoOK = false
		detalhes = append(detalhes, fmt.Sprintf("FALHA: vagas do trecho divergem apos recuperacao (esperado=%d obtido=%d) - possivel perda ou duplicacao de dados", bl.VagasRestantes, vagas))
	} else {
		detalhes = append(detalhes, fmt.Sprintf("PASS: inventario de vagas identico ao pre-queda (%d vagas restantes no trecho %s->%s)", vagas, bl.Origem, bl.Destino))
	}

	return tudoOK, detalhes
}
