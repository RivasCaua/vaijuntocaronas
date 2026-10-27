package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"vaijunto/pkg/consorcio"
	"vaijunto/pkg/protocolo"
)

// Resultado2PC consolida o veredito da auditoria de compensacao de falhas do protocolo 2PC
// quando um dos participantes de uma transacao combinada esta fora do ar.
type Resultado2PC struct {
	Sucesso             bool
	AbortouComoEsperado bool
	CompensacaoOK       bool
	VagasAntes          int
	VagasDepois         int
	ErroTransacao       string
	Resumo              string
}

// auditar2PCComParticipanteCaido monta, com um CoordenadorDistribuido de teste (sem WAL e sem
// gerenciador proprio - um orquestrador puramente de rede), uma reserva combinada cujos dois
// sub-trechos pertencem a um no sobrevivente e ao no recem-morto. Como o protocolo exige voto SIM
// de TODOS os participantes, o Prepare contra o no morto falha por conexao recusada; o coordenador
// deve entao abortar globalmente e compensar (devolver a vaga) no no sobrevivente que havia votado
// SIM. E exatamente o cenario que pkg/consorcio/coordenador_test.go valida em memoria - aqui ele e
// reproduzido contra processos de sistema operacional reais e um no de verdade que acabou de cair.
func auditar2PCComParticipanteCaido(especificacoes []nodeSpec, baselines map[string]*baselineNo, noAlvo string) Resultado2PC {
	var sobreviventeID string
	for _, e := range especificacoes {
		if e.ID != noAlvo {
			sobreviventeID = e.ID
			break
		}
	}

	blSobrevivente, okS := baselines[sobreviventeID]
	blAlvo, okA := baselines[noAlvo]
	if !okS || !okA {
		return Resultado2PC{Resumo: "FALHA: linha de base ausente para a auditoria do 2PC"}
	}

	var specSobrevivente nodeSpec
	for _, e := range especificacoes {
		if e.ID == sobreviventeID {
			specSobrevivente = e
		}
	}

	vagasAntes, err := consultarVagasRemoto(specSobrevivente, blSobrevivente)
	if err != nil {
		return Resultado2PC{Resumo: fmt.Sprintf("FALHA: nao foi possivel consultar vagas do sobrevivente antes da auditoria: %v", err)}
	}

	membros := make([]consorcio.NodoDTO, 0, len(especificacoes))
	for _, e := range especificacoes {
		membros = append(membros, consorcio.NodoDTO{
			ID: e.ID, Endereco: "127.0.0.1", PortaREST: e.PortaREST, Status: consorcio.StatusNodoOnline,
		})
	}

	relogio := consorcio.NovoRelogioLamport("coordenador-teste-caos", 0)
	coord := consorcio.NovoCoordenadorDistribuido(consorcio.ConfigCoordenador{
		NodoLocal: consorcio.NodoDTO{ID: "coordenador-teste-caos"},
		Membros:   membros,
		Relogio:   relogio,
	})

	trechosCombinados := []protocolo.ItemTrecho{
		{CaronaID: blSobrevivente.CaronaID, EmpresaID: sobreviventeID, Origem: blSobrevivente.Origem, Destino: blSobrevivente.Destino, Preco: 20.0},
		{CaronaID: blAlvo.CaronaID, EmpresaID: noAlvo, Origem: blAlvo.Origem, Destino: blAlvo.Destino, Preco: 20.0},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, errTx := coord.ExecutarReserva2PC(ctx, "passageiro_auditoria_caos", trechosCombinados, 5*time.Second)
	abortouComoEsperado := errTx != nil

	// Da tempo para a goroutine assincrona de compensacao (Abort remoto) do coordenador concluir.
	time.Sleep(300 * time.Millisecond)

	vagasDepois, errConsulta := consultarVagasRemoto(specSobrevivente, blSobrevivente)
	compensacaoOK := errConsulta == nil && vagasDepois == vagasAntes

	sucesso := abortouComoEsperado && compensacaoOK

	var resumo string
	switch {
	case !abortouComoEsperado:
		resumo = "FALHA CRITICA: a transacao 2PC foi commitada mesmo com um participante fora do ar (overbooking logico de consorcio!)"
	case !compensacaoOK:
		resumo = fmt.Sprintf("FALHA CRITICA: transacao abortou corretamente, mas a vaga do sobrevivente NAO foi devolvida (antes=%d depois=%d) - vazamento de hold", vagasAntes, vagasDepois)
	default:
		resumo = fmt.Sprintf("PASS: transacao abortada corretamente (%v) e vaga do sobrevivente '%s' devolvida integralmente (%d vagas antes e depois)", errTx, sobreviventeID, vagasAntes)
	}

	erroStr := ""
	if errTx != nil {
		erroStr = errTx.Error()
	}

	return Resultado2PC{
		Sucesso:             sucesso,
		AbortouComoEsperado: abortouComoEsperado,
		CompensacaoOK:       compensacaoOK,
		VagasAntes:          vagasAntes,
		VagasDepois:         vagasDepois,
		ErroTransacao:       erroStr,
		Resumo:              resumo,
	}
}

// consultarVagasRemoto reconecta via TCP ao no sobrevivente como o motorista da linha de base e
// confere as vagas livres atuais do trecho auditado.
func consultarVagasRemoto(spec nodeSpec, bl *baselineNo) (int, error) {
	endereco := fmt.Sprintf("127.0.0.1:%d", spec.PortaTCP)
	conn, err := net.DialTimeout("tcp", endereco, 3*time.Second)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	scanner := protocolo.CriarScanner(conn)
	respAuth, err := enviarRecv(conn, scanner, protocolo.Requisicao{
		Acao: protocolo.AcaoAutenticar, Usuario: bl.MotoristaLogin, Senha: bl.MotoristaSenha,
	})
	if err != nil {
		return 0, err
	}

	return obterVagasTrecho(conn, scanner, respAuth.Token, bl.CaronaID, bl.Origem, bl.Destino)
}
