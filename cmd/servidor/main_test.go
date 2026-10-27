package main

import (
	"path/filepath"
	"testing"

	"vaijunto/pkg/consorcio"
	"vaijunto/pkg/estado"
	"vaijunto/pkg/protocolo"
)

// setupContextoTeste monta um ServidorContexto completo (WAL real em disco, gerenciador, relogio)
// para exercitar processarRequisicao exatamente como o listener TCP real faz.
func setupContextoTeste(t *testing.T) *ServidorContexto {
	t.Helper()

	caminhoWAL := filepath.Join(t.TempDir(), "teste.wal")
	wal, err := consorcio.NovoWAL(caminhoWAL)
	if err != nil {
		t.Fatalf("falha ao criar WAL de teste: %v", err)
	}
	t.Cleanup(func() { _ = wal.Fechar() })

	gerenciador := estado.NovoGerenciadorEstado()
	relogio := consorcio.NovoRelogioLamport("nodo-teste", 0)
	nodoLocal := consorcio.NodoDTO{ID: "nodo-teste"}

	coordenador := consorcio.NovoCoordenadorDistribuido(consorcio.ConfigCoordenador{
		NodoLocal:   nodoLocal,
		Gerenciador: gerenciador,
		Relogio:     relogio,
		WAL:         wal,
	})

	return &ServidorContexto{
		Gerenciador: gerenciador,
		WAL:         wal,
		Relogio:     relogio,
		NodoLocal:   nodoLocal,
		Coordenador: coordenador,
	}
}

// TestCancelarReserva_FalhaDePersistenciaNaoVazaVaga reproduz, de ponta a ponta através do mesmo
// processarRequisicao que o listener TCP real usa, o bug de atomicidade lembrado do Problema 1:
// se o WAL falhar ao gravar o cancelamento DEPOIS que a vaga já foi devolvida em memória, o cliente
// é informado do erro, mas a vaga ficava livre mesmo assim - podendo ser vendida para outro
// passageiro enquanto o dono original ainda acredita que sua reserva está ativa. Após a correção
// (ReverterCancelamentoReserva), a reserva deve voltar a ATIVA e a vaga deve continuar ocupada.
func TestCancelarReserva_FalhaDePersistenciaNaoVazaVaga(t *testing.T) {
	ctx := setupContextoTeste(t)

	// 1. Cadastra e autentica o motorista, publica uma carona com 1 única vaga.
	respCad := processarRequisicao(protocolo.Requisicao{
		Acao: protocolo.AcaoCadastrar, Usuario: "mot1", Senha: "123", Nome: "Motorista", Papel: protocolo.PapelMotorista,
	}, ctx, 1)
	if respCad.Status != protocolo.StatusOK {
		t.Fatalf("falha ao cadastrar motorista: %s", respCad.Erro)
	}

	respAuthMot := processarRequisicao(protocolo.Requisicao{Acao: protocolo.AcaoAutenticar, Usuario: "mot1", Senha: "123"}, ctx, 1)
	tokenMot := respAuthMot.Token

	respCarona := processarRequisicao(protocolo.Requisicao{
		Acao: protocolo.AcaoPublicarCarona, Token: tokenMot, Rota: []string{"Salvador", "Feira de Santana"},
		Data: "2026-11-20", Horario: "08:00", HorarioChegada: "09:30", Assentos: 1, PrecoPorTrecho: 20.0,
	}, ctx, 1)
	if respCarona.Status != protocolo.StatusOK {
		t.Fatalf("falha ao publicar carona: %s", respCarona.Erro)
	}

	// 2. Cadastra e autentica o passageiro, busca e reserva a única vaga.
	processarRequisicao(protocolo.Requisicao{
		Acao: protocolo.AcaoCadastrar, Usuario: "pass1", Senha: "123", Nome: "Passageiro", Papel: protocolo.PapelPassageiro,
	}, ctx, 2)
	respAuthPass := processarRequisicao(protocolo.Requisicao{Acao: protocolo.AcaoAutenticar, Usuario: "pass1", Senha: "123"}, ctx, 2)
	tokenPass := respAuthPass.Token

	respBusca := processarRequisicao(protocolo.Requisicao{
		Acao: protocolo.AcaoBuscarItinerarios, Origem: "Salvador", Destino: "Feira de Santana", Data: "2026-11-20",
	}, ctx, 2)
	if len(respBusca.Itinerarios) == 0 {
		t.Fatalf("busca nao encontrou a carona publicada")
	}

	respReserva := processarRequisicao(protocolo.Requisicao{
		Acao: protocolo.AcaoReservar, Token: tokenPass, Trechos: respBusca.Itinerarios[0].Trechos,
	}, ctx, 2)
	if respReserva.Status != protocolo.StatusOK {
		t.Fatalf("falha ao reservar: %s", respReserva.Erro)
	}
	reservaID := respReserva.ReservaID

	// 3. Força a falha de persistência: fecha o WAL ANTES de tentar cancelar, exatamente como um
	// erro de disco faria o próximo Gravar() falhar de forma determinística.
	if err := ctx.WAL.Fechar(); err != nil {
		t.Fatalf("falha ao fechar WAL para o teste: %v", err)
	}

	respCancelar := processarRequisicao(protocolo.Requisicao{
		Acao: protocolo.AcaoCancelarReserva, Token: tokenPass, ReservaID: reservaID,
	}, ctx, 2)

	// 4. O cliente DEVE ser informado de que o cancelamento falhou (WAL fechado = falha real).
	if respCancelar.Status != protocolo.StatusErro {
		t.Fatalf("esperava erro ao cancelar com WAL indisponivel, obteve sucesso")
	}

	// 5. GARANTIA CRÍTICA: como o cliente foi informado de FALHA, a reserva deve continuar ATIVA e
	// a vaga deve continuar ocupada - nada pode ter sido liberado silenciosamente ao inventário.
	reserva, err := ctx.Gerenciador.ObterReserva(reservaID)
	if err != nil {
		t.Fatalf("reserva deveria continuar existindo apos falha de cancelamento: %v", err)
	}
	if reserva.Status != "ATIVA" {
		t.Fatalf("BUG DE ATOMICIDADE: reserva ficou com status '%s' apos falha de persistencia do cancelamento - deveria ter sido revertida para ATIVA", reserva.Status)
	}

	respAuthMot2 := processarRequisicao(protocolo.Requisicao{Acao: protocolo.AcaoAutenticar, Usuario: "mot1", Senha: "123"}, ctx, 1)
	respListar := processarRequisicao(protocolo.Requisicao{Acao: protocolo.AcaoListarCaronas, Token: respAuthMot2.Token}, ctx, 1)
	if len(respListar.Caronas) != 1 || len(respListar.Caronas[0].Trechos) != 1 {
		t.Fatalf("resposta de listagem de caronas inesperada: %+v", respListar.Caronas)
	}
	if vagas := respListar.Caronas[0].Trechos[0].VagasLivres; vagas != 0 {
		t.Fatalf("BUG DE ATOMICIDADE: vaga vazou para o inventario (vagas_livres=%d) apos falha de persistencia do cancelamento, mas a reserva segue sendo contada como ATIVA - overbooking em potencial", vagas)
	}
}
