package consorcio_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"vaijunto/pkg/consorcio"
	"vaijunto/pkg/estado"
	"vaijunto/pkg/protocolo"
)

// setupServidorRESTAuxiliar inicializa um ServidorREST em uma porta livre com dados de teste.
func setupServidorRESTAuxiliar(t *testing.T, nodoID, regiao string) (*consorcio.ServidorREST, *estado.GerenciadorEstado, *consorcio.RelogioLamport, string) {
	t.Helper()

	gerenciador := estado.NovoGerenciadorEstado()
	relogio := consorcio.NovoRelogioLamport(nodoID, 0)

	dir := t.TempDir()
	caminhoWAL := filepath.Join(dir, "wal_"+nodoID+".log")
	wal, err := consorcio.NovoWAL(caminhoWAL)
	if err != nil {
		t.Fatalf("falha ao criar WAL de teste: %v", err)
	}

	nodoLocal := consorcio.NodoDTO{
		ID:       nodoID,
		Endereco: "127.0.0.1",
		Regiao:   regiao,
		Status:   consorcio.StatusNodoOnline,
	}

	srv := consorcio.NovoServidorREST(consorcio.ConfigServidorREST{
		NodoLocal:   nodoLocal,
		Membros:     []consorcio.NodoDTO{},
		Gerenciador: gerenciador,
		Relogio:     relogio,
		WAL:         wal,
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("falha ao abrir listener TCP: %v", err)
	}

	if err := srv.IniciarEmListener(listener); err != nil {
		t.Fatalf("falha ao iniciar servidor REST: %v", err)
	}

	t.Cleanup(func() {
		_ = srv.Parar(context.Background())
		_ = wal.Fechar()
	})

	return srv, gerenciador, relogio, srv.ObterEnderecoBase()
}

func TestServidorREST_HealthEInfo(t *testing.T) {
	srv, _, _, urlBase := setupServidorRESTAuxiliar(t, "nodo-salvador", consorcio.RegiaoRMS)

	// 1. Testa /health
	respHealth, err := http.Get(urlBase + "/health")
	if err != nil {
		t.Fatalf("erro ao chamar /health: %v", err)
	}
	defer respHealth.Body.Close()
	if respHealth.StatusCode != http.StatusOK {
		t.Fatalf("esperado status 200 em /health, obtido %d", respHealth.StatusCode)
	}

	// 2. Testa /api/v1/cluster/info via ClienteREST
	cliente := consorcio.NovoClienteREST(consorcio.ConfigClienteREST{
		NodoLocalID:   "nodo-cliente",
		TimeoutPadrao: 2 * time.Second,
	})

	info, err := cliente.ObterInfoCluster(context.Background(), urlBase)
	if err != nil {
		t.Fatalf("falha ao obter info cluster: %v", err)
	}

	if info.NodoLocal.ID != "nodo-salvador" {
		t.Fatalf("esperado nodo_id 'nodo-salvador', obtido '%s'", info.NodoLocal.ID)
	}
	if info.NodoLocal.Regiao != consorcio.RegiaoRMS {
		t.Fatalf("esperada regiao '%s', obtido '%s'", consorcio.RegiaoRMS, info.NodoLocal.Regiao)
	}
	if srv.ObterPorta() <= 0 {
		t.Fatalf("porta do servidor REST deveria ser > 0, obtido %d", srv.ObterPorta())
	}
}

func TestServidorREST_Heartbeat(t *testing.T) {
	_, _, relogio, urlBase := setupServidorRESTAuxiliar(t, "nodo-salvador", consorcio.RegiaoRMS)

	cliente := consorcio.NovoClienteREST(consorcio.ConfigClienteREST{
		NodoLocalID:   "nodo-feira",
		TimeoutPadrao: 2 * time.Second,
	})

	hb := consorcio.HeartbeatDTO{
		RemetenteID:      "nodo-feira",
		Regiao:           consorcio.RegiaoPortalDoSertao,
		TimestampLamport: consorcio.Timestamp{Tempo: 42, NodoID: "nodo-feira"},
		EnviadoEm:        time.Now(),
	}

	resp, err := cliente.EnviarHeartbeat(context.Background(), urlBase, hb)
	if err != nil {
		t.Fatalf("falha ao enviar heartbeat: %v", err)
	}

	if !resp.Sucesso {
		t.Fatalf("esperado sucesso no heartbeat")
	}

	// O relógio do servidor deve ter sido atualizado: max(local, 42) + 1 = 43
	if relogio.ObterTempo() < 43 {
		t.Fatalf("esperado relógio local >= 43 apos heartbeat, obtido %d", relogio.ObterTempo())
	}
}

func TestServidorREST_BuscarItinerarios(t *testing.T) {
	_, gerenciador, _, urlBase := setupServidorRESTAuxiliar(t, "nodo-salvador", consorcio.RegiaoRMS)

	// Publica uma carona local no gerenciador
	_ = gerenciador.CadastrarUsuario("mot1", "123", "Motorista 1", protocolo.PapelMotorista)
	tok, _, _, _ := gerenciador.Autenticar("mot1", "123")
	caronaID, err := gerenciador.PublicarCarona(tok, []string{"Salvador", "Feira de Santana"}, "2026-10-25", "08:00", "09:30", 3, 35.0)
	if err != nil {
		t.Fatalf("falha ao publicar carona local: %v", err)
	}

	cliente := consorcio.NovoClienteREST(consorcio.ConfigClienteREST{
		NodoLocalID:   "nodo-consultador",
		TimeoutPadrao: 2 * time.Second,
	})

	req := consorcio.BuscaItinerariosReqDTO{
		Origem:           "Salvador",
		Destino:          "Feira de Santana",
		Data:             "2026-10-25",
		SolicitanteID:    "nodo-consultador",
		TimestampLamport: consorcio.Timestamp{Tempo: 5, NodoID: "nodo-consultador"},
	}

	resp, err := cliente.BuscarItinerarios(context.Background(), urlBase, req)
	if err != nil {
		t.Fatalf("falha na busca de itinerarios: %v", err)
	}

	if len(resp.Itinerarios) == 0 {
		t.Fatalf("esperava encontrar pelo menos 1 itinerario")
	}

	if resp.Itinerarios[0].Trechos[0].CaronaID != caronaID {
		t.Fatalf("esperado caronaID '%s', obtido '%s'", caronaID, resp.Itinerarios[0].Trechos[0].CaronaID)
	}
}

func TestServidorREST_2PCCicloCompleto(t *testing.T) {
	_, gerenciador, _, urlBase := setupServidorRESTAuxiliar(t, "nodo-salvador", consorcio.RegiaoRMS)

	// Publica carona com 2 vagas
	_ = gerenciador.CadastrarUsuario("mot1", "123", "Motorista 1", protocolo.PapelMotorista)
	tok, _, _, _ := gerenciador.Autenticar("mot1", "123")
	caronaID, _ := gerenciador.PublicarCarona(tok, []string{"Salvador", "Feira de Santana"}, "2026-10-25", "08:00", "09:30", 2, 35.0)

	cliente := consorcio.NovoClienteREST(consorcio.ConfigClienteREST{
		NodoLocalID:   "nodo-coordenador",
		TimeoutPadrao: 2 * time.Second,
	})

	trechos := []protocolo.ItemTrecho{
		{
			CaronaID: caronaID,
			Origem:   "Salvador",
			Destino:  "Feira de Santana",
			Preco:    35.0,
		},
	}

	// 1. Fase 1: Prepare (Voto SIM)
	reqPrepare := consorcio.PrepareReqDTO{
		TransacaoID:      "tx-001",
		CoordenadorID:    "nodo-coordenador",
		PassageiroLogin:  "passageiro1",
		Trechos:          trechos,
		TTLMillis:        5000,
		TimestampLamport: consorcio.Timestamp{Tempo: 10, NodoID: "nodo-coordenador"},
	}

	respPrepare, err := cliente.Prepare(context.Background(), urlBase, reqPrepare)
	if err != nil {
		t.Fatalf("falha ao enviar Prepare 2PC: %v", err)
	}

	if !respPrepare.Voto {
		t.Fatalf("esperava voto SIM no prepare, obtido NAO: %s", respPrepare.Motivo)
	}
	if respPrepare.HoldID == "" {
		t.Fatalf("holdID nao deveria estar vazio")
	}

	// 2. Fase 2: Commit (Confirmação)
	reqCommit := consorcio.CommitReqDTO{
		TransacaoID:      "tx-001",
		CoordenadorID:    "nodo-coordenador",
		HoldID:           respPrepare.HoldID,
		PassageiroLogin:  "passageiro1",
		TimestampLamport: consorcio.Timestamp{Tempo: 15, NodoID: "nodo-coordenador"},
	}

	respCommit, err := cliente.Commit(context.Background(), urlBase, reqCommit)
	if err != nil {
		t.Fatalf("falha ao enviar Commit 2PC: %v", err)
	}

	if !respCommit.Sucesso {
		t.Fatalf("esperava sucesso no commit: %s", respCommit.Mensagem)
	}
	if respCommit.ReservaID == "" {
		t.Fatalf("reservaID nao deveria estar vazio apos commit")
	}

	// Verifica se resta apenas 1 vaga
	itin, _ := gerenciador.BuscarItinerarios("Salvador", "Feira de Santana", "2026-10-25")
	if len(itin) == 0 {
		t.Fatalf("deveria haver itinerario com 1 vaga restante")
	}
}

func TestServidorREST_2PC_AbortECompensacao(t *testing.T) {
	_, gerenciador, _, urlBase := setupServidorRESTAuxiliar(t, "nodo-salvador", consorcio.RegiaoRMS)

	// Publica carona com 1 vaga
	_ = gerenciador.CadastrarUsuario("mot1", "123", "Motorista 1", protocolo.PapelMotorista)
	tok, _, _, _ := gerenciador.Autenticar("mot1", "123")
	caronaID, _ := gerenciador.PublicarCarona(tok, []string{"Salvador", "Feira de Santana"}, "2026-10-25", "08:00", "09:30", 1, 35.0)

	cliente := consorcio.NovoClienteREST(consorcio.ConfigClienteREST{
		NodoLocalID:   "nodo-coordenador",
		TimeoutPadrao: 2 * time.Second,
	})

	trechos := []protocolo.ItemTrecho{
		{
			CaronaID: caronaID,
			Origem:   "Salvador",
			Destino:  "Feira de Santana",
			Preco:    35.0,
		},
	}

	// 1. Prepare bloqueia a única vaga
	reqPrepare := consorcio.PrepareReqDTO{
		TransacaoID:      "tx-abort-01",
		CoordenadorID:    "nodo-coordenador",
		PassageiroLogin:  "passageiro1",
		Trechos:          trechos,
		TTLMillis:        5000,
		TimestampLamport: consorcio.Timestamp{Tempo: 10, NodoID: "nodo-coordenador"},
	}

	respPrepare, err := cliente.Prepare(context.Background(), urlBase, reqPrepare)
	if err != nil || !respPrepare.Voto {
		t.Fatalf("falha ao preparar hold: %v (voto=%v)", err, respPrepare.Voto)
	}

	// Consulta deve mostrar 0 vagas agora
	itinAntes, _ := gerenciador.BuscarItinerarios("Salvador", "Feira de Santana", "2026-10-25")
	if len(itinAntes) != 0 {
		t.Fatalf("a vaga deveria estar retida pelo Hold, mas ainda foi listada")
	}

	// 2. Abort (Compensação)
	reqAbort := consorcio.AbortReqDTO{
		TransacaoID:      "tx-abort-01",
		CoordenadorID:    "nodo-coordenador",
		HoldID:           respPrepare.HoldID,
		PassageiroLogin:  "passageiro1",
		Motivo:           "teste de cancelamento e compensacao",
		TimestampLamport: consorcio.Timestamp{Tempo: 15, NodoID: "nodo-coordenador"},
	}

	respAbort, err := cliente.Abort(context.Background(), urlBase, reqAbort)
	if err != nil {
		t.Fatalf("falha ao enviar Abort: %v", err)
	}

	if !respAbort.Sucesso {
		t.Fatalf("esperava sucesso no abort: %s", respAbort.Mensagem)
	}

	// Vaga deve ter sido devolvida imediatamente ao inventário
	itinDepois, _ := gerenciador.BuscarItinerarios("Salvador", "Feira de Santana", "2026-10-25")
	if len(itinDepois) == 0 {
		t.Fatalf("a vaga deveria ter sido liberada imediatamente apos o Abort")
	}
}

// TestServidorREST_2PC_PrepareConcorrenteNaoCorrompeEstado valida, sobre chamadas HTTP REAIS e
// CONCORRENTES (não sequenciais) de Prepare vindas de "empresas" distintas disputando a mesma vaga,
// que o estado interno nunca fica inconsistente: ao final, o inventário da vaga está em exatamente
// 0 ou 1 (nunca negativo, nunca sobrando "fantasma"), e no máximo um Hold permanece PREPARADO. O
// voto otimista do Prepare pode, por design, ser preemptado por uma transação de Lamport menor antes
// do Commit (ver TestCoordenador_ApenasUmaReservaConfirmada em coordenador_test.go para a garantia
// real de "nenhum assento confirmado duas vezes", que depende do Commit, não do Prepare sozinho).
func TestServidorREST_2PC_PrepareConcorrenteNaoCorrompeEstado(t *testing.T) {
	_, gerenciador, _, urlBase := setupServidorRESTAuxiliar(t, "nodo-dono-do-trecho", consorcio.RegiaoRMS)

	_ = gerenciador.CadastrarUsuario("mot1", "123", "Motorista 1", protocolo.PapelMotorista)
	tok, _, _, _ := gerenciador.Autenticar("mot1", "123")
	caronaID, _ := gerenciador.PublicarCarona(tok, []string{"Salvador", "Feira de Santana"}, "2026-10-25", "08:00", "09:30", 1, 35.0)

	trechos := []protocolo.ItemTrecho{{CaronaID: caronaID, Origem: "Salvador", Destino: "Feira de Santana", Preco: 35.0}}

	const totalCoordenadores = 15
	var wg sync.WaitGroup
	wg.Add(totalCoordenadores)
	for i := 0; i < totalCoordenadores; i++ {
		idx := i
		go func() {
			defer wg.Done()

			// Cada goroutine simula o coordenador de uma empresa DIFERENTE, identificado por um
			// NodoID próprio, com seu próprio cliente REST e seu próprio timestamp de Lamport
			// (sempre > 0: um relógio real nunca emite 0, Tick() já pré-incrementa a partir de 1).
			nodoID := fmt.Sprintf("empresa-%02d", idx)
			cliente := consorcio.NovoClienteREST(consorcio.ConfigClienteREST{NodoLocalID: nodoID, TimeoutPadrao: 3 * time.Second})
			tempoLamport := uint64((idx*37+11)%totalCoordenadores) + 1

			_, _ = cliente.Prepare(context.Background(), urlBase, consorcio.PrepareReqDTO{
				TransacaoID:      fmt.Sprintf("tx-%s", nodoID),
				CoordenadorID:    nodoID,
				PassageiroLogin:  fmt.Sprintf("passageiro-%s", nodoID),
				Trechos:          trechos,
				TTLMillis:        5000,
				TimestampLamport: consorcio.Timestamp{Tempo: tempoLamport, NodoID: nodoID},
			})
		}()
	}
	wg.Wait()

	// O inventário nunca pode ter sido corrompido: com 1 vaga original, o saldo final só pode ser
	// 0 (alguém ficou com o hold PREPARADO) ou 1 (todos os holds já expiraram/preemptados de volta).
	itin, err := gerenciador.BuscarItinerarios("Salvador", "Feira de Santana", "2026-10-25")
	if err != nil {
		t.Fatalf("erro ao consultar itinerarios apos concorrencia: %v", err)
	}
	if len(itin) > 1 {
		t.Fatalf("ESTADO CORROMPIDO: inventario indica mais de 1 vaga livre onde havia apenas 1 (%d itinerarios)", len(itin))
	}
}
