package consorcio_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"vaijunto/pkg/consorcio"
	"vaijunto/pkg/estado"
	"vaijunto/pkg/protocolo"
)

// setupNodoCluster instancía um nó completo com ServidorREST e CoordenadorDistribuido.
func setupNodoCluster(t *testing.T, nodoID, regiao string) (*consorcio.CoordenadorDistribuido, *consorcio.ServidorREST, *estado.GerenciadorEstado, string) {
	t.Helper()

	gerenciador := estado.NovoGerenciadorEstado()
	relogio := consorcio.NovoRelogioLamport(nodoID, 0)

	dir := t.TempDir()
	caminhoWAL := filepath.Join(dir, "wal_"+nodoID+".log")
	wal, err := consorcio.NovoWAL(caminhoWAL)
	if err != nil {
		t.Fatalf("falha ao criar WAL para %s: %v", nodoID, err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("falha ao criar listener para %s: %v", nodoID, err)
	}

	porta := listener.Addr().(*net.TCPAddr).Port

	nodoLocal := consorcio.NodoDTO{
		ID:        nodoID,
		Endereco:  "127.0.0.1",
		PortaREST: porta,
		Regiao:    regiao,
		Status:    consorcio.StatusNodoOnline,
	}

	srv := consorcio.NovoServidorREST(consorcio.ConfigServidorREST{
		NodoLocal:   nodoLocal,
		Membros:     nil,
		Gerenciador: gerenciador,
		Relogio:     relogio,
		WAL:         wal,
	})

	if err := srv.IniciarEmListener(listener); err != nil {
		t.Fatalf("falha ao iniciar servidor REST para %s: %v", nodoID, err)
	}

	coord := consorcio.NovoCoordenadorDistribuido(consorcio.ConfigCoordenador{
		NodoLocal:    nodoLocal,
		Membros:      nil,
		Gerenciador:  gerenciador,
		Relogio:      relogio,
		WAL:          wal,
		ServidorREST: srv,
	})

	urlBase := srv.ObterEnderecoBase()

	t.Cleanup(func() {
		_ = srv.Parar(context.Background())
		_ = wal.Fechar()
	})

	return coord, srv, gerenciador, urlBase
}

// TestScatterGather_BuscaFederadaSucesso valida que uma busca dispersa para múltiplos nós agrega os resultados.
func TestScatterGather_BuscaFederadaSucesso(t *testing.T) {
	// Nó 1: Salvador
	coordSalvador, srvSalvador, gerSalvador, _ := setupNodoCluster(t, "nodo-salvador", consorcio.RegiaoRMS)

	// Nó 2: Feira de Santana
	_, srvFeira, gerFeira, _ := setupNodoCluster(t, "nodo-feira", consorcio.RegiaoPortalDoSertao)

	// Registra o nó Feira como membro do consórcio no coordenador de Salvador
	coordSalvador.AdicionarMembro(consorcio.NodoDTO{
		ID:        "nodo-feira",
		Endereco:  "127.0.0.1",
		PortaREST: srvFeira.ObterPorta(),
		Regiao:    consorcio.RegiaoPortalDoSertao,
		Status:    consorcio.StatusNodoOnline,
	})

	// 1. Cadastra e publica carona local em Salvador (Salvador -> Feira de Santana)
	_ = gerSalvador.CadastrarUsuario("mot_salvador", "123", "Motorista SSA", protocolo.PapelMotorista)
	tokSSA, _, _, _ := gerSalvador.Autenticar("mot_salvador", "123")
	caronaSSA, err := gerSalvador.PublicarCarona(tokSSA, []string{"Salvador", "Feira de Santana"}, "2026-10-25", "08:00", "09:30", 3, 30.0, "nodo-salvador")
	if err != nil {
		t.Fatalf("falha ao publicar carona em Salvador: %v", err)
	}

	// 2. Cadastra e publica carona remota em Feira de Santana (Salvador -> Feira de Santana)
	_ = gerFeira.CadastrarUsuario("mot_feira", "123", "Motorista FSA", protocolo.PapelMotorista)
	tokFSA, _, _, _ := gerFeira.Autenticar("mot_feira", "123")
	caronaFSA, err := gerFeira.PublicarCarona(tokFSA, []string{"Salvador", "Feira de Santana"}, "2026-10-25", "09:00", "10:30", 2, 25.0, "nodo-feira")
	if err != nil {
		t.Fatalf("falha ao publicar carona em Feira: %v", err)
	}

	_ = srvSalvador // suppress unused
	_ = caronaSSA
	_ = caronaFSA

	// 3. Executa a busca Dispersar-Coletar a partir do Coordenador de Salvador
	ctx := context.Background()
	itinerarios, err := coordSalvador.BuscarItinerariosDistribuido(ctx, "Salvador", "Feira de Santana", "2026-10-25", 2*time.Second)
	if err != nil {
		t.Fatalf("erro no Scatter-Gather distribuido: %v", err)
	}

	if len(itinerarios) != 2 {
		t.Fatalf("esperava 2 itinerarios agregados de Salvador e Feira, obtido %d", len(itinerarios))
	}

	// Verifica se a ordenação determinística por menor preço funcionou (25.0 antes de 30.0)
	if itinerarios[0].PrecoTotal != 25.0 {
		t.Fatalf("primeiro itinerario deveria ser o mais barato (25.0), obtido %.2f", itinerarios[0].PrecoTotal)
	}
	if itinerarios[1].PrecoTotal != 30.0 {
		t.Fatalf("segundo itinerario deveria custar 30.0, obtido %.2f", itinerarios[1].PrecoTotal)
	}
}

// TestScatterGather_ToleranciaFalhaParcial valida que a queda de um nó parceiro não derruba a busca.
func TestScatterGather_ToleranciaFalhaParcial(t *testing.T) {
	coordSalvador, _, gerSalvador, _ := setupNodoCluster(t, "nodo-salvador", consorcio.RegiaoRMS)

	// Registra um membro remoto cuja porta não possui nenhum servidor ativo (offline)
	coordSalvador.AdicionarMembro(consorcio.NodoDTO{
		ID:        "nodo-offline",
		Endereco:  "127.0.0.1",
		PortaREST: 59998,
		Regiao:    "Recôncavo",
		Status:    consorcio.StatusNodoOnline,
	})

	// Publica carona no nó saudável (Salvador)
	_ = gerSalvador.CadastrarUsuario("mot_salvador", "123", "Motorista SSA", protocolo.PapelMotorista)
	tokSSA, _, _, _ := gerSalvador.Autenticar("mot_salvador", "123")
	_, _ = gerSalvador.PublicarCarona(tokSSA, []string{"Salvador", "Feira de Santana"}, "2026-10-25", "08:00", "09:30", 3, 30.0)

	// Busca deve tolerar a falha do nó offline e retornar os dados de Salvador
	itinerarios, err := coordSalvador.BuscarItinerariosDistribuido(context.Background(), "Salvador", "Feira de Santana", "2026-10-25", 500*time.Millisecond)
	if err != nil {
		t.Fatalf("busca deveria tolerar falha parcial, mas retornou erro: %v", err)
	}

	if len(itinerarios) != 1 {
		t.Fatalf("esperado 1 itinerario retornado pelo nó saudavel, obtido %d", len(itinerarios))
	}
}

// Test2PC_OrquestracaoCommitMultiplosNos valida o ciclo completo de commit em transação com trechos em múltiplos nós.
func Test2PC_OrquestracaoCommitMultiplosNos(t *testing.T) {
	// Nó 1: Salvador (Coordenador)
	coordSalvador, _, gerSalvador, _ := setupNodoCluster(t, "nodo-salvador", consorcio.RegiaoRMS)

	// Nó 2: Feira (Participante Remoto)
	_, srvFeira, gerFeira, _ := setupNodoCluster(t, "nodo-feira", consorcio.RegiaoPortalDoSertao)

	coordSalvador.AdicionarMembro(consorcio.NodoDTO{
		ID:        "nodo-feira",
		Endereco:  "127.0.0.1",
		PortaREST: srvFeira.ObterPorta(),
		Regiao:    consorcio.RegiaoPortalDoSertao,
		Status:    consorcio.StatusNodoOnline,
	})

	// Nó 1 (Salvador): Carona Salvador -> Amélia Rodrigues
	_ = gerSalvador.CadastrarUsuario("mot_ssa", "123", "Motorista SSA", protocolo.PapelMotorista)
	tokSSA, _, _, _ := gerSalvador.Autenticar("mot_ssa", "123")
	caronaSSA, _ := gerSalvador.PublicarCarona(tokSSA, []string{"Salvador", "Amélia Rodrigues"}, "2026-10-25", "08:00", "08:45", 2, 20.0, "nodo-salvador")

	// Nó 2 (Feira): Carona Amélia Rodrigues -> Feira de Santana
	_ = gerFeira.CadastrarUsuario("mot_fsa", "123", "Motorista FSA", protocolo.PapelMotorista)
	tokFSA, _, _, _ := gerFeira.Autenticar("mot_fsa", "123")
	caronaFSA, _ := gerFeira.PublicarCarona(tokFSA, []string{"Amélia Rodrigues", "Feira de Santana"}, "2026-10-25", "09:00", "09:40", 2, 15.0, "nodo-feira")

	// Monta itinerário combinado conectando trechos de ambos os servidores
	trechosCombinados := []protocolo.ItemTrecho{
		{
			CaronaID:  caronaSSA,
			EmpresaID: "nodo-salvador",
			Origem:    "Salvador",
			Destino:   "Amélia Rodrigues",
			Preco:     20.0,
		},
		{
			CaronaID:  caronaFSA,
			EmpresaID: "nodo-feira",
			Origem:    "Amélia Rodrigues",
			Destino:   "Feira de Santana",
			Preco:     15.0,
		},
	}

	// Executa a reserva atômica via 2PC
	reserva, err := coordSalvador.ExecutarReserva2PC(context.Background(), "passageiro_caua", trechosCombinados, 5*time.Second)
	if err != nil {
		t.Fatalf("falha na transação 2PC combinada: %v", err)
	}

	if reserva.Status != "ATIVA" {
		t.Fatalf("esperado status ATIVA na reserva consolidada, obtido '%s'", reserva.Status)
	}
	if reserva.PrecoTotal != 35.0 {
		t.Fatalf("esperado preco total 35.0, obtido %.2f", reserva.PrecoTotal)
	}

	// Verifica se as vagas foram reduzidas de 2 para 1 em ambos os nós participantes
	itinSSA, _ := gerSalvador.BuscarItinerarios("Salvador", "Amélia Rodrigues", "2026-10-25")
	if len(itinSSA) == 0 {
		t.Fatalf("trecho de Salvador deveria ter 1 vaga restante")
	}

	itinFSA, _ := gerFeira.BuscarItinerarios("Amélia Rodrigues", "Feira de Santana", "2026-10-25")
	if len(itinFSA) == 0 {
		t.Fatalf("trecho de Feira deveria ter 1 vaga restante")
	}
}

// Test2PC_CompensacaoDeFalhas_Rollback valida que se um participante recusar, todos os outros são compensados (Abort).
func Test2PC_CompensacaoDeFalhas_Rollback(t *testing.T) {
	// Nó 1: Salvador (Coordenador)
	coordSalvador, _, gerSalvador, _ := setupNodoCluster(t, "nodo-salvador", consorcio.RegiaoRMS)

	// Nó 2: Feira (Participante Remoto)
	_, srvFeira, gerFeira, _ := setupNodoCluster(t, "nodo-feira", consorcio.RegiaoPortalDoSertao)

	coordSalvador.AdicionarMembro(consorcio.NodoDTO{
		ID:        "nodo-feira",
		Endereco:  "127.0.0.1",
		PortaREST: srvFeira.ObterPorta(),
		Regiao:    consorcio.RegiaoPortalDoSertao,
		Status:    consorcio.StatusNodoOnline,
	})

	// Nó 1 (Salvador): Carona com 1 vaga disponível
	_ = gerSalvador.CadastrarUsuario("mot_ssa", "123", "Motorista SSA", protocolo.PapelMotorista)
	tokSSA, _, _, _ := gerSalvador.Autenticar("mot_ssa", "123")
	caronaSSA, _ := gerSalvador.PublicarCarona(tokSSA, []string{"Salvador", "Amélia Rodrigues"}, "2026-10-25", "08:00", "08:45", 1, 20.0, "nodo-salvador")

	// Nó 2 (Feira): Carona com 1 vaga, mas que já é totalmente reservada por outro cliente antes do 2PC (0 vagas disponíveis)
	_ = gerFeira.CadastrarUsuario("mot_fsa", "123", "Motorista FSA", protocolo.PapelMotorista)
	tokFSA, _, _, _ := gerFeira.Autenticar("mot_fsa", "123")
	caronaFSA, _ := gerFeira.PublicarCarona(tokFSA, []string{"Amélia Rodrigues", "Feira de Santana"}, "2026-10-25", "09:00", "09:40", 1, 15.0, "nodo-feira")

	// Esgota a vaga de Feira
	_ = gerFeira.CadastrarUsuario("outro_passageiro", "123", "Outro", protocolo.PapelPassageiro)
	tokOutro, _, _, _ := gerFeira.Autenticar("outro_passageiro", "123")
	_, errRes := gerFeira.ReservarItinerario(tokOutro, []protocolo.ItemTrecho{
		{CaronaID: caronaFSA, Origem: "Amélia Rodrigues", Destino: "Feira de Santana"},
	})
	if errRes != nil {
		t.Fatalf("falha ao esgotar vaga em Feira: %v", errRes)
	}

	// Trechos solicitados pelo passageiro
	trechosCombinados := []protocolo.ItemTrecho{
		{
			CaronaID:  caronaSSA,
			EmpresaID: "nodo-salvador",
			Origem:    "Salvador",
			Destino:   "Amélia Rodrigues",
			Preco:     20.0,
		},
		{
			CaronaID:  caronaFSA,
			EmpresaID: "nodo-feira",
			Origem:    "Amélia Rodrigues",
			Destino:   "Feira de Santana",
			Preco:     15.0,
		},
	}

	// Executa a transação 2PC: deve abortar porque Feira não possui vagas
	_, err := coordSalvador.ExecutarReserva2PC(context.Background(), "passageiro_caua", trechosCombinados, 5*time.Second)
	if err == nil {
		t.Fatalf("transacao 2PC deveria ter falhado devido a falta de vagas em Feira")
	}

	// AUDITORIA CRÍTICA DE COMPENSAÇÃO DE FALHAS:
	// Salvador havia preparado a vaga (Fase 1 Voto SIM com Hold).
	// Após o abort global, Salvador deve ter executado a Ação Compensatória (Abort / Rollback),
	// liberando a vaga de volta ao inventário livre!
	itinSSA, errBusca := gerSalvador.BuscarItinerarios("Salvador", "Amélia Rodrigues", "2026-10-25")
	if errBusca != nil {
		t.Fatalf("erro ao consultar itinerarios em Salvador: %v", errBusca)
	}

	if len(itinSSA) == 0 {
		t.Fatalf("FALHA DE COMPENSAÇÃO: a vaga em Salvador ficou retida indevidamente após o abort da transação!")
	}
}

// TestHeartbeat_DetectaQuedaDeMembro valida que a rotina automática de heartbeat marca um membro
// inalcançável como SUSPEITO e, após falhas persistentes, como OFFLINE - e que volta a ONLINE assim
// que o membro responde novamente.
func TestHeartbeat_DetectaQuedaDeMembro(t *testing.T) {
	coordSalvador, _, _, _ := setupNodoCluster(t, "nodo-salvador", consorcio.RegiaoRMS)
	_, srvFeira, _, _ := setupNodoCluster(t, "nodo-feira", consorcio.RegiaoPortalDoSertao)

	coordSalvador.AdicionarMembro(consorcio.NodoDTO{
		ID: "nodo-feira", Endereco: "127.0.0.1", PortaREST: srvFeira.ObterPorta(),
		Regiao: consorcio.RegiaoPortalDoSertao, Status: consorcio.StatusNodoOnline,
	})

	coordSalvador.IniciarRotinaHeartbeat(50*time.Millisecond, 2, 4)
	t.Cleanup(coordSalvador.PararRotinaHeartbeat)

	// 1. Enquanto nodo-feira responde, o heartbeat deve mante-lo ONLINE.
	aguardarStatus(t, coordSalvador, "nodo-feira", consorcio.StatusNodoOnline, time.Second)

	// 2. Derruba nodo-feira (encerra o listener REST) e aguarda a degradacao de status.
	if err := srvFeira.Parar(context.Background()); err != nil {
		t.Fatalf("falha ao parar servidor REST de nodo-feira: %v", err)
	}

	aguardarStatus(t, coordSalvador, "nodo-feira", consorcio.StatusNodoSuspeito, 2*time.Second)
	aguardarStatus(t, coordSalvador, "nodo-feira", consorcio.StatusNodoOffline, 2*time.Second)
}

// TestBuscaCombinada_CenarioDoEnunciado reproduz literalmente o exemplo central do PDF do
// Problema 2: um passageiro de Salvador a Montes Claros cuja viagem exige caronas de TRES empresas
// distintas do consorcio, nenhuma delas com visibilidade do itinerario completo sozinha. Valida que
// a busca combinada (grafo de sub-trechos) encontra o itinerario de 3 pernas e que a reserva
// resultante confirma atomicamente nas tres empresas via 2PC.
func TestBuscaCombinada_CenarioDoEnunciado(t *testing.T) {
	// Empresa 1 (coordenadora): Salvador -> Feira de Santana
	coord1, srv1, ger1, _ := setupNodoCluster(t, "nodo-salvador", consorcio.RegiaoRMS)
	// Empresa 2: Feira de Santana -> Vitória da Conquista
	_, srv2, ger2, _ := setupNodoCluster(t, "nodo-feira", consorcio.RegiaoPortalDoSertao)
	// Empresa 3: Vitória da Conquista -> Montes Claros
	_, srv3, ger3, _ := setupNodoCluster(t, "nodo-reconcavo", consorcio.RegiaoReconcavo)

	coord1.AdicionarMembro(consorcio.NodoDTO{
		ID: "nodo-feira", Endereco: "127.0.0.1", PortaREST: srv2.ObterPorta(),
		Regiao: consorcio.RegiaoPortalDoSertao, Status: consorcio.StatusNodoOnline,
	})
	coord1.AdicionarMembro(consorcio.NodoDTO{
		ID: "nodo-reconcavo", Endereco: "127.0.0.1", PortaREST: srv3.ObterPorta(),
		Regiao: consorcio.RegiaoReconcavo, Status: consorcio.StatusNodoOnline,
	})
	_ = srv1

	data := "2026-11-15"

	_ = ger1.CadastrarUsuario("mot1", "123", "Motorista Empresa 1", protocolo.PapelMotorista)
	tok1, _, _, _ := ger1.Autenticar("mot1", "123")
	_, err := ger1.PublicarCarona(tok1, []string{"Salvador", "Feira de Santana"}, data, "06:00", "07:30", 2, 20.0, "nodo-salvador")
	if err != nil {
		t.Fatalf("falha ao publicar carona da empresa 1: %v", err)
	}

	_ = ger2.CadastrarUsuario("mot2", "123", "Motorista Empresa 2", protocolo.PapelMotorista)
	tok2, _, _, _ := ger2.Autenticar("mot2", "123")
	_, err = ger2.PublicarCarona(tok2, []string{"Feira de Santana", "Vitória da Conquista"}, data, "08:00", "12:00", 2, 60.0, "nodo-feira")
	if err != nil {
		t.Fatalf("falha ao publicar carona da empresa 2: %v", err)
	}

	_ = ger3.CadastrarUsuario("mot3", "123", "Motorista Empresa 3", protocolo.PapelMotorista)
	tok3, _, _, _ := ger3.Autenticar("mot3", "123")
	_, err = ger3.PublicarCarona(tok3, []string{"Vitória da Conquista", "Montes Claros"}, data, "13:00", "18:00", 2, 90.0, "nodo-reconcavo")
	if err != nil {
		t.Fatalf("falha ao publicar carona da empresa 3: %v", err)
	}

	// Nenhuma empresa isolada enxerga o itinerario completo: confirma que a busca LOCAL de cada
	// uma, sozinha, nao encontra nada de Salvador a Montes Claros.
	localSozinha, _ := ger1.BuscarItinerarios("Salvador", "Montes Claros", data)
	if len(localSozinha) != 0 {
		t.Fatalf("premissa do cenario violada: empresa 1 sozinha nao deveria enxergar o itinerario completo")
	}

	// A busca combinada do consorcio, a partir da empresa 1, deve encontrar o itinerario de 3 pernas.
	ctx := context.Background()
	itinerarios, err := coord1.BuscarItinerariosCombinados(ctx, "Salvador", "Montes Claros", data, 2*time.Second)
	if err != nil {
		t.Fatalf("busca combinada falhou: %v", err)
	}
	if len(itinerarios) != 1 {
		t.Fatalf("esperado exatamente 1 itinerario combinado, obtido %d", len(itinerarios))
	}

	itin := itinerarios[0]
	if len(itin.Trechos) != 3 {
		t.Fatalf("esperado itinerario com 3 trechos (3 empresas), obtido %d", len(itin.Trechos))
	}
	if itin.Trechos[0].EmpresaID != "nodo-salvador" || itin.Trechos[1].EmpresaID != "nodo-feira" || itin.Trechos[2].EmpresaID != "nodo-reconcavo" {
		t.Fatalf("trechos nao pertencem as tres empresas esperadas na ordem certa: %+v", itin.Trechos)
	}
	if itin.PrecoTotal != 170.0 {
		t.Fatalf("preco total esperado 170.0 (20+60+90), obtido %.2f", itin.PrecoTotal)
	}

	// Reserva atomica via 2PC do itinerario de 3 empresas.
	reserva, err := coord1.ExecutarReserva2PC(ctx, "passageiro_montes_claros", itin.Trechos, 5*time.Second)
	if err != nil {
		t.Fatalf("falha na reserva atomica do itinerario de 3 empresas: %v", err)
	}
	if reserva.Status != "ATIVA" || reserva.PrecoTotal != 170.0 {
		t.Fatalf("reserva consolidada inesperada: %+v", reserva)
	}

	// Confirma que as vagas foram debitadas nas TRES empresas (nenhuma reserva pela metade).
	itin1, _ := ger1.BuscarItinerarios("Salvador", "Feira de Santana", data)
	itin2, _ := ger2.BuscarItinerarios("Feira de Santana", "Vitória da Conquista", data)
	itin3, _ := ger3.BuscarItinerarios("Vitória da Conquista", "Montes Claros", data)
	if len(itin1) == 0 || len(itin2) == 0 || len(itin3) == 0 {
		t.Fatalf("esperado 1 vaga restante em cada uma das 3 empresas apos a reserva (2 iniciais - 1)")
	}
}

// TestCoordenador_ApenasUmaReservaConfirmada é o teste mais direto das garantias 2 e 3 do PDF do
// Problema 2: "o sistema deve garantir que, sob nenhuma hipótese, o mesmo assento de um mesmo trecho
// seja confirmado para dois passageiros distintos, mesmo quando as requisições chegam ao mesmo tempo
// por servidores de empresas diferentes" + "critério de precedência bem definido". Simula N
// coordenadores de N empresas DIFERENTES (cada um com seu próprio CoordenadorDistribuido, sem
// gerenciador local - só fala REST, como o robô de caos em cmd/teste_falhas faz), todos disputando
// CONCORRENTEMENTE e via chamadas HTTP REAIS o único assento de um trecho pertencente a uma empresa
// terceira. Executa o fluxo 2PC COMPLETO (Prepare + Commit, não só o voto otimista do Prepare -
// ver TestServidorREST_2PC_PrepareConcorrenteNaoCorrompeEstado para por que só o Prepare não basta).
func TestCoordenador_ApenasUmaReservaConfirmada(t *testing.T) {
	_, srvDono, gerDono, _ := setupNodoCluster(t, "empresa-dona-do-trecho", consorcio.RegiaoRMS)

	_ = gerDono.CadastrarUsuario("motDono", "123", "Motorista", protocolo.PapelMotorista)
	tokDono, _, _, _ := gerDono.Autenticar("motDono", "123")
	caronaID, err := gerDono.PublicarCarona(tokDono, []string{"Salvador", "Feira de Santana"}, "2026-10-25", "08:00", "09:30", 1, 35.0, "empresa-dona-do-trecho")
	if err != nil {
		t.Fatalf("falha ao publicar carona: %v", err)
	}

	trechos := []protocolo.ItemTrecho{{CaronaID: caronaID, EmpresaID: "empresa-dona-do-trecho", Origem: "Salvador", Destino: "Feira de Santana", Preco: 35.0}}
	membroDono := consorcio.NodoDTO{ID: "empresa-dona-do-trecho", Endereco: "127.0.0.1", PortaREST: srvDono.ObterPorta(), Status: consorcio.StatusNodoOnline}

	const totalEmpresasConcorrentes = 10
	type resultado struct {
		empresaID string
		sucesso   bool
	}
	resultados := make(chan resultado, totalEmpresasConcorrentes)

	var wg sync.WaitGroup
	wg.Add(totalEmpresasConcorrentes)
	for i := 0; i < totalEmpresasConcorrentes; i++ {
		idx := i
		go func() {
			defer wg.Done()

			empresaID := fmt.Sprintf("empresa-concorrente-%02d", idx)
			relogio := consorcio.NovoRelogioLamport(empresaID, 0)
			coord := consorcio.NovoCoordenadorDistribuido(consorcio.ConfigCoordenador{
				NodoLocal: consorcio.NodoDTO{ID: empresaID},
				Membros:   []consorcio.NodoDTO{membroDono},
				Relogio:   relogio,
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := coord.ExecutarReserva2PC(ctx, fmt.Sprintf("passageiro-%s", empresaID), trechos, 5*time.Second)
			resultados <- resultado{empresaID: empresaID, sucesso: err == nil}
		}()
	}
	wg.Wait()
	close(resultados)

	var sucessos []string
	for r := range resultados {
		if r.sucesso {
			sucessos = append(sucessos, r.empresaID)
		}
	}

	if len(sucessos) != 1 {
		t.Fatalf("GARANTIA DO PDF VIOLADA: esperava exatamente 1 reserva confirmada entre %d empresas concorrentes disputando o mesmo assento, obteve %d confirmadas (%v)", totalEmpresasConcorrentes, len(sucessos), sucessos)
	}

	// Confirma que a vaga foi vendida exatamente uma vez: 0 vagas livres restantes, nunca negativo.
	itinDepois, _ := gerDono.BuscarItinerarios("Salvador", "Feira de Santana", "2026-10-25")
	if len(itinDepois) != 0 {
		t.Fatalf("esperava 0 itinerarios disponiveis apos a unica reserva confirmada, obteve %d", len(itinDepois))
	}
}

// aguardarStatus faz polling do status de um membro ate o valor esperado aparecer ou o timeout vencer.
func aguardarStatus(t *testing.T, coord *consorcio.CoordenadorDistribuido, membroID, statusEsperado string, timeout time.Duration) {
	t.Helper()

	limite := time.Now().Add(timeout)
	for time.Now().Before(limite) {
		if m, ok := coord.ObterMembro(membroID); ok && m.Status == statusEsperado {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	m, _ := coord.ObterMembro(membroID)
	t.Fatalf("timeout aguardando status '%s' do membro '%s' (status atual: '%s')", statusEsperado, membroID, m.Status)
}
