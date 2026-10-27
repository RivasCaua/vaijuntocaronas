package estado_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vaijunto/pkg/consorcio"
	"vaijunto/pkg/estado"
	"vaijunto/pkg/protocolo"
)

// setupAmbienteTeste inicializa o gerenciador com motorista e passageiros cadastrados
func setupAmbienteTeste(t *testing.T) (*estado.GerenciadorEstado, string, string, string) {
	t.Helper()
	g := estado.NovoGerenciadorEstado()

	// Cadastra motorista
	err := g.CadastrarUsuario("mot1", "123", "Motorista", protocolo.PapelMotorista)
	if err != nil {
		t.Fatalf("falha ao cadastrar motorista: %v", err)
	}
	tokenMot, _, _, err := g.Autenticar("mot1", "123")
	if err != nil {
		t.Fatalf("falha ao autenticar motorista: %v", err)
	}

	// Cadastra passageiro 1
	err = g.CadastrarUsuario("pass1", "123", "Passageiro 1", protocolo.PapelPassageiro)
	if err != nil {
		t.Fatalf("falha ao cadastrar passageiro 1: %v", err)
	}
	tokenPass1, _, _, err := g.Autenticar("pass1", "123")
	if err != nil {
		t.Fatalf("falha ao autenticar passageiro 1: %v", err)
	}

	// Cadastra passageiro 2
	err = g.CadastrarUsuario("pass2", "123", "Passageiro 2", protocolo.PapelPassageiro)
	if err != nil {
		t.Fatalf("falha ao cadastrar passageiro 2: %v", err)
	}
	tokenPass2, _, _, err := g.Autenticar("pass2", "123")
	if err != nil {
		t.Fatalf("falha ao autenticar passageiro 2: %v", err)
	}

	return g, tokenMot, tokenPass1, tokenPass2
}

// TestCaronaEmpresaParceira valida a publicação e filtragem de caronas vinculadas a empresas parceiras
func TestCaronaEmpresaParceira(t *testing.T) {
	g, tokenMot, _, _ := setupAmbienteTeste(t)

	rota := []string{"Salvador", "Amélia Rodrigues", "Feira de Santana"}

	// Publica carona com empresa parceira explícita "Viacao-RMS"
	carona1, err := g.PublicarCarona(tokenMot, rota, "2026-09-20", "08:00", "09:30", 3, 20.0, "Viacao-RMS")
	if err != nil {
		t.Fatalf("erro ao publicar carona 1: %v", err)
	}

	// Publica carona com empresa parceira "Expresso-Feira"
	carona2, err := g.PublicarCarona(tokenMot, rota, "2026-09-20", "10:00", "11:30", 3, 22.0, "Expresso-Feira")
	if err != nil {
		t.Fatalf("erro ao publicar carona 2: %v", err)
	}

	// Valida empresa dona da carona
	emp1, err := g.ObterEmpresaCarona(carona1)
	if err != nil || emp1 != "Viacao-RMS" {
		t.Fatalf("esperava empresa 'Viacao-RMS', obteve '%s' (err: %v)", emp1, err)
	}

	emp2, err := g.ObterEmpresaCarona(carona2)
	if err != nil || emp2 != "Expresso-Feira" {
		t.Fatalf("esperava empresa 'Expresso-Feira', obteve '%s' (err: %v)", emp2, err)
	}

	// Lista caronas por empresa
	caronasRMS, err := g.ListarCaronasPorEmpresa("Viacao-RMS")
	if err != nil || len(caronasRMS) != 1 || caronasRMS[0].CaronaID != carona1 {
		t.Fatalf("esperava 1 carona da RMS, obteve %d", len(caronasRMS))
	}

	caronasFeira, err := g.ListarCaronasPorEmpresa("Expresso-Feira")
	if err != nil || len(caronasFeira) != 1 || caronasFeira[0].CaronaID != carona2 {
		t.Fatalf("esperava 1 carona da Expresso-Feira, obteve %d", len(caronasFeira))
	}

	// Busca itinerários e valida que os trechos carregam a empresa parceira
	itin, err := g.BuscarItinerarios("Salvador", "Feira de Santana", "2026-09-20")
	if err != nil || len(itin) != 2 {
		t.Fatalf("esperava 2 itinerarios, obteve %d (err: %v)", len(itin), err)
	}

	if itin[0].Trechos[0].EmpresaID == "" {
		t.Fatalf("esperava que o trecho contivesse EmpresaID preenchido")
	}
}

// Test2PC_CicloCommit valida a Fase 1 (Prepare/Hold) seguida da Fase 2 (Commit/Confirmar)
func Test2PC_CicloCommit(t *testing.T) {
	g, tokenMot, tokenPass1, _ := setupAmbienteTeste(t)

	rota := []string{"Salvador", "Feira de Santana"}
	caronaID, err := g.PublicarCarona(tokenMot, rota, "2026-09-20", "08:00", "09:30", 1, 25.0, "Viacao-RMS")
	if err != nil {
		t.Fatalf("erro ao publicar carona: %v", err)
	}

	trechos := []protocolo.ItemTrecho{
		{CaronaID: caronaID, Origem: "Salvador", Destino: "Feira de Santana"},
	}

	// Fase 1: Criar Hold (Lease temporário com TTL de 5s)
	ts := consorcio.Timestamp{Tempo: 10, NodoID: "nodo-salvador"}
	hold, err := g.CriarHold(tokenPass1, trechos, 5*time.Second, ts)
	if err != nil {
		t.Fatalf("erro ao criar hold (Fase 1 2PC): %v", err)
	}

	if hold.Status != estado.StatusHoldPreparado {
		t.Fatalf("status do hold deveria ser PREPARADO, obteve %s", hold.Status)
	}

	// Durante o Hold, a vaga deve estar indisponível para novas buscas
	itin, err := g.BuscarItinerarios("Salvador", "Feira de Santana", "2026-09-20")
	if err != nil || len(itin) != 0 {
		t.Fatalf("esperava 0 itinerarios disponiveis durante hold ativo, obteve %d", len(itin))
	}

	// Fase 2: Confirmar Hold (Commit)
	reserva, err := g.ConfirmarHold(tokenPass1, hold.ID)
	if err != nil {
		t.Fatalf("erro ao confirmar hold (Fase 2 2PC): %v", err)
	}

	if reserva.Status != "ATIVA" {
		t.Fatalf("reserva confirmada deveria estar ATIVA, obteve %s", reserva.Status)
	}

	// Valida que o hold mudou para CONFIRMADO
	hConsultado, err := g.ObterHold(hold.ID)
	if err != nil || hConsultado.Status != estado.StatusHoldConfirmado {
		t.Fatalf("esperava status CONFIRMADO no hold, obteve %s", hConsultado.Status)
	}
}

// Test2PC_CicloAbort valida a Fase 1 (Prepare/Hold) seguida da Fase 2 (Abort) liberando vagas
func Test2PC_CicloAbort(t *testing.T) {
	g, tokenMot, tokenPass1, _ := setupAmbienteTeste(t)

	rota := []string{"Salvador", "Feira de Santana"}
	caronaID, err := g.PublicarCarona(tokenMot, rota, "2026-09-20", "08:00", "09:30", 1, 25.0)
	if err != nil {
		t.Fatalf("erro ao publicar carona: %v", err)
	}

	trechos := []protocolo.ItemTrecho{
		{CaronaID: caronaID, Origem: "Salvador", Destino: "Feira de Santana"},
	}

	// Fase 1: Criar Hold
	ts := consorcio.Timestamp{Tempo: 10, NodoID: "nodo-salvador"}
	hold, err := g.CriarHold(tokenPass1, trechos, 5*time.Second, ts)
	if err != nil {
		t.Fatalf("erro ao criar hold: %v", err)
	}

	// Fase 2: Abortar Hold
	err = g.AbortarHold(tokenPass1, hold.ID)
	if err != nil {
		t.Fatalf("erro ao abortar hold: %v", err)
	}

	hConsultado, err := g.ObterHold(hold.ID)
	if err != nil || hConsultado.Status != estado.StatusHoldAbortado {
		t.Fatalf("esperava status ABORTADO no hold, obteve %s", hConsultado.Status)
	}

	// Após abortar, a vaga deve voltar imediatamente para a busca
	itin, err := g.BuscarItinerarios("Salvador", "Feira de Santana", "2026-09-20")
	if err != nil || len(itin) != 1 {
		t.Fatalf("esperava 1 itinerario livre apos abort, obteve %d", len(itin))
	}
}

// Test2PC_BloqueioConcorrente valida a concorrência massiva de múltiplos clientes disputando 1 vaga via Hold
func Test2PC_BloqueioConcorrente(t *testing.T) {
	g := estado.NovoGerenciadorEstado()

	_ = g.CadastrarUsuario("mot", "123", "Motorista", protocolo.PapelMotorista)
	tokenMot, _, _, _ := g.Autenticar("mot", "123")

	rota := []string{"Salvador", "Feira de Santana"}
	caronaID, err := g.PublicarCarona(tokenMot, rota, "2026-09-20", "08:00", "09:30", 1, 30.0)
	if err != nil {
		t.Fatalf("erro ao publicar carona: %v", err)
	}

	trechos := []protocolo.ItemTrecho{
		{CaronaID: caronaID, Origem: "Salvador", Destino: "Feira de Santana"},
	}

	totalConcorrentes := 20
	tokens := make([]string, totalConcorrentes)
	for i := 0; i < totalConcorrentes; i++ {
		login := fmt.Sprintf("pass_%d", i)
		_ = g.CadastrarUsuario(login, "123", login, protocolo.PapelPassageiro)
		tok, _, _, _ := g.Autenticar(login, "123")
		tokens[i] = tok
	}

	var wg sync.WaitGroup
	var holdsCriados int32
	var errosCount int32

	// Disparo concorrente
	wg.Add(totalConcorrentes)
	for i := 0; i < totalConcorrentes; i++ {
		go func(idx int) {
			defer wg.Done()
			// Todos usando o mesmo timestamp de Lamport para disputar a mesma vaga
			ts := consorcio.Timestamp{Tempo: 50, NodoID: "nodo-competidor"}
			h, err := g.CriarHold(tokens[idx], trechos, 2*time.Second, ts)
			if err == nil && h != nil {
				atomic.AddInt32(&holdsCriados, 1)
			} else {
				atomic.AddInt32(&errosCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// Apenas 1 hold pode ter sido concedido e 19 recusados por esgotamento
	if holdsCriados != 1 {
		t.Fatalf("esperava exatamente 1 hold concedido na disputa concorrente, obteve %d (erros: %d)", holdsCriados, errosCount)
	}
	if errosCount != int32(totalConcorrentes-1) {
		t.Fatalf("esperava %d erros de recusa, obteve %d", totalConcorrentes-1, errosCount)
	}
}

// Test2PC_DesempatePorLamport_Preempcao valida que transação logicamente anterior preempta hold não commitado
func Test2PC_DesempatePorLamport_Preempcao(t *testing.T) {
	g, tokenMot, tokenPass1, tokenPass2 := setupAmbienteTeste(t)

	rota := []string{"Salvador", "Feira de Santana"}
	caronaID, err := g.PublicarCarona(tokenMot, rota, "2026-09-20", "08:00", "09:30", 1, 25.0)
	if err != nil {
		t.Fatalf("erro ao publicar carona: %v", err)
	}

	trechos := []protocolo.ItemTrecho{
		{CaronaID: caronaID, Origem: "Salvador", Destino: "Feira de Santana"},
	}

	// 1. Passageiro 1 adquire o hold com timestamp lógico MAIOR (Tempo: 10, Nodo: nodo-salvador)
	ts1 := consorcio.Timestamp{Tempo: 10, NodoID: "nodo-salvador"}
	hold1, err := g.CriarHold(tokenPass1, trechos, 5*time.Second, ts1)
	if err != nil {
		t.Fatalf("erro ao criar hold 1: %v", err)
	}
	if hold1.Status != estado.StatusHoldPreparado {
		t.Fatalf("esperava status PREPARADO no hold 1")
	}

	// 2. Passageiro 2 chega com timestamp lógico MENOR (Tempo: 5, Nodo: nodo-feira) - precedência estrita!
	ts2 := consorcio.Timestamp{Tempo: 5, NodoID: "nodo-feira"}
	hold2, err := g.CriarHold(tokenPass2, trechos, 5*time.Second, ts2)
	if err != nil {
		t.Fatalf("esperava que o hold 2 vencesse pelo desempate de Lamport (5 < 10): %v", err)
	}

	// 3. Verifica se o hold 1 foi preemptado
	h1Atualizado, err := g.ObterHold(hold1.ID)
	if err != nil || h1Atualizado.Status != estado.StatusHoldPreemptado {
		t.Fatalf("esperava que o hold 1 ficasse PREEMPTADO, obteve %s", h1Atualizado.Status)
	}

	// 4. Passageiro 1 tenta confirmar o hold preemptado -> DEVE FALHAR
	_, err = g.ConfirmarHold(tokenPass1, hold1.ID)
	if err == nil {
		t.Fatalf("passageiro 1 nao deveria conseguir confirmar um hold preemptado!")
	}

	// 5. Passageiro 2 confirma o hold vencedor com sucesso
	reserva2, err := g.ConfirmarHold(tokenPass2, hold2.ID)
	if err != nil || reserva2.Status != "ATIVA" {
		t.Fatalf("passageiro 2 deveria confirmar a reserva com sucesso: %v", err)
	}
}

// Test2PC_DesempatePorLamport_Rejeicao valida que requisição com Lamport maior é rejeitada
func Test2PC_DesempatePorLamport_Rejeicao(t *testing.T) {
	g, tokenMot, tokenPass1, tokenPass2 := setupAmbienteTeste(t)

	rota := []string{"Salvador", "Feira de Santana"}
	caronaID, _ := g.PublicarCarona(tokenMot, rota, "2026-09-20", "08:00", "09:30", 1, 25.0)

	trechos := []protocolo.ItemTrecho{
		{CaronaID: caronaID, Origem: "Salvador", Destino: "Feira de Santana"},
	}

	// Hold 1 com Lamport (5, nodo-salvador)
	ts1 := consorcio.Timestamp{Tempo: 5, NodoID: "nodo-salvador"}
	_, err := g.CriarHold(tokenPass1, trechos, 5*time.Second, ts1)
	if err != nil {
		t.Fatalf("erro ao criar hold 1: %v", err)
	}

	// Hold 2 com Lamport maior (12, nodo-feira) tenta pegar a mesma vaga esgotada
	ts2 := consorcio.Timestamp{Tempo: 12, NodoID: "nodo-feira"}
	_, err = g.CriarHold(tokenPass2, trechos, 5*time.Second, ts2)
	if err == nil {
		t.Fatalf("hold 2 com Lamport maior deveria ter sido recusado por conflito concorrente!")
	}
}

// Test2PC_DesempatePorLamport_NodoID valida desempate lexicográfico por NodoID quando tempos escalares são iguais
func Test2PC_DesempatePorLamport_NodoID(t *testing.T) {
	g, tokenMot, tokenPass1, tokenPass2 := setupAmbienteTeste(t)

	rota := []string{"Salvador", "Feira de Santana"}
	caronaID, _ := g.PublicarCarona(tokenMot, rota, "2026-09-20", "08:00", "09:30", 1, 25.0)

	trechos := []protocolo.ItemTrecho{
		{CaronaID: caronaID, Origem: "Salvador", Destino: "Feira de Santana"},
	}

	// Hold 1 com Tempo 5, "nodo-salvador"
	ts1 := consorcio.Timestamp{Tempo: 5, NodoID: "nodo-salvador"}
	hold1, _ := g.CriarHold(tokenPass1, trechos, 5*time.Second, ts1)

	// Hold 2 com Tempo 5, "nodo-feira" ("nodo-feira" < "nodo-salvador")
	ts2 := consorcio.Timestamp{Tempo: 5, NodoID: "nodo-feira"}
	hold2, err := g.CriarHold(tokenPass2, trechos, 5*time.Second, ts2)
	if err != nil {
		t.Fatalf("esperava que 'nodo-feira' vencesse o desempate lexicográfico: %v", err)
	}

	h1, _ := g.ObterHold(hold1.ID)
	if h1.Status != estado.StatusHoldPreemptado {
		t.Fatalf("hold1 deveria ter sido preemptado pelo criterio lexicografico")
	}

	h2, _ := g.ObterHold(hold2.ID)
	if h2.Status != estado.StatusHoldPreparado {
		t.Fatalf("hold2 deveria estar em PREPARADO")
	}
}

// Test2PC_ConfirmarHoldInterno_RetentativaEIdempotente valida que confirmar o MESMO hold duas vezes
// (simulando um coordenador reenviando o Commit porque a resposta da primeira tentativa se perdeu
// na rede, mesmo o commit original tendo sido aplicado com sucesso) devolve a reserva já criada em
// vez de um erro - sem isso, a retentativa de commit do coordenador (pkg/consorcio/coordenador.go)
// trataria uma transação na verdade bem-sucedida como uma falha parcial.
func Test2PC_ConfirmarHoldInterno_RetentativaEIdempotente(t *testing.T) {
	g, tokenMot, tokenPass1, _ := setupAmbienteTeste(t)

	rota := []string{"Salvador", "Feira de Santana"}
	caronaID, err := g.PublicarCarona(tokenMot, rota, "2026-09-20", "08:00", "09:30", 1, 25.0)
	if err != nil {
		t.Fatalf("erro ao publicar carona: %v", err)
	}

	trechos := []protocolo.ItemTrecho{{CaronaID: caronaID, Origem: "Salvador", Destino: "Feira de Santana"}}
	ts := consorcio.Timestamp{Tempo: 10, NodoID: "nodo-salvador"}
	hold, err := g.CriarHold(tokenPass1, trechos, 5*time.Second, ts)
	if err != nil {
		t.Fatalf("erro ao criar hold: %v", err)
	}

	reservaPrimeiraTentativa, err := g.ConfirmarHold(tokenPass1, hold.ID)
	if err != nil {
		t.Fatalf("primeira confirmacao deveria ter sucesso: %v", err)
	}

	// Retentativa: mesmo HoldID, simula o reenvio do Commit apos uma resposta perdida na rede.
	reservaSegundaTentativa, err := g.ConfirmarHold(tokenPass1, hold.ID)
	if err != nil {
		t.Fatalf("retentativa de confirmacao do mesmo hold deveria ser idempotente, nao retornar erro: %v", err)
	}

	if reservaSegundaTentativa.ID != reservaPrimeiraTentativa.ID {
		t.Fatalf("retentativa deveria devolver a MESMA reserva (%s), obteve uma diferente (%s)", reservaPrimeiraTentativa.ID, reservaSegundaTentativa.ID)
	}

	// Confirma que a retentativa nao debitou a vaga uma segunda vez nem duplicou o passageiro no trecho.
	itin, _ := g.BuscarItinerarios("Salvador", "Feira de Santana", "2026-09-20")
	if len(itin) != 0 {
		t.Fatalf("vaga nao deveria estar disponivel (ja ocupada por uma unica reserva), obteve %d itinerarios", len(itin))
	}
}

// Test2PC_ExpiracaoAutomaticaAssentosOrfaos valida a rotina de limpeza periódica em background
func Test2PC_ExpiracaoAutomaticaAssentosOrfaos(t *testing.T) {
	g, tokenMot, tokenPass1, _ := setupAmbienteTeste(t)

	rota := []string{"Salvador", "Feira de Santana"}
	caronaID, _ := g.PublicarCarona(tokenMot, rota, "2026-09-20", "08:00", "09:30", 1, 25.0)

	trechos := []protocolo.ItemTrecho{
		{CaronaID: caronaID, Origem: "Salvador", Destino: "Feira de Santana"},
	}

	// Inicia a rotina de limpeza com checagem a cada 20ms
	g.IniciarRotinaExpiracao(20 * time.Millisecond)
	defer g.PararRotinaExpiracao()

	// Cria Hold com TTL curto de 60ms
	ts := consorcio.Timestamp{Tempo: 1, NodoID: "nodo-salvador"}
	hold, err := g.CriarHold(tokenPass1, trechos, 60*time.Millisecond, ts)
	if err != nil {
		t.Fatalf("erro ao criar hold: %v", err)
	}

	// Aguarda o TTL expirar e a rotina rodar (120ms)
	time.Sleep(120 * time.Millisecond)

	// Valida se o status mudou para EXPIRADO
	h, err := g.ObterHold(hold.ID)
	if err != nil || h.Status != estado.StatusHoldExpirado {
		t.Fatalf("esperava status EXPIRADO pela rotina automatica, obteve %s (err: %v)", h.Status, err)
	}

	// Valida que o assento órfão foi devolvido ao inventário livre
	itin, err := g.BuscarItinerarios("Salvador", "Feira de Santana", "2026-09-20")
	if err != nil || len(itin) != 1 {
		t.Fatalf("esperava que a vaga orfa voltasse a ficar disponivel apos expiracao automatica! Obteve %d", len(itin))
	}
}
