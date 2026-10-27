package estado_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"vaijunto/pkg/consorcio"
	"vaijunto/pkg/estado"
	"vaijunto/pkg/protocolo"
)

// TestPersistenciaJSON_SalvarECarregar valida o ciclo de salvar o estado em JSON e restaurar em nova instância
func TestPersistenciaJSON_SalvarECarregar(t *testing.T) {
	tmpDir := t.TempDir()
	caminhoJSON := filepath.Join(tmpDir, "estado_teste.json")

	g1 := estado.NovoGerenciadorEstado()

	// 1. Cadastra dados no g1
	_ = g1.CadastrarUsuario("mot_pers", "123", "Motorista Pers", protocolo.PapelMotorista)
	_ = g1.CadastrarUsuario("pass_pers", "123", "Passageiro Pers", protocolo.PapelPassageiro)

	tokMot, _, _, _ := g1.Autenticar("mot_pers", "123")
	tokPass, _, _, _ := g1.Autenticar("pass_pers", "123")

	rota := []string{"Salvador", "Amélia Rodrigues", "Feira de Santana"}
	caronaID, err := g1.PublicarCarona(tokMot, rota, "2026-09-20", "08:00", "09:30", 2, 25.0, "Viacao-RMS")
	if err != nil {
		t.Fatalf("erro ao publicar carona: %v", err)
	}

	trechos := []protocolo.ItemTrecho{
		{CaronaID: caronaID, Origem: "Salvador", Destino: "Amélia Rodrigues"},
	}

	// Cria uma reserva direta
	_, err = g1.ReservarItinerario(tokPass, trechos)
	if err != nil {
		t.Fatalf("erro ao reservar itinerario: %v", err)
	}

	// Cria um hold ativo no segundo trecho
	trechosHold := []protocolo.ItemTrecho{
		{CaronaID: caronaID, Origem: "Amélia Rodrigues", Destino: "Feira de Santana"},
	}
	ts := consorcio.Timestamp{Tempo: 15, NodoID: "nodo-salvador"}
	hold, err := g1.CriarHold(tokPass, trechosHold, 10*time.Minute, ts)
	if err != nil {
		t.Fatalf("erro ao criar hold: %v", err)
	}

	// 2. Salva em disco em formato JSON
	err = g1.SalvarJSON(caminhoJSON)
	if err != nil {
		t.Fatalf("erro ao salvar estado em JSON: %v", err)
	}

	// Valida que o arquivo foi realmente criado no disco
	info, err := os.Stat(caminhoJSON)
	if err != nil || info.Size() == 0 {
		t.Fatalf("arquivo JSON de estado nao existe ou esta vazio: %v", err)
	}

	// 3. Cria uma NOVA instância do gerenciador a partir do arquivo JSON
	g2, err := estado.NovoGerenciadorEstadoComPersistencia(caminhoJSON)
	if err != nil {
		t.Fatalf("erro ao inicializar g2 a partir do JSON salvo: %v", err)
	}

	// 4. Valida se todos os dados foram reconstituídos com integridade
	if g2.ObterTotalUsuarios() != 2 {
		t.Fatalf("esperava 2 usuarios restaurados, obteve %d", g2.ObterTotalUsuarios())
	}

	if g2.ObterTotalCaronas() != 1 {
		t.Fatalf("esperava 1 carona restaurada, obteve %d", g2.ObterTotalCaronas())
	}

	emp, err := g2.ObterEmpresaCarona(caronaID)
	if err != nil || emp != "Viacao-RMS" {
		t.Fatalf("esperava empresa 'Viacao-RMS', obteve '%s'", emp)
	}

	if g2.ObterTotalReservas() != 1 {
		t.Fatalf("esperava 1 reserva restaurada, obteve %d", g2.ObterTotalReservas())
	}

	if g2.ObterTotalHolds() != 1 {
		t.Fatalf("esperava 1 hold restaurado, obteve %d", g2.ObterTotalHolds())
	}

	// Autentica com as credenciais salvas e confirma o hold em g2
	tokPass2, _, _, err := g2.Autenticar("pass_pers", "123")
	if err != nil {
		t.Fatalf("esperava autenticacao bem-sucedida em g2: %v", err)
	}

	reserva2, err := g2.ConfirmarHold(tokPass2, hold.ID)
	if err != nil || reserva2 == nil {
		t.Fatalf("esperava confirmar hold restaurado em g2 com sucesso: %v", err)
	}

	if g2.ObterTotalReservas() != 2 {
		t.Fatalf("esperava 2 reservas apos confirmacao do hold em g2, obteve %d", g2.ObterTotalReservas())
	}
}

// TestPersistenciaJSON_HoldsExpiradosNoCarregamento valida que holds vencidos durante o downtime sao limpos ao carregar
func TestPersistenciaJSON_HoldsExpiradosNoCarregamento(t *testing.T) {
	tmpDir := t.TempDir()
	caminhoJSON := filepath.Join(tmpDir, "estado_expirado.json")

	g1 := estado.NovoGerenciadorEstado()

	_ = g1.CadastrarUsuario("mot", "123", "Motorista", protocolo.PapelMotorista)
	_ = g1.CadastrarUsuario("pass", "123", "Passageiro", protocolo.PapelPassageiro)

	tokMot, _, _, _ := g1.Autenticar("mot", "123")
	tokPass, _, _, _ := g1.Autenticar("pass", "123")

	rota := []string{"Salvador", "Feira de Santana"}
	caronaID, _ := g1.PublicarCarona(tokMot, rota, "2026-09-20", "08:00", "09:30", 1, 30.0)

	trechos := []protocolo.ItemTrecho{
		{CaronaID: caronaID, Origem: "Salvador", Destino: "Feira de Santana"},
	}

	// Cria Hold com TTL muito curto (30ms)
	ts := consorcio.Timestamp{Tempo: 1, NodoID: "nodo-salvador"}
	hold, err := g1.CriarHold(tokPass, trechos, 30*time.Millisecond, ts)
	if err != nil {
		t.Fatalf("erro ao criar hold: %v", err)
	}

	// Salva em disco imediatamente
	_ = g1.SalvarJSON(caminhoJSON)

	// Aguarda o TTL expirar simulando downtime do servidor
	time.Sleep(80 * time.Millisecond)

	// Carrega em nova instância g2: a rotina de boot deve limpar o hold expirado e devolver a vaga!
	g2 := estado.NovoGerenciadorEstado()
	err = g2.CarregarJSON(caminhoJSON)
	if err != nil {
		t.Fatalf("erro ao carregar JSON: %v", err)
	}

	h2, err := g2.ObterHold(hold.ID)
	if err != nil || h2.Status != estado.StatusHoldExpirado {
		t.Fatalf("esperava que o hold estivesse EXPIRADO ao recarregar, obteve %s", h2.Status)
	}

	// O assento deve estar disponível novamente para reservas
	itin, err := g2.BuscarItinerarios("Salvador", "Feira de Santana", "2026-09-20")
	if err != nil || len(itin) != 1 {
		t.Fatalf("esperava que o assento estivesse livre apos recuperar do snapshot com hold expirado! Obteve %d", len(itin))
	}
}
