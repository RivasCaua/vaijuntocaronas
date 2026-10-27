package consorcio_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"vaijunto/pkg/consorcio"
)

// TestWAL_CriacaoEEscrita valida a persistência sequencial básica no disco
func TestWAL_CriacaoEEscrita(t *testing.T) {
	dir := t.TempDir()
	caminho := filepath.Join(dir, "wal.log")

	w, err := consorcio.NovoWAL(caminho)
	if err != nil {
		t.Fatalf("falha ao criar WAL: %v", err)
	}
	defer w.Fechar()

	ts1 := consorcio.Timestamp{Tempo: 1, NodoID: "nodo-salvador"}
	reg1, err := w.Gravar(consorcio.OpCadastrarUsuario, []byte(`{"login":"motorista1"}`), ts1)
	if err != nil {
		t.Fatalf("falha ao gravar registro 1: %v", err)
	}
	if reg1.Indice != 1 {
		t.Fatalf("esperado índice 1, obtido %d", reg1.Indice)
	}

	ts2 := consorcio.Timestamp{Tempo: 2, NodoID: "nodo-salvador"}
	reg2, err := w.Gravar(consorcio.OpPublicarCarona, []byte(`{"carona_id":"C1"}`), ts2)
	if err != nil {
		t.Fatalf("falha ao gravar registro 2: %v", err)
	}
	if reg2.Indice != 2 {
		t.Fatalf("esperado índice 2, obtido %d", reg2.Indice)
	}

	if w.UltimoIndice() != 2 {
		t.Fatalf("esperado último índice 2, obtido %d", w.UltimoIndice())
	}
}

// TestWAL_RecuperacaoIntegridade valida a restauração das entradas via replay e filtros de índice
func TestWAL_RecuperacaoIntegridade(t *testing.T) {
	dir := t.TempDir()
	caminho := filepath.Join(dir, "wal.log")

	w, err := consorcio.NovoWAL(caminho)
	if err != nil {
		t.Fatalf("falha ao criar WAL: %v", err)
	}

	_, _ = w.Gravar(consorcio.OpCadastrarUsuario, []byte(`{"login":"user1"}`), consorcio.Timestamp{Tempo: 1, NodoID: "nodo-feira"})
	_, _ = w.Gravar(consorcio.OpPublicarCarona, []byte(`{"carona_id":"C100"}`), consorcio.Timestamp{Tempo: 2, NodoID: "nodo-feira"})
	_, _ = w.Gravar(consorcio.OpReservar, []byte(`{"reserva_id":"R100"}`), consorcio.Timestamp{Tempo: 3, NodoID: "nodo-salvador"})

	// Replay completo
	registros, err := w.Recuperar()
	if err != nil {
		t.Fatalf("falha na recuperação: %v", err)
	}

	if len(registros) != 3 {
		t.Fatalf("esperado 3 registros, obtidos %d", len(registros))
	}

	if registros[0].Operacao != consorcio.OpCadastrarUsuario || registros[0].Indice != 1 {
		t.Errorf("registro 0 incompatível: %+v", registros[0])
	}
	if registros[1].Operacao != consorcio.OpPublicarCarona || registros[1].Indice != 2 {
		t.Errorf("registro 1 incompatível: %+v", registros[1])
	}
	if registros[2].Operacao != consorcio.OpReservar || registros[2].Indice != 3 {
		t.Errorf("registro 2 incompatível: %+v", registros[2])
	}

	// Filtro incremental (após índice 1)
	apos1, err := w.RecuperarAposIndice(1)
	if err != nil {
		t.Fatalf("falha ao filtrar pós índice 1: %v", err)
	}
	if len(apos1) != 2 {
		t.Fatalf("esperado 2 registros após índice 1, obtidos %d", len(apos1))
	}
	if apos1[0].Indice != 2 || apos1[1].Indice != 3 {
		t.Fatalf("índices incorretos no filtro incremental: %d, %d", apos1[0].Indice, apos1[1].Indice)
	}

	_ = w.Fechar()
}

// TestWAL_DeteccaoCorrupcaoCRC32 injeta deliberadamente uma corrupção de byte no arquivo físico
// e comprova que a validação do CRC32 rejeita o log corrompido
func TestWAL_DeteccaoCorrupcaoCRC32(t *testing.T) {
	dir := t.TempDir()
	caminho := filepath.Join(dir, "wal_corrompido.log")

	w, err := consorcio.NovoWAL(caminho)
	if err != nil {
		t.Fatalf("erro ao criar WAL: %v", err)
	}

	_, err = w.Gravar(consorcio.OpCadastrarUsuario, []byte(`{"login":"alvo_corrupcao"}`), consorcio.Timestamp{Tempo: 1, NodoID: "nodo-1"})
	if err != nil {
		t.Fatalf("erro ao gravar: %v", err)
	}
	_ = w.Fechar()

	// Lê o arquivo cru do disco
	conteudo, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatalf("erro ao ler arquivo físico: %v", err)
	}

	// Injeta corrupção alterando um caractere no meio do arquivo (sem alterar o campo de checksum)
	textoCorrompido := strings.Replace(string(conteudo), "alvo_corrupcao", "alvo_HACKEADO", 1)
	if textoCorrompido == string(conteudo) {
		t.Fatalf("falha ao injetar corrupção sintética")
	}

	err = os.WriteFile(caminho, []byte(textoCorrompido), 0644)
	if err != nil {
		t.Fatalf("erro ao sobrescrever arquivo com corrupção: %v", err)
	}

	// Tentar reabrir o WAL corrompido deve falhar imediatamente na validação de integridade
	_, errReabertura := consorcio.NovoWAL(caminho)
	if errReabertura == nil {
		t.Fatalf("esperava erro de corrupção detectado pelo CRC32, mas abriu com sucesso")
	}

	if !strings.Contains(errReabertura.Error(), "corrupção") && !strings.Contains(errReabertura.Error(), "checksum") {
		t.Fatalf("mensagem de erro inesperada: %v", errReabertura)
	}
}

// TestWAL_ReaberturaEContinuidade valida persistência entre reinicializações do processo
func TestWAL_ReaberturaEContinuidade(t *testing.T) {
	dir := t.TempDir()
	caminho := filepath.Join(dir, "wal_persistente.log")

	// Sessão 1: grava 2 registros e fecha
	w1, err := consorcio.NovoWAL(caminho)
	if err != nil {
		t.Fatalf("erro ao abrir w1: %v", err)
	}
	_, _ = w1.Gravar(consorcio.OpCadastrarUsuario, []byte(`{"user":"u1"}`), consorcio.Timestamp{Tempo: 1, NodoID: "nodo-1"})
	_, _ = w1.Gravar(consorcio.OpCadastrarUsuario, []byte(`{"user":"u2"}`), consorcio.Timestamp{Tempo: 2, NodoID: "nodo-1"})
	_ = w1.Fechar()

	// Sessão 2: reabre o mesmo arquivo físico
	w2, err := consorcio.NovoWAL(caminho)
	if err != nil {
		t.Fatalf("erro ao reabrir w2: %v", err)
	}
	defer w2.Fechar()

	if w2.UltimoIndice() != 2 {
		t.Fatalf("esperado que w2 continuasse do índice 2, obtido %d", w2.UltimoIndice())
	}

	reg3, err := w2.Gravar(consorcio.OpPublicarCarona, []byte(`{"carona":"c1"}`), consorcio.Timestamp{Tempo: 3, NodoID: "nodo-1"})
	if err != nil {
		t.Fatalf("erro ao gravar em w2: %v", err)
	}

	if reg3.Indice != 3 {
		t.Fatalf("esperado que novo registro tivesse índice 3, obtido %d", reg3.Indice)
	}

	todos, err := w2.Recuperar()
	if err != nil {
		t.Fatalf("erro ao recuperar após reabertura: %v", err)
	}
	if len(todos) != 3 {
		t.Fatalf("esperado 3 registros totais, obtidos %d", len(todos))
	}
}

// TestWAL_Limpar valida truncamento seguro e reset do arquivo
func TestWAL_Limpar(t *testing.T) {
	dir := t.TempDir()
	caminho := filepath.Join(dir, "wal_limpeza.log")

	w, err := consorcio.NovoWAL(caminho)
	if err != nil {
		t.Fatalf("erro ao abrir: %v", err)
	}
	defer w.Fechar()

	_, _ = w.Gravar(consorcio.OpCadastrarUsuario, []byte(`{"user":"teste"}`), consorcio.Timestamp{Tempo: 1, NodoID: "nodo-1"})
	if w.UltimoIndice() != 1 {
		t.Fatalf("esperava índice 1")
	}

	if err := w.Limpar(); err != nil {
		t.Fatalf("erro ao limpar WAL: %v", err)
	}

	if w.UltimoIndice() != 0 {
		t.Fatalf("esperava índice resetado para 0, obtido %d", w.UltimoIndice())
	}

	rec, err := w.Recuperar()
	if err != nil {
		t.Fatalf("erro ao recuperar pós limpeza: %v", err)
	}
	if len(rec) != 0 {
		t.Fatalf("esperava lista vazia pós limpeza, obtidos %d", len(rec))
	}
}

// TestWAL_Concorrencia estressa o WAL com múltiplas goroutines concorrentes gravando em disco
func TestWAL_Concorrencia(t *testing.T) {
	dir := t.TempDir()
	caminho := filepath.Join(dir, "wal_concorrente.log")

	w, err := consorcio.NovoWAL(caminho)
	if err != nil {
		t.Fatalf("erro ao abrir: %v", err)
	}
	defer w.Fechar()

	numGoroutines := 20
	registrosPorGoroutine := 25
	totalEsperado := numGoroutines * registrosPorGoroutine

	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < registrosPorGoroutine; i++ {
				payload, _ := json.Marshal(map[string]any{
					"goroutine": gid,
					"iteracao":  i,
				})
				ts := consorcio.Timestamp{Tempo: uint64(i + 1), NodoID: fmt.Sprintf("nodo-%d", gid)}
				_, errGravar := w.Gravar(consorcio.OpPublicarCarona, payload, ts)
				if errGravar != nil {
					t.Errorf("falha ao gravar concorrentemente: %v", errGravar)
				}
			}
		}(g)
	}

	wg.Wait()

	if w.UltimoIndice() != uint64(totalEsperado) {
		t.Fatalf("esperado %d registros gravados, obtido %d", totalEsperado, w.UltimoIndice())
	}

	// Recupera e valida integridade de todos os registros concorrentes
	todos, err := w.Recuperar()
	if err != nil {
		t.Fatalf("erro na recuperação concorrente: %v", err)
	}

	if len(todos) != totalEsperado {
		t.Fatalf("esperado %d registros recuperados, obtido %d", totalEsperado, len(todos))
	}

	// Valida se os índices estão estritamente ordenados de 1 até totalEsperado
	for i, reg := range todos {
		esperadoIndice := uint64(i + 1)
		if reg.Indice != esperadoIndice {
			t.Fatalf("registro %d possui índice desalinhado: %d", i, reg.Indice)
		}
	}
}
