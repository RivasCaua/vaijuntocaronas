package consorcio

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// RegistroWAL representa uma entrada atômica, imutável e verificável no Write-Ahead Log.
type RegistroWAL struct {
	Indice    uint64          `json:"indice"`    // Log Sequence Number (LSN) monotônico crescente
	Timestamp Timestamp       `json:"timestamp"` // Relógio de Lamport associado ao evento
	Operacao  string          `json:"operacao"`  // Nome da operação (ex: PUBLICAR_CARONA)
	Payload   json.RawMessage `json:"payload"`   // Dados brutos da operação serializada
	CriadoEm  time.Time       `json:"criado_em"` // Timestamp físico de persistência
	Checksum  uint32          `json:"checksum"`  // Hash CRC32 IEEE para integridade contra corrupção
}

// WAL gerencia o arquivo append-only em disco com sincronização estrita (fsync) e tolerância a falhas.
type WAL struct {
	mu             sync.Mutex
	caminhoArquivo string
	arquivo        *os.File
	ultimoIndice   uint64
	fechado        bool
}

// calcularChecksum gera o hash CRC32 determinístico sobre os dados canônicos do registro.
func calcularChecksum(indice uint64, ts Timestamp, operacao string, payload []byte) uint32 {
	h := crc32.NewIEEE()
	buf := make([]byte, 8)

	// Indice (8 bytes)
	binary.BigEndian.PutUint64(buf, indice)
	h.Write(buf)

	// Timestamp Lamport Tempo (8 bytes)
	binary.BigEndian.PutUint64(buf, ts.Tempo)
	h.Write(buf)

	// Timestamp Lamport NodoID
	h.Write([]byte(ts.NodoID))

	// Operação
	h.Write([]byte(operacao))

	// Payload
	h.Write(payload)

	return h.Sum32()
}

// NovoWAL inicializa ou reabre um arquivo WAL existente, validando a integridade das entradas prévias.
func NovoWAL(caminhoArquivo string) (*WAL, error) {
	if strings.TrimSpace(caminhoArquivo) == "" {
		return nil, errors.New("caminho do arquivo WAL não pode ser vazio")
	}

	// Assegura criação dos diretórios pais
	dir := filepath.Dir(caminhoArquivo)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("erro ao criar diretório para o WAL (%s): %w", dir, err)
		}
	}

	// Abre o arquivo para leitura, escrita e criação em modo append
	arquivo, err := os.OpenFile(caminhoArquivo, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir arquivo WAL (%s): %w", caminhoArquivo, err)
	}

	w := &WAL{
		caminhoArquivo: caminhoArquivo,
		arquivo:        arquivo,
		ultimoIndice:   0,
		fechado:        false,
	}

	// Escaneia entradas existentes para validar integridade e descobrir o último LSN
	registros, err := w.lerTodosRegistros()
	if err != nil {
		arquivo.Close()
		return nil, fmt.Errorf("falha na validação de integridade do arquivo WAL: %w", err)
	}

	if len(registros) > 0 {
		w.ultimoIndice = registros[len(registros)-1].Indice
	}

	return w, nil
}

// Gravar persiste de forma atômica e durável uma operação no final do log.
// Aplica a chamada obrigatória a fsync (Sync) antes de liberar a gravação.
func (w *WAL) Gravar(operacao string, payload []byte, ts Timestamp) (*RegistroWAL, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fechado {
		return nil, errors.New("não é possível gravar em um WAL já fechado")
	}

	w.ultimoIndice++
	checksum := calcularChecksum(w.ultimoIndice, ts, operacao, payload)

	reg := &RegistroWAL{
		Indice:    w.ultimoIndice,
		Timestamp: ts,
		Operacao:  operacao,
		Payload:   json.RawMessage(payload),
		CriadoEm:  time.Now().UTC(),
		Checksum:  checksum,
	}

	linha, err := json.Marshal(reg)
	if err != nil {
		w.ultimoIndice-- // Reverte o índice em caso de erro de serialização
		return nil, fmt.Errorf("erro ao serializar entrada do WAL: %w", err)
	}
	linha = append(linha, '\n')

	if _, err := w.arquivo.Write(linha); err != nil {
		w.ultimoIndice--
		return nil, fmt.Errorf("erro de I/O ao gravar no arquivo WAL: %w", err)
	}

	// Fsync estrito: garante que o registro foi descarregado fisicamente dos buffers do SO para o disco
	if err := w.arquivo.Sync(); err != nil {
		return nil, fmt.Errorf("erro no fsync do WAL: %w", err)
	}

	return reg, nil
}

// Recuperar lê todas as entradas do início do log, validando o checksum de cada uma delas.
// Utilizado no boot para replay de estado (crash-recovery).
func (w *WAL) Recuperar() ([]*RegistroWAL, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.lerTodosRegistros()
}

// RecuperarAposIndice retorna apenas os registros com LSN maior que o índice fornecido.
// Utilizado para sincronização de nós atrasados na rede.
func (w *WAL) RecuperarAposIndice(indice uint64) ([]*RegistroWAL, error) {
	todos, err := w.Recuperar()
	if err != nil {
		return nil, err
	}

	filtrados := make([]*RegistroWAL, 0)
	for _, reg := range todos {
		if reg.Indice > indice {
			filtrados = append(filtrados, reg)
		}
	}
	return filtrados, nil
}

// lerTodosRegistros realiza a leitura física a partir do byte 0 do arquivo.
// Assume que a trava de exclusão mútua já está segura.
func (w *WAL) lerTodosRegistros() ([]*RegistroWAL, error) {
	if _, err := w.arquivo.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("erro ao posicionar ponteiro no início do WAL: %w", err)
	}

	scanner := bufio.NewScanner(w.arquivo)
	// Suporta registros de até 4MB por linha
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 4*1024*1024)

	var registros []*RegistroWAL
	linhaNum := 0

	for scanner.Scan() {
		linhaNum++
		texto := strings.TrimSpace(scanner.Text())
		if texto == "" {
			continue // Ignora linhas em branco
		}

		var reg RegistroWAL
		if err := json.Unmarshal([]byte(texto), &reg); err != nil {
			return nil, fmt.Errorf("linha %d corrompida: JSON inválido: %w", linhaNum, err)
		}

		// Valida integridade via CRC32
		esperado := calcularChecksum(reg.Indice, reg.Timestamp, reg.Operacao, reg.Payload)
		if reg.Checksum != esperado {
			return nil, fmt.Errorf("corrupção de dados detectada na linha %d (LSN %d): checksum esperado %d, obtido %d",
				linhaNum, reg.Indice, esperado, reg.Checksum)
		}

		registros = append(registros, &reg)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("erro ao escanear arquivo WAL: %w", err)
	}

	// Reposiciona o ponteiro de arquivo para o final para futuras escritas
	if _, err := w.arquivo.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("erro ao reposicionar ponteiro no fim do WAL: %w", err)
	}

	return registros, nil
}

// UltimoIndice retorna o último índice persistido com sucesso no WAL.
func (w *WAL) UltimoIndice() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ultimoIndice
}

// Limpar trunca completamente o arquivo WAL, resetando o índice para 0.
func (w *WAL) Limpar() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fechado {
		return errors.New("não é possível limpar um WAL fechado")
	}

	if err := w.arquivo.Truncate(0); err != nil {
		return fmt.Errorf("falha ao truncar arquivo WAL: %w", err)
	}
	if _, err := w.arquivo.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("falha ao reposicionar ponteiro: %w", err)
	}
	if err := w.arquivo.Sync(); err != nil {
		return fmt.Errorf("falha ao sincronizar arquivo após limpeza: %w", err)
	}

	w.ultimoIndice = 0
	return nil
}

// Fechar descarrega os buffers pendentes e encerra o descritor de arquivo de forma segura.
func (w *WAL) Fechar() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fechado {
		return nil
	}

	w.fechado = true
	_ = w.arquivo.Sync()
	return w.arquivo.Close()
}
