package consorcio

import (
	"sync"

	"vaijunto/pkg/protocolo"
)

// Timestamp é um alias para protocolo.Timestamp, mantendo compatibilidade de API e desacoplando pacotes.
type Timestamp = protocolo.Timestamp

// Comparar avalia dois timestamps de Lamport retornando -1, 0 ou 1.
func Comparar(t1, t2 Timestamp) int {
	return protocolo.Comparar(t1, t2)
}

// RelogioLamport gerencia o relógio lógico local de um processo/nó de forma thread-safe.
type RelogioLamport struct {
	mu     sync.RWMutex
	nodoID string
	tempo  uint64
}

// NovoRelogioLamport cria uma nova instância de relógio para o nó com um tempo inicial especificado.
func NovoRelogioLamport(nodoID string, tempoInicial uint64) *RelogioLamport {
	return &RelogioLamport{
		nodoID: nodoID,
		tempo:  tempoInicial,
	}
}

// Tick incrementa monotonicamente o relógio local antes de um evento interno ou disparo de mensagem.
// Regra de Lamport: L = L + 1.
func (r *RelogioLamport) Tick() Timestamp {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tempo++
	return Timestamp{
		Tempo:  r.tempo,
		NodoID: r.nodoID,
	}
}

// Atualizar avança o relógio local a partir do timestamp recebido em uma mensagem de outro nó.
// Regra de Lamport: L = max(L, L_recebido) + 1.
func (r *RelogioLamport) Atualizar(tempoRecebido uint64) Timestamp {
	r.mu.Lock()
	defer r.mu.Unlock()
	if tempoRecebido > r.tempo {
		r.tempo = tempoRecebido
	}
	r.tempo++
	return Timestamp{
		Tempo:  r.tempo,
		NodoID: r.nodoID,
	}
}

// ObterTimestamp retorna uma cópia atômica do timestamp atual do nó.
func (r *RelogioLamport) ObterTimestamp() Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return Timestamp{
		Tempo:  r.tempo,
		NodoID: r.nodoID,
	}
}

// ObterTempo retorna apenas o valor numérico do relógio local de forma segura.
func (r *RelogioLamport) ObterTempo() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tempo
}

// NodoID retorna o identificador do nó associado ao relógio.
func (r *RelogioLamport) NodoID() string {
	return r.nodoID
}
