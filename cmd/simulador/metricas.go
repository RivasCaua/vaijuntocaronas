package main

import (
	"sort"
	"sync"
	"time"
)

// acaoStat acumula contadores e amostras de latencia de um unico tipo de operacao (acao de protocolo).
type acaoStat struct {
	mu         sync.Mutex
	tentativas int64
	sucessos   int64
	falhas     int64
	latencias  []time.Duration
}

// Metricas agrega as estatisticas de todas as acoes realizadas durante a simulacao de carga,
// thread-safe para ser compartilhada livremente entre centenas de goroutines de passageiros.
type Metricas struct {
	mu            sync.Mutex
	porAcao       map[string]*acaoStat
	falhasConexao int64
}

// NovasMetricas inicializa o coletor de estatisticas.
func NovasMetricas() *Metricas {
	return &Metricas{porAcao: make(map[string]*acaoStat)}
}

// Registrar contabiliza o resultado (sucesso/falha) e a latencia de uma acao concluida.
func (m *Metricas) Registrar(acao string, sucesso bool, duracao time.Duration) {
	m.mu.Lock()
	s, existe := m.porAcao[acao]
	if !existe {
		s = &acaoStat{}
		m.porAcao[acao] = s
	}
	m.mu.Unlock()

	s.mu.Lock()
	s.tentativas++
	if sucesso {
		s.sucessos++
	} else {
		s.falhas++
	}
	s.latencias = append(s.latencias, duracao)
	s.mu.Unlock()
}

// Totais soma tentativas, sucessos e falhas de todas as acoes registradas ate o momento.
func (m *Metricas) Totais() (total, sucesso, falha int64) {
	m.mu.Lock()
	acoes := make([]*acaoStat, 0, len(m.porAcao))
	for _, s := range m.porAcao {
		acoes = append(acoes, s)
	}
	m.mu.Unlock()

	for _, s := range acoes {
		s.mu.Lock()
		total += s.tentativas
		sucesso += s.sucessos
		falha += s.falhas
		s.mu.Unlock()
	}
	return
}

// NomesOrdenados retorna os nomes das acoes registradas em ordem alfabetica, para impressao estavel.
func (m *Metricas) NomesOrdenados() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	nomes := make([]string, 0, len(m.porAcao))
	for nome := range m.porAcao {
		nomes = append(nomes, nome)
	}
	sort.Strings(nomes)
	return nomes
}

// snapshotAcao consolida os contadores e percentis de latencia de uma unica acao em um instante.
type snapshotAcao struct {
	tentativas, sucessos, falhas int64
	media, p50, p95, p99         float64
}

// snapshot calcula um retrato estatistico da acao nomeada (em milissegundos).
func (m *Metricas) snapshot(acao string) snapshotAcao {
	m.mu.Lock()
	s, existe := m.porAcao[acao]
	m.mu.Unlock()
	if !existe {
		return snapshotAcao{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	out := snapshotAcao{tentativas: s.tentativas, sucessos: s.sucessos, falhas: s.falhas}
	if len(s.latencias) == 0 {
		return out
	}

	ordenadas := make([]time.Duration, len(s.latencias))
	copy(ordenadas, s.latencias)
	sort.Slice(ordenadas, func(i, j int) bool { return ordenadas[i] < ordenadas[j] })

	var soma time.Duration
	for _, d := range ordenadas {
		soma += d
	}
	out.media = msDuration(soma) / float64(len(ordenadas))
	out.p50 = msDuration(percentil(ordenadas, 50))
	out.p95 = msDuration(percentil(ordenadas, 95))
	out.p99 = msDuration(percentil(ordenadas, 99))

	return out
}

// percentil retorna o valor no percentil p (0-100) de um slice ja ordenado de duracoes.
func percentil(ordenadas []time.Duration, p int) time.Duration {
	if len(ordenadas) == 0 {
		return 0
	}
	idx := (p * len(ordenadas)) / 100
	if idx >= len(ordenadas) {
		idx = len(ordenadas) - 1
	}
	return ordenadas[idx]
}

func msDuration(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
