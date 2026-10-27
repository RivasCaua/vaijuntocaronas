package consorcio_test

import (
	"sort"
	"sync"
	"testing"

	"vaijunto/pkg/consorcio"
)

// TestRelogioLamport_TickMonotonico verifica se eventos locais avançam o relógio estritamente
func TestRelogioLamport_TickMonotonico(t *testing.T) {
	rel := consorcio.NovoRelogioLamport("nodo-salvador", 0)

	for i := uint64(1); i <= 5; i++ {
		ts := rel.Tick()
		if ts.Tempo != i {
			t.Fatalf("esperado tempo %d após Tick, obtido %d", i, ts.Tempo)
		}
		if ts.NodoID != "nodo-salvador" {
			t.Fatalf("esperado NodoID 'nodo-salvador', obtido '%s'", ts.NodoID)
		}
	}

	if rel.ObterTempo() != 5 {
		t.Fatalf("esperado tempo final 5, obtido %d", rel.ObterTempo())
	}
}

// TestRelogioLamport_Atualizar valida a regra de Lamport L = max(L_local, L_recebido) + 1
func TestRelogioLamport_Atualizar(t *testing.T) {
	rel := consorcio.NovoRelogioLamport("nodo-feira", 5)

	// Caso 1: tempo recebido menor que o local (3 < 5) -> L = 5 + 1 = 6
	ts1 := rel.Atualizar(3)
	if ts1.Tempo != 6 {
		t.Fatalf("caso 1: esperado tempo 6, obtido %d", ts1.Tempo)
	}

	// Caso 2: tempo recebido maior que o local (15 > 6) -> L = 15 + 1 = 16
	ts2 := rel.Atualizar(15)
	if ts2.Tempo != 16 {
		t.Fatalf("caso 2: esperado tempo 16, obtido %d", ts2.Tempo)
	}

	// Caso 3: tempo recebido igual ao local (16 == 16) -> L = 16 + 1 = 17
	ts3 := rel.Atualizar(16)
	if ts3.Tempo != 17 {
		t.Fatalf("caso 3: esperado tempo 17, obtido %d", ts3.Tempo)
	}
}

// TestRelogioLamport_PrecedenciaTotal valida a ordenação total estrita (desempate por ID)
func TestRelogioLamport_PrecedenciaTotal(t *testing.T) {
	// 1. Diferença de tempo escalar
	tA := consorcio.Timestamp{Tempo: 5, NodoID: "nodo-salvador"}
	tB := consorcio.Timestamp{Tempo: 6, NodoID: "nodo-salvador"}

	if !tA.MenorQue(tB) {
		t.Errorf("esperava tA < tB por ter tempo menor (5 < 6)")
	}
	if consorcio.Comparar(tA, tB) != -1 {
		t.Errorf("Comparar(tA, tB) deveria ser -1, obtido %d", consorcio.Comparar(tA, tB))
	}

	// 2. Empate no tempo escalar -> Desempate lexicográfico por NodoID
	tC := consorcio.Timestamp{Tempo: 5, NodoID: "nodo-feira"}
	tD := consorcio.Timestamp{Tempo: 5, NodoID: "nodo-salvador"}

	if !tC.MenorQue(tD) {
		t.Errorf("esperava tC < tD pelo desempate lexicográfico ('nodo-feira' < 'nodo-salvador')")
	}
	if !tD.MaiorQue(tC) {
		t.Errorf("esperava tD > tC")
	}
	if consorcio.Comparar(tC, tD) != -1 {
		t.Errorf("Comparar(tC, tD) deveria ser -1")
	}

	// 3. Timestamps idênticos
	tE := consorcio.Timestamp{Tempo: 5, NodoID: "nodo-feira"}
	if !tC.Igual(tE) {
		t.Errorf("esperava tC == tE")
	}
	if consorcio.Comparar(tC, tE) != 0 {
		t.Errorf("Comparar de timestamps iguais deveria ser 0")
	}

	// 4. Ordenação completa de uma lista não ordenada de eventos
	lista := []consorcio.Timestamp{
		{Tempo: 10, NodoID: "nodo-salvador"},
		{Tempo: 5, NodoID: "nodo-reconcavo"},
		{Tempo: 5, NodoID: "nodo-feira"},
		{Tempo: 2, NodoID: "nodo-salvador"},
		{Tempo: 10, NodoID: "nodo-feira"},
	}

	sort.Slice(lista, func(i, j int) bool {
		return lista[i].MenorQue(lista[j])
	})

	esperados := []consorcio.Timestamp{
		{Tempo: 2, NodoID: "nodo-salvador"},
		{Tempo: 5, NodoID: "nodo-feira"},
		{Tempo: 5, NodoID: "nodo-reconcavo"},
		{Tempo: 10, NodoID: "nodo-feira"},
		{Tempo: 10, NodoID: "nodo-salvador"},
	}

	for i, ts := range lista {
		if !ts.Igual(esperados[i]) {
			t.Fatalf("posição %d: esperado %s, obtido %s", i, esperados[i], ts)
		}
	}
}

// TestRelogioLamport_Concorrencia submete o relógio a dezenas de goroutines simultâneas
func TestRelogioLamport_Concorrencia(t *testing.T) {
	rel := consorcio.NovoRelogioLamport("nodo-concorrente", 0)
	var wg sync.WaitGroup

	numGoroutines := 30
	iteracoes := 500

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iteracoes; i++ {
				if id%2 == 0 {
					rel.Tick()
				} else {
					rel.Atualizar(uint64(i * 2))
				}
				_ = rel.ObterTimestamp()
			}
		}(g)
	}

	wg.Wait()

	if rel.ObterTempo() == 0 {
		t.Fatalf("relógio não deveria ser zero após execução concorrente")
	}
}
