package protocolo

import "fmt"

// Timestamp representa uma tupla escalar de Lamport com identificador do nó para ordenação total.
type Timestamp struct {
	Tempo  uint64 `json:"tempo"`
	NodoID string `json:"nodo_id"`
}

// MenorQue implementa a regra clássica de precedência total de Lamport (1978):
// Um evento (T1, Nodo1) precede (T2, Nodo2) se:
//  1. T1 < T2; ou
//  2. T1 == T2 e Nodo1 < Nodo2 (desempate determinístico lexicográfico).
func (t Timestamp) MenorQue(outro Timestamp) bool {
	if t.Tempo != outro.Tempo {
		return t.Tempo < outro.Tempo
	}
	return t.NodoID < outro.NodoID
}

// MaiorQue verifica se este timestamp sucede o outro pela regra de precedência total.
func (t Timestamp) MaiorQue(outro Timestamp) bool {
	if t.Tempo != outro.Tempo {
		return t.Tempo > outro.Tempo
	}
	return t.NodoID > outro.NodoID
}

// Igual verifica se ambos os timestamps possuem o mesmo valor lógico e pertencem ao mesmo nó.
func (t Timestamp) Igual(outro Timestamp) bool {
	return t.Tempo == outro.Tempo && t.NodoID == outro.NodoID
}

// String formata o timestamp para exibição textual e depuração.
func (t Timestamp) String() string {
	return fmt.Sprintf("(L:%d, Nodo:%s)", t.Tempo, t.NodoID)
}

// Comparar avalia dois timestamps de Lamport retornando:
//   -1 se t1 precede t2
//    0 se t1 e t2 são equivalentes
//    1 se t1 sucede t2
func Comparar(t1, t2 Timestamp) int {
	if t1.MenorQue(t2) {
		return -1
	}
	if t1.MaiorQue(t2) {
		return 1
	}
	return 0
}
