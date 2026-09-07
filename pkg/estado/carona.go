package estado

import "time"

// Carona representa uma oferta de viagem cadastrada por um motorista
type Carona struct {
	ID                   string              `json:"id"`
	MotoristaLogin       string              `json:"motorista_login"`
	Rota                 []string            `json:"rota"`                  // Ex: ["Salvador", "Amélia Rodrigues", "Feira de Santana"]
	Data                 string              `json:"data"`                  // Ex: "2026-09-20"
	Horario              string              `json:"horario"`               // Ex: "08:00"
	AssentosTotais       int                 `json:"assentos_totais"`        // Capacidade total por trecho
	PrecoPorTrecho       float64             `json:"preco_por_trecho"`       // Valor de cada trecho individual
	VagasPorTrecho       map[string]int      `json:"vagas_por_trecho"`       // Chave: "Salvador->Amélia Rodrigues", Valor: vagas livres
	PassageirosPorTrecho map[string][]string `json:"passageiros_por_trecho"` // Chave: "Salvador->Amélia Rodrigues", Valor: logins
	Ativa                bool                `json:"ativa"`
}

// ItemTrechoReserva mapeia o assento consumido em uma carona
type ItemTrechoReserva struct {
	CaronaID string  `json:"carona_id"`
	Origem   string  `json:"origem"`
	Destino  string  `json:"destino"`
	Preco    float64 `json:"preco"`
}

// Reserva registra os trechos confirmados por um passageiro
type Reserva struct {
	ID              string              `json:"id"`
	PassageiroLogin string              `json:"passageiro_login"`
	Trechos         []ItemTrechoReserva `json:"trechos"`
	PrecoTotal      float64             `json:"preco_total"`
	Data            string              `json:"data"`
	Status          string              `json:"status"` // "ATIVA" ou "CANCELADA"
	CriadoEm        time.Time           `json:"criado_em"`
}
