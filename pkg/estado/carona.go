package estado

import (
	"time"

	"vaijunto/pkg/protocolo"
)

// Constantes de status de Holds / Leases temporários para 2PC
const (
	StatusHoldPreparado  = "PREPARADO"  // Fase 1 concluída, assento bloqueado aguardando Commit
	StatusHoldConfirmado = "CONFIRMADO" // Fase 2 concluída (Commit), convertido em Reserva definitiva
	StatusHoldAbortado   = "ABORTADO"   // Fase 2 cancelada (Abort), assento liberado
	StatusHoldExpirado   = "EXPIRADO"   // TTL esgotado sem confirmação, assento órfão recuperado
	StatusHoldPreemptado = "PREEMPTADO" // Preemptado por transação concorrente com Lamport menor
)

// Carona representa uma oferta de viagem cadastrada por um motorista vinculada a uma empresa parceira
type Carona struct {
	ID                   string              `json:"id"`
	EmpresaID            string              `json:"empresa_id,omitempty"`  // Empresa parceira proprietária da carona
	MotoristaLogin       string              `json:"motorista_login"`
	Rota                 []string            `json:"rota"`                  // Ex: ["Salvador", "Amélia Rodrigues", "Feira de Santana"]
	Data                 string              `json:"data"`                  // Ex: "2026-09-20"
	Horario              string              `json:"horario"`               // Ex: "08:00" (Partida)
	HorarioChegada       string              `json:"horario_chegada"`       // Ex: "09:30" (Previsão de chegada)
	AssentosTotais       int                 `json:"assentos_totais"`        // Capacidade total por trecho
	PrecoPorTrecho       float64             `json:"preco_por_trecho"`       // Valor de cada trecho individual
	VagasPorTrecho       map[string]int      `json:"vagas_por_trecho"`       // Chave: "Salvador->Amélia Rodrigues", Valor: vagas livres
	PassageirosPorTrecho map[string][]string `json:"passageiros_por_trecho"` // Chave: "Salvador->Amélia Rodrigues", Valor: logins
	Ativa                bool                `json:"ativa"`
}

// ItemTrechoReserva mapeia o assento consumido em uma carona
type ItemTrechoReserva struct {
	CaronaID  string  `json:"carona_id"`
	EmpresaID string  `json:"empresa_id,omitempty"`
	Origem    string  `json:"origem"`
	Destino   string  `json:"destino"`
	Preco     float64 `json:"preco"`
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

// Hold representa um bloqueio temporário (Lease com TTL) para a Fase 1 do Two-Phase Commit (2PC).
type Hold struct {
	ID               string              `json:"id"`
	PassageiroLogin  string              `json:"passageiro_login"`
	Trechos          []ItemTrechoReserva `json:"trechos"`
	PrecoTotal       float64             `json:"preco_total"`
	Data             string              `json:"data"`
	TimestampLamport protocolo.Timestamp `json:"timestamp_lamport"` // Para desempate determinístico
	Status           string              `json:"status"`            // PREPARADO, CONFIRMADO, ABORTADO, EXPIRADO, PREEMPTADO
	CriadoEm         time.Time           `json:"criado_em"`
	ExpiraEm         time.Time           `json:"expira_em"`
}

