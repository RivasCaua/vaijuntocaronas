package consorcio

import (
	"encoding/json"
	"time"

	"vaijunto/pkg/protocolo"
)

// Constantes de identificação de nós e regiões do consórcio
const (
	RegiaoRMS            = "RMS"
	RegiaoPortalDoSertao = "Portal do Sertão"
	RegiaoReconcavo      = "Recôncavo"

	NodoSalvador  = "nodo-salvador"
	NodoFeira     = "nodo-feira"
	NodoReconcavo = "nodo-reconcavo"

	StatusNodoOnline   = "ONLINE"
	StatusNodoSuspeito = "SUSPEITO"
	StatusNodoOffline  = "OFFLINE"
)

// Constantes de tipos de mensagens trocadas entre nós do consórcio
const (
	MsgRequisicaoCS   = "REQUISICAO_CS"   // Solicitação de entrada em Seção Crítica (Ricart-Agrawala / Lamport)
	MsgRespostaCS     = "RESPOSTA_CS"     // Autorização/OK concedido para Seção Crítica
	MsgLiberacaoCS    = "LIBERACAO_CS"    // Notificação de saída da Seção Crítica
	MsgProposta       = "PROPOSTA"        // Proposta de nova transação para replicação
	MsgVoto           = "VOTO"            // Voto positivo/negativo sobre uma transação
	MsgCommit         = "COMMIT"          // Confirmação definitiva de transação
	MsgAbort          = "ABORT"           // Aborto/rejeição de transação
	MsgHeartbeat      = "HEARTBEAT"       // Batimento cardíaco para detecção de falhas
	MsgSincronizarWAL = "SINCRONIZAR_WAL" // Solicitação de entradas defasadas do log
)

// Constantes de operações de domínio replicadas no consórcio
const (
	OpCadastrarUsuario = "CADASTRAR_USUARIO"
	OpPublicarCarona   = "PUBLICAR_CARONA"
	OpReservar         = "RESERVAR"
	OpCancelarReserva  = "CANCELAR_RESERVA"
	OpCancelarCarona   = "CANCELAR_CARONA"
)

// Constantes de status de transação
const (
	StatusTransacaoPendente  = "PENDENTE"
	StatusTransacaoCommitada = "COMMITADA"
	StatusTransacaoAbortada  = "ABORTADA"
)

// NodoDTO representa as propriedades de rede, região e estado de um membro do consórcio.
type NodoDTO struct {
	ID          string    `json:"id"`
	Endereco    string    `json:"endereco"`     // Ex: "localhost", "192.168.1.10"
	PortaREST   int       `json:"porta_rest"`   // Porta HTTP REST de comunicação inter-nós
	PortaTCP    int       `json:"porta_tcp"`    // Porta TCP de atendimento a clientes (motoristas/passageiros)
	Regiao      string    `json:"regiao"`       // Região geográfica de atuação
	Cidades     []string  `json:"cidades"`      // Catálogo de cidades sob responsabilidade do nó
	Status      string    `json:"status"`       // "ONLINE", "SUSPEITO", "OFFLINE"
	UltimoVisto time.Time `json:"ultimo_visto"` // Timestamp físico do último heartbeat recebido
}

// InfoClusterDTO consolida a topologia visível do consórcio no ponto de vista do nó consultado.
type InfoClusterDTO struct {
	NodoLocal    NodoDTO   `json:"nodo_local"`
	Membros      []NodoDTO `json:"membros"`
	LiderAtual   string    `json:"lider_atual,omitempty"`
	LamportLocal Timestamp `json:"lamport_local"`
	TotalMembros int       `json:"total_membros"`
}

// MensagemConsorcioDTO envelope padrão para comunicação REST entre os nós do cluster.
type MensagemConsorcioDTO struct {
	ID               string          `json:"id"`                        // Identificador único da mensagem (UUID/hash)
	Tipo             string          `json:"tipo"`                      // MsgRequisicaoCS, MsgProposta, etc.
	RemetenteID      string          `json:"remetente_id"`              // ID do nó originador
	DestinatarioID   string          `json:"destinatario_id,omitempty"` // Vazio para difusão/broadcast
	TimestampLamport Timestamp       `json:"timestamp_lamport"`         // Relógio lógico do remetente
	Dados            json.RawMessage `json:"dados,omitempty"`           // Payload específico da mensagem
	CriadoEm         time.Time       `json:"criado_em"`
}

// TransacaoConsorcioDTO representa uma transação de mutação de estado submetida ao consórcio.
type TransacaoConsorcioDTO struct {
	IDTransacao      string          `json:"id_transacao"`
	TipoOp           string          `json:"tipo_op"`           // OpPublicarCarona, OpReservar, etc.
	TimestampLamport Timestamp       `json:"timestamp_lamport"` // Ordem total definida pelo consórcio
	OrigemNodoID     string          `json:"origem_nodo_id"`    // Nó que atendeu o cliente inicialmente
	Status           string          `json:"status"`            // PENDENTE, COMMITADA, ABORTADA
	Payload          json.RawMessage `json:"payload"`           // DTO do domínio serializado
	CriadoEm         time.Time       `json:"criado_em"`
}

// Payloads de domínio serializáveis para as transações do consórcio

// PayloadCadastroUsuarioDTO dados de cadastro sincronizados entre nós
type PayloadCadastroUsuarioDTO struct {
	Login string `json:"login"`
	Senha string `json:"senha"`
	Nome  string `json:"nome"`
	Papel string `json:"papel"`
}

// PayloadPublicarCaronaDTO dados de oferta de carona a ser gravada no consórcio
type PayloadPublicarCaronaDTO struct {
	CaronaID       string   `json:"carona_id"`
	EmpresaID      string   `json:"empresa_id,omitempty"`
	MotoristaLogin string   `json:"motorista_login"`
	Rota           []string `json:"rota"`
	Data           string   `json:"data"`
	Horario        string   `json:"horario"`
	HorarioChegada string   `json:"horario_chegada"`
	Assentos       int      `json:"assentos"`
	PrecoPorTrecho float64  `json:"preco_por_trecho"`
}

// PayloadReservarDTO dados de reserva atômica coordenada via consórcio
type PayloadReservarDTO struct {
	ReservaID       string                 `json:"reserva_id"`
	PassageiroLogin string                 `json:"passageiro_login"`
	Trechos         []protocolo.ItemTrecho `json:"trechos"`
	PrecoTotal      float64                `json:"preco_total"`
	Data            string                 `json:"data"`
}

// PayloadCancelarReservaDTO dados para cancelamento de reserva e devolução de vagas
type PayloadCancelarReservaDTO struct {
	ReservaID       string `json:"reserva_id"`
	PassageiroLogin string `json:"passageiro_login"`
}

// PayloadCancelarCaronaDTO dados para cancelamento de carona pelo motorista
type PayloadCancelarCaronaDTO struct {
	CaronaID       string `json:"carona_id"`
	MotoristaLogin string `json:"motorista_login"`
}

// HeartbeatDTO payload periódico de liveness transmitido entre os membros
type HeartbeatDTO struct {
	RemetenteID      string    `json:"remetente_id"`
	Regiao           string    `json:"regiao"`
	TimestampLamport Timestamp `json:"timestamp_lamport"`
	UltimoIndiceWAL  uint64    `json:"ultimo_indice_wal"`
	EnviadoEm        time.Time `json:"enviado_em"`
}

// SincronizacaoWALReqDTO requisição para recuperação de log defasado
type SincronizacaoWALReqDTO struct {
	NodoSolicitanteID     string    `json:"nodo_solicitante_id"`
	UltimoIndiceConhecido uint64    `json:"ultimo_indice_conhecido"`
	TimestampLamport      Timestamp `json:"timestamp_lamport"`
}

// SincronizacaoWALRespDTO resposta contendo lote de registros do WAL para replay
type SincronizacaoWALRespDTO struct {
	RemetenteID      string         `json:"remetente_id"`
	IndiceInicial    uint64         `json:"indice_inicial"`
	IndiceFinal      uint64         `json:"indice_final"`
	Registros        []*RegistroWAL `json:"registros"`
	TimestampLamport Timestamp      `json:"timestamp_lamport"`
}

// RespostaREST envelope padronizado para respostas da API HTTP
type RespostaREST struct {
	Sucesso   bool       `json:"sucesso"`
	Mensagem  string     `json:"mensagem,omitempty"`
	Dados     any        `json:"dados,omitempty"`
	Timestamp *Timestamp `json:"timestamp,omitempty"`
}

// --- DTOs para busca federada (Scatter-Gather / Dispersar-Coletar) ---

// BuscaItinerariosReqDTO representa a consulta distribuída de viagens
type BuscaItinerariosReqDTO struct {
	Origem           string    `json:"origem"`
	Destino          string    `json:"destino"`
	Data             string    `json:"data"`
	SolicitanteID    string    `json:"solicitante_id"`
	TimestampLamport Timestamp `json:"timestamp_lamport"`
}

// BuscaItinerariosRespDTO resposta com itinerários encontrados pelo nó consultado
type BuscaItinerariosRespDTO struct {
	NodoID           string                    `json:"nodo_id"`
	Regiao           string                    `json:"regiao"`
	Itinerarios      []protocolo.ItinerarioDTO `json:"itinerarios"`
	TimestampLamport Timestamp                 `json:"timestamp_lamport"`
}

// --- DTOs para Orquestração Two-Phase Commit (2PC) Inter-Servidores ---

// PrepareReqDTO requisição da Fase 1 (Prepare / Vote Request)
type PrepareReqDTO struct {
	TransacaoID      string                 `json:"transacao_id"`
	CoordenadorID    string                 `json:"coordenador_id"`
	PassageiroLogin  string                 `json:"passageiro_login"`
	Trechos          []protocolo.ItemTrecho `json:"trechos"`
	TTLMillis        int64                  `json:"ttl_millis"`
	TimestampLamport Timestamp              `json:"timestamp_lamport"`
}

// PrepareRespDTO resposta com voto da Fase 1 (Voto SIM/NÃO + HoldID gerado)
type PrepareRespDTO struct {
	TransacaoID      string    `json:"transacao_id"`
	ParticipanteID   string    `json:"participante_id"`
	Voto             bool      `json:"voto"` // true = SIM (PREPARED), false = NÃO (ABORT)
	HoldID           string    `json:"hold_id,omitempty"`
	Motivo           string    `json:"motivo,omitempty"`
	TimestampLamport Timestamp `json:"timestamp_lamport"`
}

// CommitReqDTO requisição da Fase 2 (Commit Definitivo)
type CommitReqDTO struct {
	TransacaoID      string    `json:"transacao_id"`
	CoordenadorID    string    `json:"coordenador_id"`
	HoldID           string    `json:"hold_id"`
	PassageiroLogin  string    `json:"passageiro_login"`
	TimestampLamport Timestamp `json:"timestamp_lamport"`
}

// CommitRespDTO resposta da Fase 2 (Commit confirmado com ID de reserva)
type CommitRespDTO struct {
	TransacaoID      string    `json:"transacao_id"`
	ParticipanteID   string    `json:"participante_id"`
	Sucesso          bool      `json:"sucesso"`
	ReservaID        string    `json:"reserva_id,omitempty"`
	Data             string    `json:"data,omitempty"` // data da viagem confirmada, usada pelo coordenador para consolidar a reserva final
	Mensagem         string    `json:"mensagem,omitempty"`
	TimestampLamport Timestamp `json:"timestamp_lamport"`
}

// AbortReqDTO requisição da Fase 2 (Abort / Rollback / Compensação de assentos)
type AbortReqDTO struct {
	TransacaoID      string    `json:"transacao_id"`
	CoordenadorID    string    `json:"coordenador_id"`
	HoldID           string    `json:"hold_id"`
	PassageiroLogin  string    `json:"passageiro_login"`
	Motivo           string    `json:"motivo,omitempty"`
	TimestampLamport Timestamp `json:"timestamp_lamport"`
}

// AbortRespDTO resposta da Fase 2 (Abort executado com sucesso)
type AbortRespDTO struct {
	TransacaoID      string    `json:"transacao_id"`
	ParticipanteID   string    `json:"participante_id"`
	Sucesso          bool      `json:"sucesso"`
	Mensagem         string    `json:"mensagem,omitempty"`
	TimestampLamport Timestamp `json:"timestamp_lamport"`
}
