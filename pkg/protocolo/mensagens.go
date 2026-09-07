package protocolo

const (
	// Constantes de ações para mensagens
	AcaoCadastrar         = "CADASTRAR"
	AcaoAutenticar        = "AUTENTICAR"
	AcaoPublicarCarona    = "PUBLICAR_CARONA"
	AcaoBuscarItinerarios = "BUSCAR_ITINERARIOS"
	AcaoReservar          = "RESERVAR"
	AcaoListarCaronas     = "LISTAR_CARONAS"
	AcaoListarReservas    = "LISTAR_RESERVAS"
	AcaoCancelarReserva   = "CANCELAR_RESERVA"
	AcaoCancelarCarona    = "CANCELAR_CARONA"
	AcaoObterCidades      = "OBTER_CIDADES"
)

const (
	// Constantes para o status da resposta 
	StatusOK   = "OK"
	StatusErro = "ERRO"
)

const (
	// Constantes para o papel do usuário
	PapelMotorista  = "motorista"
	PapelPassageiro = "passageiro"
)

// ItemTrecho representa um segmento reservável (CaronaID + Origem + Destino)
type ItemTrecho struct {
	CaronaID string  `json:"carona_id"`
	Origem   string  `json:"origem"`
	Destino  string  `json:"destino"`
	Preco    float64 `json:"preco,omitempty"`
}

// ItinerarioDTO representa uma opção de viagem (pode combinar 1 ou mais trechos)
type ItinerarioDTO struct {
	Trechos    []ItemTrecho `json:"trechos"`
	PrecoTotal float64      `json:"preco_total"`
}

// TrechoCaronaDTO detalha a ocupação por sub-trecho de uma carona
type TrechoCaronaDTO struct {
	Trecho      string   `json:"trecho"`
	Origem      string   `json:"origem"`
	Destino     string   `json:"destino"`
	VagasLivres int      `json:"vagas_livres"`
	Passageiros []string `json:"passageiros"`
}

// CaronaDTO representa os dados públicos de uma carona cadastrada
type CaronaDTO struct {
	CaronaID       string            `json:"carona_id"`
	Motorista      string            `json:"motorista,omitempty"`
	Rota           []string          `json:"rota"`
	Data           string            `json:"data"`
	Horario        string            `json:"horario"`
	Assentos       int               `json:"assentos"`
	PrecoPorTrecho float64           `json:"preco_por_trecho"`
	Trechos        []TrechoCaronaDTO `json:"trechos,omitempty"`
}

// ReservaDTO representa uma reserva realizada por um passageiro
type ReservaDTO struct {
	ReservaID  string       `json:"reserva_id"`
	Passageiro string       `json:"passageiro,omitempty"`
	Trechos    []ItemTrecho `json:"trechos"`
	PrecoTotal float64      `json:"preco_total"`
	Data       string       `json:"data"`
	Status     string       `json:"status"`
}

type Requisicao struct {
	// Dados enviados do cliente para o servidor
	Acao           string       `json:"acao"`
	Usuario        string       `json:"usuario,omitempty"`
	Senha          string       `json:"senha,omitempty"`
	Nome           string       `json:"nome,omitempty"`
	Papel          string       `json:"papel,omitempty"`
	Token          string       `json:"token,omitempty"`
	Rota           []string     `json:"rota,omitempty"`
	Data           string       `json:"data,omitempty"`
	Horario        string       `json:"horario,omitempty"`
	Assentos       int          `json:"assentos,omitempty"`
	PrecoPorTrecho float64      `json:"preco_por_trecho,omitempty"`
	Origem         string       `json:"origem,omitempty"`
	Destino        string       `json:"destino,omitempty"`
	CaronaID       string       `json:"carona_id,omitempty"`
	ReservaID      string       `json:"reserva_id,omitempty"`
	Trechos        []ItemTrecho `json:"trechos,omitempty"`
}

type Resposta struct {
	// Mensagem enviada do servidor para o cliente
	Status      string          `json:"status"`
	Mensagem    string          `json:"mensagem,omitempty"`
	Erro        string          `json:"erro,omitempty"`
	Token       string          `json:"token,omitempty"`
	Papel       string          `json:"papel,omitempty"`
	Nome        string          `json:"nome,omitempty"`
	CaronaID    string          `json:"carona_id,omitempty"`
	ReservaID   string          `json:"reserva_id,omitempty"`
	Cidades     []string        `json:"cidades,omitempty"`
	Itinerarios []ItinerarioDTO `json:"itinerarios,omitempty"`
	Caronas     []CaronaDTO     `json:"caronas,omitempty"`
	Reservas    []ReservaDTO    `json:"reservas,omitempty"`
}