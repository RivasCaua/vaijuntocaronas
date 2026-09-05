package protocolo

const (
	// Constantes de ações para mensagens
	AcaoCadastrar = "CADASTRAR"
	AcaoAutenticar = "AUTENTICAR"
)

const (
	// Constantes para o status da resposta 
	StatusOK = "OK"
	StatusErro = "ERRO"
)

const (
	// Constantes para o papel do usuário
	PapelMotorista = "motorista"
	PapelPassageiro = "passageiro"
)

type Requisicao struct {
	// Dados enviados do cliente para o servidor
	Acao string `json:"acao"`
	Usuario string `json:"usuario,omitempty"`
	Senha string `json:"senha,omitempty"`
	Nome string `json:"nome,omitempty"`
	Papel string `json:"papel,omitempty"`
	Token string `json:"token,omitempty"`
}

type Resposta struct {
	// Mensagem enviada do servidor para o cliente
	Status string `json:"status"`
	Mensagem string `json:"mensagem,omitempty"`
	Erro string `json:"erro,omitempty"`
	Token string `json:"token,omitempty"`
	Papel string `json:"papel,omitempty"`
	Nome string `json:"nome,omitempty"`
}




