package estado

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"vaijunto/pkg/protocolo"
)

// GerenciadorEstado mantem todos os dados em memoria com controle de concorrencia
type GerenciadorEstado struct {
	mu       sync.RWMutex        // Trava para evitar condicao de corrida entre multiplos clientes
	usuarios map[string]*Usuario // Mapa de login -> Usuario
	sessoes  map[string]*Sessao  // Mapa de token -> Sessao
}

// NovoGerenciadorEstado inicializa a estrutura do gerenciador
func NovoGerenciadorEstado() *GerenciadorEstado {
	return &GerenciadorEstado{
		usuarios: make(map[string]*Usuario),
		sessoes:  make(map[string]*Sessao),
	}
}

// gerarTokenSeguro cria um token hexadecimal aleatorio de 128 bits
func gerarTokenSeguro() (string, error) {
	bytes := make([]byte, 16)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// CadastrarUsuario adiciona um novo motorista ou passageiro de forma segura
func (g *GerenciadorEstado) CadastrarUsuario(login, senha, nome, papel string) error {
	login = strings.TrimSpace(login)
	senha = strings.TrimSpace(senha)
	nome = strings.TrimSpace(nome)
	papel = strings.ToLower(strings.TrimSpace(papel))

	// Validacoes
	if login == "" || senha == "" || nome == "" || papel == "" {
		return errors.New("todos os campos sao obrigatorios")
	}

	if papel != protocolo.PapelMotorista && papel != protocolo.PapelPassageiro {
		return fmt.Errorf("papel invalido: '%s'. Deve ser '%s' ou '%s'", papel, protocolo.PapelMotorista, protocolo.PapelPassageiro)
	}

	// Secao Critica: Exclusao mutua para escrita
	g.mu.Lock()
	defer g.mu.Unlock()

	// Verifica se o usuario ja existe
	if _, existe := g.usuarios[login]; existe {
		return fmt.Errorf("usuario '%s' ja cadastrado", login)
	}

	// Salva o novo usuario no mapa em memoria
	g.usuarios[login] = &Usuario{
		Login: login,
		Senha: senha,
		Nome:  nome,
		Papel: papel,
	}

	return nil
}

// Autenticar valida as credenciais e gera uma nova sessao com token
func (g *GerenciadorEstado) Autenticar(login, senha string) (token string, papel string, nome string, err error) {
	login = strings.TrimSpace(login)
	senha = strings.TrimSpace(senha)

	if login == "" || senha == "" {
		return "", "", "", errors.New("login e senha sao obrigatorios")
	}

	// Bloqueia escrita pois vamos salvar a nova sessao no mapa
	g.mu.Lock()
	defer g.mu.Unlock()

	usuario, existe := g.usuarios[login]
	if !existe || usuario.Senha != senha {
		return "", "", "", errors.New("credenciais invalidas, usuario ou senha incorretos")
	}

	novoToken, err := gerarTokenSeguro()
	if err != nil {
		return "", "", "", fmt.Errorf("falha ao gerar token: %w", err)
	}

	g.sessoes[novoToken] = &Sessao{
		Token:    novoToken,
		Usuario:  usuario,
		CriadoEm: time.Now(),
	}

	return novoToken, usuario.Papel, usuario.Nome, nil
}

// ValidarToken permite fazer a verificacao rapidamente sob trava de leitura (RLock)
func (g *GerenciadorEstado) ValidarToken(token string) (*Usuario, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, errors.New("token e obrigatorio")
	}

	// Trava de leitura: permite leituras simultaneas
	g.mu.RLock()
	defer g.mu.RUnlock()

	sessao, existe := g.sessoes[token]
	if !existe {
		return nil, errors.New("sessao invalida ou expirada")
	}

	return sessao.Usuario, nil
}

// ObterTotalUsuarios retorna o total de usuarios cadastrados
func (g *GerenciadorEstado) ObterTotalUsuarios() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.usuarios)
}