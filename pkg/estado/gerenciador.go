package estado

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"vaijunto/pkg/protocolo"
)

// GerenciadorEstado mantem todos os dados em memoria com controle de concorrencia
type GerenciadorEstado struct {
	mu           sync.RWMutex        // Trava para evitar condicao de corrida entre multiplos clientes
	usuarios     map[string]*Usuario // Mapa de login -> Usuario
	sessoes      map[string]*Sessao  // Mapa de token -> Sessao
	caronas      map[string]*Carona  // Mapa de ID -> Carona
	reservas     map[string]*Reserva // Mapa de ID -> Reserva
	seqCaronaId  int
	seqReservaId int
}

// NovoGerenciadorEstado inicializa a estrutura do gerenciador
func NovoGerenciadorEstado() *GerenciadorEstado {
	return &GerenciadorEstado{
		usuarios: make(map[string]*Usuario),
		sessoes:  make(map[string]*Sessao),
		caronas:  make(map[string]*Carona),
		reservas: make(map[string]*Reserva),
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

	if login == "" || senha == "" || nome == "" || papel == "" {
		return errors.New("todos os campos sao obrigatorios")
	}

	if papel != protocolo.PapelMotorista && papel != protocolo.PapelPassageiro {
		return fmt.Errorf("papel invalido: '%s'", papel)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if _, existe := g.usuarios[login]; existe {
		return fmt.Errorf("usuario '%s' ja cadastrado", login)
	}

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

// PublicarCarona desmembra a rota em trechos e inicializa as vagas por trecho
func (g *GerenciadorEstado) PublicarCarona(token string, rota []string, data, horario, horarioChegada string, assentos int, preco float64) (string, error) {
	u, err := g.ValidarToken(token)
	if err != nil {
		return "", err
	}
	if u.Papel != protocolo.PapelMotorista {
		return "", errors.New("apenas motoristas podem publicar caronas")
	}

	rotaValida, err := ValidarRota(rota)
	if err != nil {
		return "", err
	}

	if assentos <= 0 || preco <= 0 {
		return "", errors.New("assentos e preco por trecho devem ser maiores que zero")
	}

	horario = strings.TrimSpace(horario)
	horarioChegada = strings.TrimSpace(horarioChegada)

	g.mu.Lock()
	defer g.mu.Unlock()

	g.seqCaronaId++
	caronaID := fmt.Sprintf("c-%d", g.seqCaronaId)

	vagasPorTrecho := make(map[string]int)
	passageirosPorTrecho := make(map[string][]string)

	for i := 0; i < len(rotaValida)-1; i++ {
		chave := fmt.Sprintf("%s->%s", rotaValida[i], rotaValida[i+1])
		vagasPorTrecho[chave] = assentos
		passageirosPorTrecho[chave] = make([]string, 0)
	}

	g.caronas[caronaID] = &Carona{
		ID:                   caronaID,
		MotoristaLogin:       u.Login,
		Rota:                 rotaValida,
		Data:                 data,
		Horario:              horario,
		HorarioChegada:       horarioChegada,
		AssentosTotais:       assentos,
		PrecoPorTrecho:       preco,
		VagasPorTrecho:       vagasPorTrecho,
		PassageirosPorTrecho: passageirosPorTrecho,
		Ativa:                true,
	}

	return caronaID, nil
}

// BuscarItinerarios encontra combinacoes de trechos com vagas disponiveis e ordena por prioridade (menor preco)
func (g *GerenciadorEstado) BuscarItinerarios(origem, destino, data string) ([]protocolo.ItinerarioDTO, error) {
	origemNorm, okOrig := NormalizarCidade(origem)
	destinoNorm, okDest := NormalizarCidade(destino)
	if !okOrig || !okDest {
		return nil, errors.New("origem ou destino invalidos no catalogo regional")
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	var itinerarios []protocolo.ItinerarioDTO

	for _, c := range g.caronas {
		if !c.Ativa || c.Data != data {
			continue
		}

		idxOrig := -1
		idxDest := -1
		for i, city := range c.Rota {
			if city == origemNorm && idxOrig == -1 {
				idxOrig = i
			}
			if city == destinoNorm && idxOrig != -1 {
				idxDest = i
				break
			}
		}

		if idxOrig != -1 && idxDest != -1 && idxOrig < idxDest {
			temVagaTodas := true
			var trechos []protocolo.ItemTrecho
			precoTotal := 0.0

			for i := idxOrig; i < idxDest; i++ {
				o := c.Rota[i]
				d := c.Rota[i+1]
				chave := fmt.Sprintf("%s->%s", o, d)

				if c.VagasPorTrecho[chave] <= 0 {
					temVagaTodas = false
					break
				}

				trechos = append(trechos, protocolo.ItemTrecho{
					CaronaID:       c.ID,
					Origem:         o,
					Destino:        d,
					Preco:          c.PrecoPorTrecho,
					HorarioPartida: c.Horario,
					HorarioChegada: c.HorarioChegada,
				})
				precoTotal += c.PrecoPorTrecho
			}

			if temVagaTodas {
				itinerarios = append(itinerarios, protocolo.ItinerarioDTO{
					Trechos:        trechos,
					PrecoTotal:     precoTotal,
					HorarioPartida: c.Horario,
					HorarioChegada: c.HorarioChegada,
				})
			}
		}
	}

	sort.Slice(itinerarios, func(i, j int) bool {
		if itinerarios[i].PrecoTotal != itinerarios[j].PrecoTotal {
			return itinerarios[i].PrecoTotal < itinerarios[j].PrecoTotal
		}
		if len(itinerarios[i].Trechos) != len(itinerarios[j].Trechos) {
			return len(itinerarios[i].Trechos) < len(itinerarios[j].Trechos)
		}
		return itinerarios[i].HorarioPartida < itinerarios[j].HorarioPartida
	})

	return itinerarios, nil
}

// ReservarItinerario executa o Check-Then-Act sob Lock() de forma atômica
func (g *GerenciadorEstado) ReservarItinerario(token string, trechosSolicitados []protocolo.ItemTrecho) (string, error) {
	u, err := g.ValidarToken(token)
	if err != nil {
		return "", err
	}

	if len(trechosSolicitados) == 0 {
		return "", errors.New("nenhum trecho informado para reserva")
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// FASE 1 (CHECK): Verifica se TODOS os trechos possuem vaga
	for _, item := range trechosSolicitados {
		carona, existe := g.caronas[item.CaronaID]
		if !existe || !carona.Ativa {
			return "", fmt.Errorf("carona %s nao disponivel", item.CaronaID)
		}

		chave := fmt.Sprintf("%s->%s", item.Origem, item.Destino)
		if carona.VagasPorTrecho[chave] <= 0 {
			return "", fmt.Errorf("sem vagas no trecho %s", chave)
		}
	}

	// FASE 2 (ACT): Debita as vagas de todos os trechos
	itemsReserva := make([]ItemTrechoReserva, 0, len(trechosSolicitados))
	precoTotal := 0.0
	dataViagem := ""

	for _, item := range trechosSolicitados {
		carona := g.caronas[item.CaronaID]
		chave := fmt.Sprintf("%s->%s", item.Origem, item.Destino)

		carona.VagasPorTrecho[chave]--
		carona.PassageirosPorTrecho[chave] = append(carona.PassageirosPorTrecho[chave], u.Login)

		precoTotal += carona.PrecoPorTrecho
		dataViagem = carona.Data

		itemsReserva = append(itemsReserva, ItemTrechoReserva{
			CaronaID: item.CaronaID,
			Origem:   item.Origem,
			Destino:  item.Destino,
			Preco:    carona.PrecoPorTrecho,
		})
	}

	g.seqReservaId++
	reservaID := fmt.Sprintf("r-%d", g.seqReservaId)

	g.reservas[reservaID] = &Reserva{
		ID:              reservaID,
		PassageiroLogin: u.Login,
		Trechos:         itemsReserva,
		PrecoTotal:      precoTotal,
		Data:            dataViagem,
		Status:          "ATIVA",
		CriadoEm:        time.Now(),
	}

	return reservaID, nil
}

// ListarCaronasMotorista retorna as caronas cadastradas pelo motorista com detalhes dos trechos
func (g *GerenciadorEstado) ListarCaronasMotorista(token string) ([]protocolo.CaronaDTO, error) {
	u, err := g.ValidarToken(token)
	if err != nil {
		return nil, err
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	var caronasDTO []protocolo.CaronaDTO

	for _, c := range g.caronas {
		if c.MotoristaLogin == u.Login && c.Ativa {
			var trechosDTO []protocolo.TrechoCaronaDTO
			for i := 0; i < len(c.Rota)-1; i++ {
				o := c.Rota[i]
				d := c.Rota[i+1]
				chave := fmt.Sprintf("%s->%s", o, d)

				trechosDTO = append(trechosDTO, protocolo.TrechoCaronaDTO{
					Trecho:      chave,
					Origem:      o,
					Destino:     d,
					VagasLivres: c.VagasPorTrecho[chave],
					Passageiros: c.PassageirosPorTrecho[chave],
				})
			}

			caronasDTO = append(caronasDTO, protocolo.CaronaDTO{
				CaronaID:       c.ID,
				Motorista:      c.MotoristaLogin,
				Rota:           c.Rota,
				Data:           c.Data,
				Horario:        c.Horario,
				HorarioChegada: c.HorarioChegada,
				Assentos:       c.AssentosTotais,
				PrecoPorTrecho: c.PrecoPorTrecho,
				Trechos:        trechosDTO,
			})
		}
	}

	return caronasDTO, nil
}

// CancelarCarona desativa a carona e cancela as reservas vinculadas a ela
func (g *GerenciadorEstado) CancelarCarona(token, caronaID string) error {
	u, err := g.ValidarToken(token)
	if err != nil {
		return err
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	c, existe := g.caronas[caronaID]
	if !existe || !c.Ativa {
		return fmt.Errorf("carona %s nao encontrada ou ja cancelada", caronaID)
	}

	if c.MotoristaLogin != u.Login {
		return errors.New("voce nao tem permissao para cancelar esta carona")
	}

	c.Ativa = false

	for _, r := range g.reservas {
		if r.Status == "ATIVA" {
			afetada := false
			for _, item := range r.Trechos {
				if item.CaronaID == caronaID {
					afetada = true
					break
				}
			}
			if afetada {
				r.Status = "CANCELADA_PELO_MOTORISTA"
			}
		}
	}

	return nil
}

// ListarReservasPassageiro retorna o historico de reservas do passageiro
func (g *GerenciadorEstado) ListarReservasPassageiro(token string) ([]protocolo.ReservaDTO, error) {
	u, err := g.ValidarToken(token)
	if err != nil {
		return nil, err
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	var reservasDTO []protocolo.ReservaDTO

	for _, r := range g.reservas {
		if r.PassageiroLogin == u.Login {
			var trechosDTO []protocolo.ItemTrecho
			for _, item := range r.Trechos {
				trechosDTO = append(trechosDTO, protocolo.ItemTrecho{
					CaronaID: item.CaronaID,
					Origem:   item.Origem,
					Destino:  item.Destino,
					Preco:    item.Preco,
				})
			}

			reservasDTO = append(reservasDTO, protocolo.ReservaDTO{
				ReservaID:  r.ID,
				Passageiro: r.PassageiroLogin,
				Trechos:    trechosDTO,
				PrecoTotal: r.PrecoTotal,
				Data:       r.Data,
				Status:     r.Status,
			})
		}
	}

	return reservasDTO, nil
}

// CancelarReserva devolve as vagas aos sub-trechos e altera o status para CANCELADA
func (g *GerenciadorEstado) CancelarReserva(token, reservaID string) error {
	u, err := g.ValidarToken(token)
	if err != nil {
		return err
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	r, existe := g.reservas[reservaID]
	if !existe || r.Status != "ATIVA" {
		return fmt.Errorf("reserva %s nao encontrada ou ja cancelada", reservaID)
	}

	if r.PassageiroLogin != u.Login {
		return errors.New("voce nao tem permissao para cancelar esta reserva")
	}

	r.Status = "CANCELADA"

	for _, item := range r.Trechos {
		if c, ok := g.caronas[item.CaronaID]; ok {
			chave := fmt.Sprintf("%s->%s", item.Origem, item.Destino)
			c.VagasPorTrecho[chave]++

			novosPassageiros := make([]string, 0)
			for _, pass := range c.PassageirosPorTrecho[chave] {
				if pass != u.Login {
					novosPassageiros = append(novosPassageiros, pass)
				}
			}
			c.PassageirosPorTrecho[chave] = novosPassageiros
		}
	}

	return nil
}