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
	mu                  sync.RWMutex        // Trava para evitar condicao de corrida entre multiplos clientes
	usuarios            map[string]*Usuario // Mapa de login -> Usuario
	sessoes             map[string]*Sessao  // Mapa de token -> Sessao
	caronas             map[string]*Carona  // Mapa de ID -> Carona
	reservas            map[string]*Reserva // Mapa de ID -> Reserva
	holds               map[string]*Hold    // Mapa de ID -> Hold (Leases temporários para 2PC)
	holdParaReserva     map[string]string   // Mapa de HoldID -> ReservaID, para tornar ConfirmarHoldInterno idempotente sob retentativa
	seqCaronaId         int
	seqReservaId        int
	seqHoldId           int
	caminhoPersistencia string
	stopCleaner         chan struct{}
	cleanerAtivo        bool
}

// NovoGerenciadorEstado inicializa a estrutura do gerenciador
func NovoGerenciadorEstado() *GerenciadorEstado {
	return &GerenciadorEstado{
		usuarios:        make(map[string]*Usuario),
		sessoes:         make(map[string]*Sessao),
		caronas:         make(map[string]*Carona),
		reservas:        make(map[string]*Reserva),
		holds:           make(map[string]*Hold),
		holdParaReserva: make(map[string]string),
		stopCleaner:     make(chan struct{}),
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

// CadastrarUsuario adiciona um novo motorista ou passageiro de forma segura.
// Atalho sem persistência externa, usado pelos testes e por quem não precisa da garantia de
// atomicidade entre memória e disco - ver CadastrarUsuarioComPersistencia para o caminho real
// usado pelo servidor.
func (g *GerenciadorEstado) CadastrarUsuario(login, senha, nome, papel string) error {
	return g.CadastrarUsuarioComPersistencia(login, senha, nome, papel, func() error { return nil })
}

// CadastrarUsuarioComPersistencia adiciona um novo usuário e, AINDA SOB A MESMA TRAVA que protege o
// mapa de usuários, chama "persistir" (tipicamente a gravação no WAL) antes de considerar a operação
// concluída. Se "persistir" falhar, o cadastro é desfeito atomicamente antes da trava ser liberada -
// nenhuma outra goroutine jamais enxerga um usuário "cadastrado" que não está (ou pode não estar)
// gravado de forma durável. Isso fecha, pela raiz, a janela de inconsistência memória-vs-disco que
// existia quando a persistência acontecia DEPOIS da trava ser liberada (ver seção 12 do
// docs/api_consorcio.md para o caso irmão, já corrigido, de CancelarReserva).
func (g *GerenciadorEstado) CadastrarUsuarioComPersistencia(login, senha, nome, papel string, persistir func() error) error {
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

	if err := persistir(); err != nil {
		delete(g.usuarios, login)
		return fmt.Errorf("falha ao persistir cadastro de forma duravel: %w", err)
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

// PublicarCarona desmembra a rota em trechos e inicializa as vagas por trecho.
// empresaID é opcional; se não informado, assume "empresa-local". Atalho sem persistência externa -
// ver PublicarCaronaComPersistencia para o caminho real usado pelo servidor.
func (g *GerenciadorEstado) PublicarCarona(token string, rota []string, data, horario, horarioChegada string, assentos int, preco float64, empresaID ...string) (string, error) {
	return g.PublicarCaronaComPersistencia(token, rota, data, horario, horarioChegada, assentos, preco, func(string) error { return nil }, empresaID...)
}

// PublicarCaronaComPersistencia publica a carona e, ainda sob a trava, chama "persistir" (recebendo
// o ID já gerado para a carona, necessário para montar o registro do WAL) antes de confirmar - se
// falhar, a carona recém-criada é removida atomicamente (ver CadastrarUsuarioComPersistencia para a
// justificativa completa do padrão).
func (g *GerenciadorEstado) PublicarCaronaComPersistencia(token string, rota []string, data, horario, horarioChegada string, assentos int, preco float64, persistir func(caronaID string) error, empresaID ...string) (string, error) {
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

	data = strings.TrimSpace(data)
	horario = strings.TrimSpace(horario)
	horarioChegada = strings.TrimSpace(horarioChegada)

	idEmpresa := "empresa-local"
	if len(empresaID) > 0 && strings.TrimSpace(empresaID[0]) != "" {
		idEmpresa = strings.TrimSpace(empresaID[0])
	}

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
		EmpresaID:            idEmpresa,
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

	if err := persistir(caronaID); err != nil {
		delete(g.caronas, caronaID)
		return "", fmt.Errorf("falha ao persistir carona de forma duravel: %w", err)
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
					EmpresaID:      c.EmpresaID,
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
			CaronaID:  item.CaronaID,
			EmpresaID: carona.EmpresaID,
			Origem:    item.Origem,
			Destino:   item.Destino,
			Preco:     carona.PrecoPorTrecho,
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

// ObterReserva retorna uma copia de consulta de uma Reserva por ID, para uso interno (ex: registro no WAL).
func (g *GerenciadorEstado) ObterReserva(reservaID string) (*Reserva, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	r, existe := g.reservas[reservaID]
	if !existe {
		return nil, fmt.Errorf("reserva %s nao encontrada", reservaID)
	}

	copia := *r
	copia.Trechos = append([]ItemTrechoReserva(nil), r.Trechos...)
	return &copia, nil
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
				EmpresaID:      c.EmpresaID,
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
// CancelarCarona atalho sem persistência externa - ver CancelarCaronaComPersistencia.
func (g *GerenciadorEstado) CancelarCarona(token, caronaID string) error {
	return g.CancelarCaronaComPersistencia(token, caronaID, func() error { return nil })
}

// CancelarCaronaComPersistencia desativa a carona e, ainda sob a trava, chama "persistir" antes de
// confirmar - se falhar, a carona e TODAS as reservas que esta chamada marcou como
// "CANCELADA_PELO_MOTORISTA" (e só essas, nunca reservas já canceladas por outro motivo antes desta
// chamada) voltam exatamente ao estado anterior.
func (g *GerenciadorEstado) CancelarCaronaComPersistencia(token, caronaID string, persistir func() error) error {
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

	var reservasAfetadas []*Reserva
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
				reservasAfetadas = append(reservasAfetadas, r)
			}
		}
	}

	if err := persistir(); err != nil {
		c.Ativa = true
		for _, r := range reservasAfetadas {
			r.Status = "ATIVA"
		}
		return fmt.Errorf("falha ao persistir cancelamento de carona de forma duravel: %w", err)
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
					CaronaID:  item.CaronaID,
					EmpresaID: item.EmpresaID,
					Origem:    item.Origem,
					Destino:   item.Destino,
					Preco:     item.Preco,
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

// CancelarReserva devolve as vagas aos sub-trechos e altera o status para CANCELADA. Atalho sem
// persistência externa - ver CancelarReservaComPersistencia para o caminho real usado pelo servidor.
func (g *GerenciadorEstado) CancelarReserva(token, reservaID string) error {
	return g.CancelarReservaComPersistencia(token, reservaID, func() error { return nil })
}

// CancelarReservaComPersistencia cancela a reserva e, ainda sob a MESMA trava que protege o
// inventário de vagas, chama "persistir" (tipicamente a gravação no WAL) antes de liberar a trava.
// Se "persistir" falhar, a reversão acontece atomicamente ali mesmo, sem nunca ter liberado a trava
// entre a devolução da vaga e a tentativa de persistência.
//
// Esta ordem importa de verdade: uma versão anterior deste método devolvia a vaga, liberava a trava,
// e só então o chamador tentava persistir; se a persistência falhasse, um método separado
// ("ReverterCancelamentoReserva") tentava desfazer a devolução - mas entre a liberação da trava e essa
// reversão, QUALQUER outra reserva concorrente podia legitimamente ocupar a vaga recém-liberada,
// e a reversão então re-debitava a vaga (ou re-adicionava o passageiro) sem saber que ela já tinha
// dono novo, corrompendo a contagem (podendo até ficar negativa) e colocando dois passageiros no
// mesmo assento. Mantendo a trava presa durante TODO o ciclo mutação-e-persistência, a vaga nunca
// fica visivelmente livre para um concorrente a menos que o cancelamento já esteja garantidamente
// durável - fechando essa janela pela raiz, em vez de tentar consertar depois que ela já abriu.
func (g *GerenciadorEstado) CancelarReservaComPersistencia(token, reservaID string, persistir func() error) error {
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

	type trechoAfetado struct {
		chave               string
		carona              *Carona
		passageirosOriginal []string
	}
	var afetados []trechoAfetado

	for _, item := range r.Trechos {
		if c, ok := g.caronas[item.CaronaID]; ok {
			chave := fmt.Sprintf("%s->%s", item.Origem, item.Destino)
			afetados = append(afetados, trechoAfetado{chave: chave, carona: c, passageirosOriginal: c.PassageirosPorTrecho[chave]})

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

	if err := persistir(); err != nil {
		r.Status = "ATIVA"
		for _, a := range afetados {
			a.carona.VagasPorTrecho[a.chave]--
			a.carona.PassageirosPorTrecho[a.chave] = a.passageirosOriginal
		}
		return fmt.Errorf("falha ao persistir cancelamento de forma duravel: %w", err)
	}

	return nil
}

// ListarCaronasPorEmpresa retorna as caronas ativas vinculadas a uma empresa parceira
func (g *GerenciadorEstado) ListarCaronasPorEmpresa(empresaID string) ([]protocolo.CaronaDTO, error) {
	empresaID = strings.TrimSpace(empresaID)
	if empresaID == "" {
		return nil, errors.New("identificador de empresa parceira nao pode ser vazio")
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	var caronasDTO []protocolo.CaronaDTO

	for _, c := range g.caronas {
		if c.EmpresaID == empresaID && c.Ativa {
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
				EmpresaID:      c.EmpresaID,
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

// ListarTodasCaronasAtivas retorna todas as caronas ativas deste nó, independente da empresa,
// opcionalmente filtradas por data de viagem (data vazia retorna todas as datas). Usado tanto pelo
// endpoint REST "/api/v1/caronas" quanto pela busca federada do consórcio, que precisa do inventário
// bruto de sub-trechos de um nó para montar itinerários combinando caronas de empresas diferentes.
func (g *GerenciadorEstado) ListarTodasCaronasAtivas(data string) ([]protocolo.CaronaDTO, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var caronasDTO []protocolo.CaronaDTO

	for _, c := range g.caronas {
		if !c.Ativa {
			continue
		}
		if data != "" && c.Data != data {
			continue
		}

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
			EmpresaID:      c.EmpresaID,
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

	return caronasDTO, nil
}

// ObterEmpresaCarona retorna a empresa parceira dona da carona especificada
func (g *GerenciadorEstado) ObterEmpresaCarona(caronaID string) (string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	c, existe := g.caronas[caronaID]
	if !existe {
		return "", fmt.Errorf("carona %s nao encontrada", caronaID)
	}

	return c.EmpresaID, nil
}

// ObterTotalHolds retorna a contagem total de holds registrados
func (g *GerenciadorEstado) ObterTotalHolds() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.holds)
}

// ObterTotalReservas retorna a contagem total de reservas registradas
func (g *GerenciadorEstado) ObterTotalReservas() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.reservas)
}

// ObterTotalCaronas retorna a contagem total de caronas registradas
func (g *GerenciadorEstado) ObterTotalCaronas() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.caronas)
}
