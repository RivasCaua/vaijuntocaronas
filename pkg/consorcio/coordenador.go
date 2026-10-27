package consorcio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vaijunto/pkg/estado"
	"vaijunto/pkg/protocolo"
)

// CoordenadorDistribuido orquestra a federação de nós, busca Scatter-Gather e o protocolo Two-Phase Commit (2PC).
type CoordenadorDistribuido struct {
	mu                 sync.RWMutex
	nodoLocal          NodoDTO
	membros            map[string]NodoDTO
	falhasConsecutivas map[string]int
	gerenciador        *estado.GerenciadorEstado
	relogio            *RelogioLamport
	wal                *WAL
	clienteREST        *ClienteREST
	servidorREST       *ServidorREST
	seqTransacao       uint64
	stopHeartbeat      chan struct{}
	heartbeatAtivo     bool
}

// ConfigCoordenador parâmetros para inicialização do coordenador distribuído.
type ConfigCoordenador struct {
	NodoLocal    NodoDTO
	Membros      []NodoDTO
	Gerenciador  *estado.GerenciadorEstado
	Relogio      *RelogioLamport
	WAL          *WAL
	ClienteREST  *ClienteREST
	ServidorREST *ServidorREST
}

// NovoCoordenadorDistribuido instancia o coordenador associando suas dependências e parceiros de consórcio.
func NovoCoordenadorDistribuido(cfg ConfigCoordenador) *CoordenadorDistribuido {
	mapaMembros := make(map[string]NodoDTO)
	for _, m := range cfg.Membros {
		mapaMembros[m.ID] = m
	}

	cliente := cfg.ClienteREST
	if cliente == nil {
		cliente = NovoClienteREST(ConfigClienteREST{
			NodoLocalID:   cfg.NodoLocal.ID,
			TimeoutPadrao: 3 * time.Second,
		})
	}

	return &CoordenadorDistribuido{
		nodoLocal:          cfg.NodoLocal,
		membros:            mapaMembros,
		falhasConsecutivas: make(map[string]int),
		gerenciador:        cfg.Gerenciador,
		relogio:            cfg.Relogio,
		wal:                cfg.WAL,
		clienteREST:        cliente,
		servidorREST:       cfg.ServidorREST,
	}
}

// AdicionarMembro registra ou atualiza um nó parceiro no consórcio.
func (c *CoordenadorDistribuido) AdicionarMembro(membro NodoDTO) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.membros[membro.ID] = membro
}

// RemoverMembro desvincula um nó parceiro do consórcio.
func (c *CoordenadorDistribuido) RemoverMembro(membroID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.membros, membroID)
}

// ObterMembros retorna a lista atualizada de membros conhecidos do consórcio.
func (c *CoordenadorDistribuido) ObterMembros() []NodoDTO {
	c.mu.RLock()
	defer c.mu.RUnlock()

	lista := make([]NodoDTO, 0, len(c.membros))
	for _, m := range c.membros {
		lista = append(lista, m)
	}
	return lista
}

// ObterMembro retorna os dados de um membro específico.
func (c *CoordenadorDistribuido) ObterMembro(membroID string) (NodoDTO, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m, ok := c.membros[membroID]
	return m, ok
}

// IniciarRotinaHeartbeat inicia uma goroutine de fundo que, a cada "intervalo", envia um heartbeat
// a todos os membros conhecidos do consórcio e atualiza o status de cada um com base na capacidade
// de resposta observada: ONLINE enquanto responde, SUSPEITO após "limiteSuspeito" falhas seguidas,
// e OFFLINE após "limiteOffline" falhas seguidas. Chamar mais de uma vez é um no-op seguro.
func (c *CoordenadorDistribuido) IniciarRotinaHeartbeat(intervalo time.Duration, limiteSuspeito, limiteOffline int) {
	c.mu.Lock()
	if c.heartbeatAtivo {
		c.mu.Unlock()
		return
	}
	if intervalo <= 0 {
		intervalo = 2 * time.Second
	}
	if limiteSuspeito <= 0 {
		limiteSuspeito = 1
	}
	if limiteOffline <= limiteSuspeito {
		limiteOffline = limiteSuspeito + 2
	}
	c.heartbeatAtivo = true
	c.stopHeartbeat = make(chan struct{})
	c.mu.Unlock()

	go func() {
		ticker := time.NewTicker(intervalo)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.enviarHeartbeatsParaTodos(limiteSuspeito, limiteOffline)
			case <-c.stopHeartbeat:
				return
			}
		}
	}()
}

// PararRotinaHeartbeat encerra com segurança a goroutine de heartbeat, se estiver ativa.
func (c *CoordenadorDistribuido) PararRotinaHeartbeat() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.heartbeatAtivo {
		return
	}
	close(c.stopHeartbeat)
	c.heartbeatAtivo = false
}

// enviarHeartbeatsParaTodos dispara, em paralelo, um heartbeat para cada membro conhecido e
// atualiza seu status (ONLINE/SUSPEITO/OFFLINE) conforme a resposta observada.
func (c *CoordenadorDistribuido) enviarHeartbeatsParaTodos(limiteSuspeito, limiteOffline int) {
	ts := c.relogio.Tick()

	var ultimoIndiceWAL uint64
	if c.wal != nil {
		ultimoIndiceWAL = c.wal.UltimoIndice()
	}

	hb := HeartbeatDTO{
		RemetenteID:      c.nodoLocal.ID,
		Regiao:           c.nodoLocal.Regiao,
		TimestampLamport: ts,
		UltimoIndiceWAL:  ultimoIndiceWAL,
		EnviadoEm:        time.Now(),
	}

	c.mu.RLock()
	membros := make([]NodoDTO, 0, len(c.membros))
	for _, m := range c.membros {
		membros = append(membros, m)
	}
	c.mu.RUnlock()

	var wg sync.WaitGroup
	for _, m := range membros {
		membro := m
		wg.Add(1)
		go func() {
			defer wg.Done()

			endereco := membro.Endereco
			if endereco == "" {
				endereco = "127.0.0.1"
			}
			urlBase := fmt.Sprintf("http://%s:%d", endereco, membro.PortaREST)

			ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
			defer cancel()

			_, err := c.clienteREST.EnviarHeartbeat(ctx, urlBase, hb)
			c.registrarResultadoHeartbeat(membro.ID, err == nil, limiteSuspeito, limiteOffline)
		}()
	}
	wg.Wait()
}

// RegistrarHeartbeatRecebido marca um membro como ONLINE ao RECEBER um heartbeat dele (em vez de
// enviar um), criando o registro se este for o primeiro contato - caminho usado pelo handler REST
// handleHeartbeat para manter o mesmo mapa de membros usado pela rotina automática de heartbeat.
func (c *CoordenadorDistribuido) RegistrarHeartbeatRecebido(remetenteID, regiao string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.falhasConsecutivas[remetenteID] = 0

	membro, existe := c.membros[remetenteID]
	if !existe {
		membro = NodoDTO{ID: remetenteID, Regiao: regiao}
	}
	membro.Status = StatusNodoOnline
	membro.UltimoVisto = time.Now()
	c.membros[remetenteID] = membro
}

// registrarResultadoHeartbeat atualiza o contador de falhas consecutivas e o status do membro.
func (c *CoordenadorDistribuido) registrarResultadoHeartbeat(membroID string, sucesso bool, limiteSuspeito, limiteOffline int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	membro, existe := c.membros[membroID]
	if !existe {
		return
	}

	if sucesso {
		c.falhasConsecutivas[membroID] = 0
		membro.Status = StatusNodoOnline
		membro.UltimoVisto = time.Now()
		c.membros[membroID] = membro
		return
	}

	c.falhasConsecutivas[membroID]++
	falhas := c.falhasConsecutivas[membroID]

	switch {
	case falhas >= limiteOffline:
		membro.Status = StatusNodoOffline
	case falhas >= limiteSuspeito:
		membro.Status = StatusNodoSuspeito
	}
	c.membros[membroID] = membro
}

// ObterEnderecoBaseMembro resolve a URL base http://<host>:<porta> de um membro do consórcio.
func (c *CoordenadorDistribuido) ObterEnderecoBaseMembro(membroID string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	m, ok := c.membros[membroID]
	if !ok {
		return "", fmt.Errorf("membro %s nao encontrado no consorcio", membroID)
	}

	endereco := m.Endereco
	if endereco == "" {
		endereco = "127.0.0.1"
	}
	return fmt.Sprintf("http://%s:%d", endereco, m.PortaREST), nil
}

// infoParticipante guarda as credenciais de roteamento de um participante do 2PC.
type infoParticipante struct {
	NodoID       string
	EnderecoBase string
	EhLocal      bool
	Trechos      []protocolo.ItemTrecho
}

// DeterminarParticipante resolve se o trecho deve ser atendido localmente ou encaminhado a um nó remoto parceiro.
//
// A EmpresaID explícita do trecho (quando informada) tem SEMPRE prioridade sobre a busca pelo
// CaronaID no estado local: cada GerenciadorEstado de cada empresa gera seus próprios IDs de forma
// independente a partir de "c-1", então duas empresas distintas podem perfeitamente ter caronas
// com o MESMO CaronaID. Checar primeiro o estado local faria o coordenador tratar por engano uma
// carona de um nó parceiro como local sempre que os IDs colidissem, corrompendo a transação 2PC.
func (c *CoordenadorDistribuido) DeterminarParticipante(trecho protocolo.ItemTrecho) (nodoID string, enderecoBase string, ehLocal bool, err error) {
	// 1. EmpresaID explicita: resolve direto para o nó local ou para o membro parceiro correspondente.
	if trecho.EmpresaID != "" {
		if trecho.EmpresaID == c.nodoLocal.ID || trecho.EmpresaID == c.nodoLocal.Regiao {
			return c.nodoLocal.ID, "", true, nil
		}

		c.mu.RLock()
		membro, existe := c.membros[trecho.EmpresaID]
		if !existe {
			for _, m := range c.membros {
				if m.Regiao == trecho.EmpresaID {
					membro, existe = m, true
					break
				}
			}
		}
		c.mu.RUnlock()

		if existe {
			end := membro.Endereco
			if end == "" {
				end = "127.0.0.1"
			}
			return membro.ID, fmt.Sprintf("http://%s:%d", end, membro.PortaREST), false, nil
		}
	}

	// 2. Sem EmpresaID informada (cliente legado/ambíguo): tenta localizar a carona no estado local
	// antes de assumir que o trecho pertence a este nó.
	if c.gerenciador != nil {
		if _, errEmpresa := c.gerenciador.ObterEmpresaCarona(trecho.CaronaID); errEmpresa == nil {
			return c.nodoLocal.ID, "", true, nil
		}
	}

	// Fallback padrão: assume nó local
	return c.nodoLocal.ID, "", true, nil
}

// --- BUSCA DISPERSAR-COLETAR (SCATTER-GATHER) ---

// resultadoBuscaScatter agrega o retorno assíncrono de cada nó consultado.
type resultadoBuscaScatter struct {
	NodoID      string
	Itinerarios []protocolo.ItinerarioDTO
	Erro        error
}

// BuscarItinerariosDistribuido implementa o padrão Scatter-Gather.
// Dispersa a consulta em paralelo para todos os nós parceiros e consolida os resultados.
// Tolera falhas parciais caso um ou mais nós estejam temporariamente inacessíveis ou lentos.
func (c *CoordenadorDistribuido) BuscarItinerariosDistribuido(ctx context.Context, origem, destino, data string, timeout time.Duration) ([]protocolo.ItinerarioDTO, error) {
	if timeout <= 0 {
		timeout = 2500 * time.Millisecond
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Avança o relógio de Lamport para o evento de despacho da busca
	tsLocal := c.relogio.Tick()

	c.mu.RLock()
	membrosRemotos := make([]NodoDTO, 0, len(c.membros))
	for _, m := range c.membros {
		if m.Status != StatusNodoOffline {
			membrosRemotos = append(membrosRemotos, m)
		}
	}
	c.mu.RUnlock()

	totalConsultas := 1 + len(membrosRemotos)
	canalResultados := make(chan resultadoBuscaScatter, totalConsultas)
	var wg sync.WaitGroup

	// 1. Scatter Local: busca no estado em memória do próprio nó
	wg.Add(1)
	go func() {
		defer wg.Done()
		itinerariosLocais, err := c.gerenciador.BuscarItinerarios(origem, destino, data)
		canalResultados <- resultadoBuscaScatter{
			NodoID:      c.nodoLocal.ID,
			Itinerarios: itinerariosLocais,
			Erro:        err,
		}
	}()

	// 2. Scatter Remoto: dispara goroutines paralelas para cada nó parceiro do consórcio
	for _, m := range membrosRemotos {
		membro := m
		wg.Add(1)
		go func() {
			defer wg.Done()

			endereco := membro.Endereco
			if endereco == "" {
				endereco = "127.0.0.1"
			}
			urlBase := fmt.Sprintf("http://%s:%d", endereco, membro.PortaREST)

			reqDTO := BuscaItinerariosReqDTO{
				Origem:           origem,
				Destino:          destino,
				Data:             data,
				SolicitanteID:    c.nodoLocal.ID,
				TimestampLamport: tsLocal,
			}

			respDTO, err := c.clienteREST.BuscarItinerarios(ctxTimeout, urlBase, reqDTO)
			if err != nil {
				canalResultados <- resultadoBuscaScatter{
					NodoID: membro.ID,
					Erro:   err,
				}
				return
			}

			// Atualiza relógio de Lamport com o timestamp da resposta remota
			if respDTO.TimestampLamport.Tempo > 0 {
				c.relogio.Atualizar(respDTO.TimestampLamport.Tempo)
			}

			canalResultados <- resultadoBuscaScatter{
				NodoID:      membro.ID,
				Itinerarios: respDTO.Itinerarios,
				Erro:        nil,
			}
		}()
	}

	// Fecha o canal quando todas as goroutines finalizarem
	go func() {
		wg.Wait()
		close(canalResultados)
	}()

	// 3. Gather: Coleta e consolida os itinerários retornados
	itinerariosAgregados := make([]protocolo.ItinerarioDTO, 0)
	errosParciais := make([]string, 0)
	sucessos := 0

	for res := range canalResultados {
		if res.Erro != nil {
			errosParciais = append(errosParciais, fmt.Sprintf("[%s]: %v", res.NodoID, res.Erro))
			continue
		}
		sucessos++
		itinerariosAgregados = append(itinerariosAgregados, res.Itinerarios...)
	}

	// Se nenhum nó respondeu com sucesso e houve erros, retorna a falha
	if sucessos == 0 && len(errosParciais) > 0 {
		return nil, fmt.Errorf("busca federada falhou em todos os nós: %s", strings.Join(errosParciais, "; "))
	}

	// Ordena determinísticamente os itinerários por menor preço total e horário de partida
	sort.Slice(itinerariosAgregados, func(i, j int) bool {
		if itinerariosAgregados[i].PrecoTotal != itinerariosAgregados[j].PrecoTotal {
			return itinerariosAgregados[i].PrecoTotal < itinerariosAgregados[j].PrecoTotal
		}
		return itinerariosAgregados[i].HorarioPartida < itinerariosAgregados[j].HorarioPartida
	})

	return itinerariosAgregados, nil
}

// --- BUSCA COMBINADA ENTRE EMPRESAS (GRAFO DE SUB-TRECHOS) ---

// arestaGrafo representa uma aresta do grafo de busca combinada: um sub-trecho reservável de uma
// carona ativa, de qualquer empresa do consórcio, com vaga disponível na data solicitada.
type arestaGrafo struct {
	CaronaID       string
	EmpresaID      string
	Destino        string
	Preco          float64
	HorarioPartida string
	HorarioChegada string
}

// BuscarItinerariosCombinados monta itinerários que podem atravessar MÚLTIPLAS empresas do
// consórcio, encadeando sub-trechos de caronas distintas - da mesma empresa ou de parceiras -
// sempre que compartilharem uma cidade. É o mecanismo que resolve o cenário central do consórcio:
// um passageiro de Salvador a Montes Claros pode precisar de caronas de três empresas diferentes, e
// nenhuma delas sozinha enxerga o itinerário completo (BuscarItinerariosDistribuido, acima, apenas
// agrega lado a lado itinerários JÁ PRONTOS de cada nó - nunca costura um trecho de um nó com o
// trecho de outro).
//
// Primeiro dispersa (Scatter) uma consulta de INVENTÁRIO BRUTO - sub-trechos individuais com vaga,
// não itinerários já combinados - para o estado local e todos os parceiros em paralelo, tolerando
// falha parcial de qualquer nó indisponível. Em seguida monta um grafo onde cada cidade é um
// vértice e cada sub-trecho com vaga é uma aresta rotulada com a empresa dona da carona, e busca em
// profundidade (DFS, com limite de saltos e sem revisitar cidade) todos os caminhos simples de
// Origem a Destino.
func (c *CoordenadorDistribuido) BuscarItinerariosCombinados(ctx context.Context, origem, destino, data string, timeout time.Duration) ([]protocolo.ItinerarioDTO, error) {
	origemNorm, okOrig := estado.NormalizarCidade(origem)
	destinoNorm, okDest := estado.NormalizarCidade(destino)
	if !okOrig || !okDest {
		return nil, errors.New("origem ou destino invalidos no catalogo regional")
	}

	if timeout <= 0 {
		timeout = 2500 * time.Millisecond
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	c.mu.RLock()
	membrosRemotos := make([]NodoDTO, 0, len(c.membros))
	for _, m := range c.membros {
		if m.Status != StatusNodoOffline {
			membrosRemotos = append(membrosRemotos, m)
		}
	}
	c.mu.RUnlock()

	totalConsultas := 1 + len(membrosRemotos)
	canalLotes := make(chan []protocolo.CaronaDTO, totalConsultas)
	var wg sync.WaitGroup

	// Scatter local: inventário bruto deste próprio nó.
	wg.Add(1)
	go func() {
		defer wg.Done()
		caronas, err := c.gerenciador.ListarTodasCaronasAtivas(data)
		if err != nil {
			canalLotes <- nil
			return
		}
		canalLotes <- caronas
	}()

	// Scatter remoto: inventário bruto de cada parceiro ONLINE/SUSPEITO, em paralelo.
	for _, m := range membrosRemotos {
		membro := m
		wg.Add(1)
		go func() {
			defer wg.Done()

			endereco := membro.Endereco
			if endereco == "" {
				endereco = "127.0.0.1"
			}
			urlBase := fmt.Sprintf("http://%s:%d", endereco, membro.PortaREST)

			caronas, err := c.clienteREST.ListarCaronasDisponiveis(ctxTimeout, urlBase, data)
			if err != nil {
				canalLotes <- nil
				return
			}
			canalLotes <- caronas
		}()
	}

	go func() {
		wg.Wait()
		close(canalLotes)
	}()

	// Gather: monta o grafo de sub-trechos combináveis a partir de todos os lotes recebidos.
	grafo := make(map[string][]arestaGrafo)
	for lote := range canalLotes {
		for _, carona := range lote {
			for _, t := range carona.Trechos {
				if t.VagasLivres <= 0 {
					continue
				}
				grafo[t.Origem] = append(grafo[t.Origem], arestaGrafo{
					CaronaID:       carona.CaronaID,
					EmpresaID:      carona.EmpresaID,
					Destino:        t.Destino,
					Preco:          carona.PrecoPorTrecho,
					HorarioPartida: carona.Horario,
					HorarioChegada: carona.HorarioChegada,
				})
			}
		}
	}

	const maxSaltos = 4      // generoso para o catalogo regional (14 cidades) sem explodir combinatoriamente
	const maxResultados = 50 // teto de segurança; nunca atingido em escala regional real

	var resultados []protocolo.ItinerarioDTO
	visitados := map[string]bool{origemNorm: true}
	buscarCaminhosEmGrafo(grafo, origemNorm, destinoNorm, visitados, nil, maxSaltos, maxResultados, &resultados)

	sort.Slice(resultados, func(i, j int) bool {
		if resultados[i].PrecoTotal != resultados[j].PrecoTotal {
			return resultados[i].PrecoTotal < resultados[j].PrecoTotal
		}
		if len(resultados[i].Trechos) != len(resultados[j].Trechos) {
			return len(resultados[i].Trechos) < len(resultados[j].Trechos)
		}
		return resultados[i].HorarioPartida < resultados[j].HorarioPartida
	})

	return resultados, nil
}

// buscarCaminhosEmGrafo executa uma busca em profundidade (DFS) sobre o grafo de sub-trechos,
// enumerando todos os caminhos simples (sem repetir cidade) de "atual" até "destino", respeitando o
// limite de saltos e o número máximo de resultados.
func buscarCaminhosEmGrafo(grafo map[string][]arestaGrafo, atual, destino string, visitados map[string]bool, caminhoAtual []protocolo.ItemTrecho, maxSaltos, maxResultados int, resultados *[]protocolo.ItinerarioDTO) {
	if len(*resultados) >= maxResultados {
		return
	}

	if atual == destino && len(caminhoAtual) > 0 {
		trechos := make([]protocolo.ItemTrecho, len(caminhoAtual))
		copy(trechos, caminhoAtual)

		precoTotal := 0.0
		for _, t := range trechos {
			precoTotal += t.Preco
		}

		*resultados = append(*resultados, protocolo.ItinerarioDTO{
			Trechos:        trechos,
			PrecoTotal:     precoTotal,
			HorarioPartida: trechos[0].HorarioPartida,
			HorarioChegada: trechos[len(trechos)-1].HorarioChegada,
		})
		return
	}

	if len(caminhoAtual) >= maxSaltos {
		return
	}

	for _, aresta := range grafo[atual] {
		if visitados[aresta.Destino] {
			continue
		}

		visitados[aresta.Destino] = true
		novoCaminho := append(append([]protocolo.ItemTrecho{}, caminhoAtual...), protocolo.ItemTrecho{
			CaronaID:       aresta.CaronaID,
			EmpresaID:      aresta.EmpresaID,
			Origem:         atual,
			Destino:        aresta.Destino,
			Preco:          aresta.Preco,
			HorarioPartida: aresta.HorarioPartida,
			HorarioChegada: aresta.HorarioChegada,
		})

		buscarCaminhosEmGrafo(grafo, aresta.Destino, destino, visitados, novoCaminho, maxSaltos, maxResultados, resultados)
		delete(visitados, aresta.Destino)
	}
}

// --- ORQUESTRAÇÃO TWO-PHASE COMMIT (2PC) COM COMPENSAÇÃO DE FALHAS ---

// votoParticipante2PC registra a resposta individual de um participante na Fase 1.
type votoParticipante2PC struct {
	NodoID       string
	EnderecoBase string
	EhLocal      bool
	Voto         bool
	HoldID       string
	MotivoErro   string
}

// ExecutarReserva2PC coordena a reserva atômica distribuída de uma viagem que pode envolver múltiplos servidores.
// Se todos os participantes votarem SIM (Prepare), efetiva a reserva (Commit).
// Se qualquer participante recusar ou falhar, aborta a transação e executa compensações imediatas (Rollback).
func (c *CoordenadorDistribuido) ExecutarReserva2PC(ctx context.Context, passageiroLogin string, trechos []protocolo.ItemTrecho, ttl time.Duration) (*protocolo.ReservaDTO, error) {
	if len(trechos) == 0 {
		return nil, errors.New("nenhum trecho especificado para reserva distribuida")
	}

	if strings.TrimSpace(passageiroLogin) == "" {
		return nil, errors.New("login do passageiro obrigatorio para transacao 2PC")
	}

	if ttl <= 0 {
		ttl = 30 * time.Second
	}

	// 1. Agrupa os sub-trechos pelo participante responsável (local vs remotos)
	participantes := make(map[string]*infoParticipante)
	for _, item := range trechos {
		nodoID, endBase, ehLocal, err := c.DeterminarParticipante(item)
		if err != nil {
			return nil, fmt.Errorf("falha ao determinar no para trecho %s->%s: %w", item.Origem, item.Destino, err)
		}

		part, existe := participantes[nodoID]
		if !existe {
			part = &infoParticipante{
				NodoID:       nodoID,
				EnderecoBase: endBase,
				EhLocal:      ehLocal,
				Trechos:      make([]protocolo.ItemTrecho, 0),
			}
			participantes[nodoID] = part
		}
		part.Trechos = append(part.Trechos, item)
	}

	// 2. Inicializa a transação 2PC com ID único e relógio de Lamport
	transacaoID := fmt.Sprintf("2pc-%s-%d", c.nodoLocal.ID, atomic.AddUint64(&c.seqTransacao, 1))
	tsLamport := c.relogio.Tick()

	// Persiste início da transação no WAL
	if c.wal != nil {
		dadosInicio, _ := json.Marshal(map[string]any{
			"transacao_id": transacaoID,
			"passageiro":   passageiroLogin,
			"trechos":      trechos,
			"fase":         "INICIO",
		})
		_, _ = c.wal.Gravar("2PC_COORDENADOR_INICIO", dadosInicio, tsLamport)
	}

	// 3. FASE 1: PREPARE (Votação em Paralelo)
	canalVotos := make(chan votoParticipante2PC, len(participantes))
	var wgPrepare sync.WaitGroup

	ttlMillis := ttl.Milliseconds()

	for _, p := range participantes {
		part := p
		wgPrepare.Add(1)

		go func() {
			defer wgPrepare.Done()

			if part.EhLocal {
				// Execução local da Fase 1: CriarHoldInterno
				hold, err := c.gerenciador.CriarHoldInterno(passageiroLogin, part.Trechos, ttl, tsLamport)
				if err != nil {
					canalVotos <- votoParticipante2PC{
						NodoID:     part.NodoID,
						EhLocal:    true,
						Voto:       false,
						MotivoErro: err.Error(),
					}
					return
				}
				canalVotos <- votoParticipante2PC{
					NodoID:  part.NodoID,
					EhLocal: true,
					Voto:    true,
					HoldID:  hold.ID,
				}
				return
			}

			// Execução remota da Fase 1 via HTTP REST
			reqPrepare := PrepareReqDTO{
				TransacaoID:      transacaoID,
				CoordenadorID:    c.nodoLocal.ID,
				PassageiroLogin:  passageiroLogin,
				Trechos:          part.Trechos,
				TTLMillis:        ttlMillis,
				TimestampLamport: tsLamport,
			}

			respPrepare, err := c.clienteREST.Prepare(ctx, part.EnderecoBase, reqPrepare)
			if err != nil {
				canalVotos <- votoParticipante2PC{
					NodoID:       part.NodoID,
					EnderecoBase: part.EnderecoBase,
					EhLocal:      false,
					Voto:         false,
					MotivoErro:   fmt.Sprintf("falha de comunicacao prepare: %v", err),
				}
				return
			}

			if respPrepare.TimestampLamport.Tempo > 0 {
				c.relogio.Atualizar(respPrepare.TimestampLamport.Tempo)
			}

			canalVotos <- votoParticipante2PC{
				NodoID:       part.NodoID,
				EnderecoBase: part.EnderecoBase,
				EhLocal:      false,
				Voto:         respPrepare.Voto,
				HoldID:       respPrepare.HoldID,
				MotivoErro:   respPrepare.Motivo,
			}
		}()
	}

	wgPrepare.Wait()
	close(canalVotos)

	// 4. Avaliação do Consenso dos Votos
	votosPositivos := make([]votoParticipante2PC, 0, len(participantes))
	var falhas []string
	consensoAprovado := true

	for voto := range canalVotos {
		if voto.Voto {
			votosPositivos = append(votosPositivos, voto)
		} else {
			consensoAprovado = false
			falhas = append(falhas, fmt.Sprintf("no %s recusou: %s", voto.NodoID, voto.MotivoErro))
		}
	}

	// 5. FASE 2: COMMIT ou ABORT com COMPENSAÇÃO DE FALHAS
	if !consensoAprovado {
		// DECISÃO GLOBAL: ABORT
		tsAbort := c.relogio.Tick()
		if c.wal != nil {
			dadosAbort, _ := json.Marshal(map[string]any{
				"transacao_id": transacaoID,
				"falhas":       falhas,
				"fase":         "ABORT",
			})
			_, _ = c.wal.Gravar("2PC_COORDENADOR_ABORT", dadosAbort, tsAbort)
		}

		// AÇÃO COMPENSATÓRIA: Executa Rollback/Abort em todos os participantes que haviam votado SIM
		var wgCompensacao sync.WaitGroup
		for _, votoSim := range votosPositivos {
			voto := votoSim
			wgCompensacao.Add(1)

			go func() {
				defer wgCompensacao.Done()
				if voto.EhLocal {
					_ = c.gerenciador.AbortarHoldInterno(passageiroLogin, voto.HoldID)
					return
				}

				reqAbort := AbortReqDTO{
					TransacaoID:      transacaoID,
					CoordenadorID:    c.nodoLocal.ID,
					HoldID:           voto.HoldID,
					PassageiroLogin:  passageiroLogin,
					Motivo:           "transacao abortada por falha em outros participantes",
					TimestampLamport: tsAbort,
				}
				_, _ = c.clienteREST.Abort(context.Background(), voto.EnderecoBase, reqAbort)
			}()
		}
		wgCompensacao.Wait()

		return nil, fmt.Errorf("transacao 2PC abortada e compensada: %s", strings.Join(falhas, "; "))
	}

	// DECISÃO GLOBAL: COMMIT
	tsCommit := c.relogio.Tick()
	if c.wal != nil {
		dadosCommit, _ := json.Marshal(map[string]any{
			"transacao_id": transacaoID,
			"passageiro":   passageiroLogin,
			"fase":         "COMMIT",
		})
		_, _ = c.wal.Gravar("2PC_COORDENADOR_COMMIT", dadosCommit, tsCommit)
	}

	var wgCommit sync.WaitGroup
	var muCommit sync.Mutex
	reservasIDs := make([]string, 0, len(votosPositivos))
	errosCommit := make([]string, 0)
	var dataViagem string

	for _, voto := range votosPositivos {
		v := voto
		wgCommit.Add(1)

		go func() {
			defer wgCommit.Done()

			if v.EhLocal {
				reserva, err := c.gerenciador.ConfirmarHoldInterno(passageiroLogin, v.HoldID)

				muCommit.Lock()
				if err != nil {
					errosCommit = append(errosCommit, fmt.Sprintf("commit local falhou: %v", err))
					muCommit.Unlock()
					return
				}
				reservasIDs = append(reservasIDs, reserva.ID)
				dataViagem = reserva.Data
				muCommit.Unlock()

				// Persiste o fato definitivo no WAL local, no MESMO formato que o handler REST
				// handleCommit2PC usa para um participante remoto - garante que uma reserva
				// confirmada pelo coordenador em seu próprio nó sobreviva a uma queda do processo
				// exatamente como sobreviveria se tivesse chegado via HTTP de outro coordenador.
				// Uma falha aqui não desfaz o commit já decidido (ver justificativa equivalente em
				// handleCommit2PC), mas precisa ficar bem visível: sem o registro, a reserva confirmada
				// desaparece silenciosamente de uma recuperação futura.
				if c.wal != nil {
					payloadBytes, _ := json.Marshal(reserva)
					if _, err := c.wal.Gravar("2PC_COMMIT", payloadBytes, tsCommit); err != nil {
						log.Printf("[CRITICO][DURABILIDADE] reserva %s confirmada em memoria mas NAO persistida no WAL local (desaparecera em uma recuperacao futura): %v", reserva.ID, err)
					}
				}
				return
			}

			reqCommit := CommitReqDTO{
				TransacaoID:      transacaoID,
				CoordenadorID:    c.nodoLocal.ID,
				HoldID:           v.HoldID,
				PassageiroLogin:  passageiroLogin,
				TimestampLamport: tsCommit,
			}

			// O protocolo 2PC classico e "bloqueante" exatamente neste ponto: uma vez que o Commit foi
			// decidido (todos votaram SIM), um participante que falhe em aplicá-lo não pode ser
			// unilateralmente compensado como se tivesse recusado - outro participante pode já ter
			// commitado de verdade, e desfazer o dele criaria uma inconsistência pior. A mitigação
			// prática aqui é tentar novamente algumas vezes antes de desistir, reduzindo bastante a
			// janela de uma falha transitória de rede virar uma divergência real entre participantes;
			// resolver isso por completo exigiria um protocolo não-bloqueante (3PC, Paxos/Raft), fora
			// do escopo deste projeto - ver docs/api_consorcio.md para a limitação documentada.
			var respCommit *CommitRespDTO
			var err error
			for tentativa := 1; tentativa <= 3; tentativa++ {
				respCommit, err = c.clienteREST.Commit(ctx, v.EnderecoBase, reqCommit)
				if err == nil {
					break
				}
				if tentativa < 3 {
					time.Sleep(time.Duration(tentativa) * 150 * time.Millisecond)
				}
			}

			muCommit.Lock()
			if err != nil {
				log.Printf("[CRITICO][2PC] commit remoto em %s falhou apos 3 tentativas para a transacao %s - possivel divergencia: participante local pode ja ter commitado enquanto %s nao confirmou: %v", v.NodoID, transacaoID, v.NodoID, err)
				errosCommit = append(errosCommit, fmt.Sprintf("commit remoto em %s falhou: %v", v.NodoID, err))
			} else if !respCommit.Sucesso {
				errosCommit = append(errosCommit, fmt.Sprintf("commit recusado em %s: %s", v.NodoID, respCommit.Mensagem))
			} else {
				reservasIDs = append(reservasIDs, respCommit.ReservaID)
				dataViagem = respCommit.Data
			}
			muCommit.Unlock()
		}()
	}

	wgCommit.Wait()

	if len(errosCommit) > 0 {
		return nil, fmt.Errorf("falhas parciais na confirmacao de commit: %s", strings.Join(errosCommit, "; "))
	}

	// Calcula preço total acumulado (a data de viagem já foi capturada do commit de algum
	// participante acima - todos os trechos de uma mesma reserva compartilham a mesma data)
	precoTotal := 0.0
	for _, t := range trechos {
		precoTotal += t.Preco
	}

	// O ID de reserva devolvido ao cliente precisa ser algo que ListarReservas/CancelarReserva
	// consigam resolver depois. Quando a transação tem um único participante (o caso comum hoje,
	// já que a descoberta de itinerários ainda não combina trechos de empresas diferentes em um
	// só resultado), o ID real da reserva naquele nó é usado diretamente. Só quando há múltiplos
	// participantes (reserva manualmente combinando empresas) não existe um ID único por nó e o
	// identificador composto da própria transação 2PC é usado como fallback.
	reservaID := transacaoID
	if len(reservasIDs) == 1 {
		reservaID = reservasIDs[0]
	}

	reservaConsolidada := &protocolo.ReservaDTO{
		ReservaID:  reservaID,
		Passageiro: passageiroLogin,
		Trechos:    trechos,
		PrecoTotal: precoTotal,
		Data:       dataViagem,
		Status:     "ATIVA",
	}

	return reservaConsolidada, nil
}
