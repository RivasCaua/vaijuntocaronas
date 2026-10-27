package consorcio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"vaijunto/pkg/estado"
)

// ServidorREST disponibiliza a API HTTP do nó para comunicação inter-servidores no consórcio.
type ServidorREST struct {
	mu          sync.RWMutex
	nodoLocal   NodoDTO
	membros     map[string]NodoDTO
	gerenciador *estado.GerenciadorEstado
	relogio     *RelogioLamport
	wal         *WAL
	coordenador *CoordenadorDistribuido
	httpServer  *http.Server
	listener    net.Listener
	porta       int
	ativo       bool
}

// ConfigServidorREST parâmetros para instanciação do servidor HTTP.
type ConfigServidorREST struct {
	NodoLocal   NodoDTO
	Membros     []NodoDTO
	Gerenciador *estado.GerenciadorEstado
	Relogio     *RelogioLamport
	WAL         *WAL
	// Coordenador, quando informado, torna-se a FONTE ÚNICA DE VERDADE sobre o status dos membros
	// do consórcio: /api/v1/cluster/info passa a refletir o mapa de membros do coordenador (o mesmo
	// que a rotina automática de heartbeat atualiza) em vez de um mapa local duplicado, e heartbeats
	// recebidos de parceiros também atualizam esse mesmo mapa compartilhado. Opcional - testes que
	// exercitam o ServidorREST isoladamente podem omitir este campo e usar o mapa local de sempre.
	Coordenador *CoordenadorDistribuido
}

// NovoServidorREST instancia a estrutura do servidor com suas dependências de domínio.
func NovoServidorREST(cfg ConfigServidorREST) *ServidorREST {
	mapaMembros := make(map[string]NodoDTO)
	for _, m := range cfg.Membros {
		mapaMembros[m.ID] = m
	}

	s := &ServidorREST{
		nodoLocal:   cfg.NodoLocal,
		membros:     mapaMembros,
		gerenciador: cfg.Gerenciador,
		relogio:     cfg.Relogio,
		wal:         cfg.WAL,
		coordenador: cfg.Coordenador,
	}

	return s
}

// Iniciar abre a porta de escuta HTTP e despacha o atendimento de requisições REST.
// Se porta == 0, o sistema operacional aloca dinamicamente uma porta livre.
func (s *ServidorREST) Iniciar(porta int) error {
	s.mu.Lock()
	if s.ativo {
		s.mu.Unlock()
		return errors.New("servidor REST ja esta em execucao")
	}

	endereco := fmt.Sprintf(":%d", porta)
	listener, err := net.Listen("tcp", endereco)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("falha ao abrir listener REST em %s: %w", endereco, err)
	}

	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		listener.Close()
		s.mu.Unlock()
		return errors.New("falha ao resolver endereco TCP do listener REST")
	}

	s.porta = tcpAddr.Port
	s.nodoLocal.PortaREST = s.porta
	s.listener = listener
	s.ativo = true

	mux := http.NewServeMux()
	s.registrarRotas(mux)

	s.httpServer = &http.Server{
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}
	s.mu.Unlock()

	go func() {
		_ = s.httpServer.Serve(listener)
	}()

	return nil
}

// IniciarEmListener permite injetar um listener TCP pré-alocado (ideal para testes unitários).
func (s *ServidorREST) IniciarEmListener(listener net.Listener) error {
	s.mu.Lock()
	if s.ativo {
		s.mu.Unlock()
		return errors.New("servidor REST ja esta em execucao")
	}

	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		s.mu.Unlock()
		return errors.New("falha ao resolver endereco TCP do listener")
	}

	s.porta = tcpAddr.Port
	s.nodoLocal.PortaREST = s.porta
	s.listener = listener
	s.ativo = true

	mux := http.NewServeMux()
	s.registrarRotas(mux)

	s.httpServer = &http.Server{
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}
	s.mu.Unlock()

	go func() {
		_ = s.httpServer.Serve(listener)
	}()

	return nil
}

// Parar desliga com segurança o servidor HTTP drenando conexões pendentes.
func (s *ServidorREST) Parar(ctx context.Context) error {
	s.mu.Lock()
	if !s.ativo {
		s.mu.Unlock()
		return nil
	}
	s.ativo = false
	srv := s.httpServer
	s.mu.Unlock()

	if srv != nil {
		return srv.Shutdown(ctx)
	}
	return nil
}

// ObterPorta retorna a porta local onde o servidor está escutando.
func (s *ServidorREST) ObterPorta() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.porta
}

// ObterEnderecoBase retorna a URL base http://127.0.0.1:<porta> para chamadas locais.
func (s *ServidorREST) ObterEnderecoBase() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("http://127.0.0.1:%d", s.porta)
}

// registrarRotas vincula os padrões de caminho aos seus respectivos handlers.
func (s *ServidorREST) registrarRotas(mux *http.ServeMux) {
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/v1/cluster/info", s.handleClusterInfo)
	mux.HandleFunc("/api/v1/cluster/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("/api/v1/itinerarios/buscar", s.handleBuscarItinerarios)
	mux.HandleFunc("/api/v1/caronas", s.handleListarCaronas)
	mux.HandleFunc("/api/v1/2pc/prepare", s.handlePrepare2PC)
	mux.HandleFunc("/api/v1/2pc/commit", s.handleCommit2PC)
	mux.HandleFunc("/api/v1/2pc/abort", s.handleAbort2PC)
	mux.HandleFunc("/api/v1/wal/sincronizar", s.handleSincronizarWAL)
}

// handleHealth verifica liveness básico.
func (s *ServidorREST) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"UP","nodo_id":"%s"}`, s.nodoLocal.ID)))
}

// handleClusterInfo retorna a visão local do cluster e topologia de nós.
func (s *ServidorREST) handleClusterInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.responderErro(w, http.StatusMethodNotAllowed, "metodo HTTP nao permitido")
		return
	}

	var membros []NodoDTO
	if s.coordenador != nil {
		// O coordenador é a fonte única de verdade quando presente - é o mapa que a rotina
		// automática de heartbeat (envio e recebimento) mantém atualizado.
		membros = s.coordenador.ObterMembros()
	} else {
		s.mu.RLock()
		membros = make([]NodoDTO, 0, len(s.membros))
		for _, m := range s.membros {
			membros = append(membros, m)
		}
		s.mu.RUnlock()
	}

	info := InfoClusterDTO{
		NodoLocal:    s.nodoLocal,
		Membros:      membros,
		LamportLocal: s.relogio.ObterTimestamp(),
		TotalMembros: len(membros) + 1,
	}

	s.responderJSON(w, http.StatusOK, RespostaREST{
		Sucesso:   true,
		Dados:     info,
		Timestamp: &info.LamportLocal,
	})
}

// handleHeartbeat processa o batimento cardíaco vindo de um nó par.
func (s *ServidorREST) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.responderErro(w, http.StatusMethodNotAllowed, "metodo HTTP nao permitido")
		return
	}

	var hb HeartbeatDTO
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&hb); err != nil {
		s.responderErro(w, http.StatusBadRequest, fmt.Sprintf("payload de heartbeat invalido: %v", err))
		return
	}

	// Atualiza relógio lógico com o timestamp recebido
	tsAtualizado := s.relogio.Atualizar(hb.TimestampLamport.Tempo)

	// Atualiza o registro do membro: receber um heartbeat é, por si só, prova de liveness do
	// remetente, então isso conta como sinal positivo mesmo que o envio automático NO SENTIDO
	// CONTRÁRIO (deste nó para o remetente) esteja falhando por algum motivo de rede assimétrico.
	if s.coordenador != nil {
		s.coordenador.RegistrarHeartbeatRecebido(hb.RemetenteID, hb.Regiao)
	} else {
		s.mu.Lock()
		if membro, existe := s.membros[hb.RemetenteID]; existe {
			membro.Status = StatusNodoOnline
			membro.UltimoVisto = time.Now()
			s.membros[hb.RemetenteID] = membro
		} else {
			s.membros[hb.RemetenteID] = NodoDTO{
				ID:          hb.RemetenteID,
				Regiao:      hb.Regiao,
				Status:      StatusNodoOnline,
				UltimoVisto: time.Now(),
			}
		}
		s.mu.Unlock()
	}

	s.responderJSON(w, http.StatusOK, RespostaREST{
		Sucesso:   true,
		Mensagem:  "heartbeat processado",
		Timestamp: &tsAtualizado,
	})
}

// handleBuscarItinerarios executa busca de viagens no estado local para responder ao Scatter-Gather.
func (s *ServidorREST) handleBuscarItinerarios(w http.ResponseWriter, r *http.Request) {
	var origem, destino, data string
	var lamportTempo uint64

	if r.Method == http.MethodGet {
		origem = r.URL.Query().Get("origem")
		destino = r.URL.Query().Get("destino")
		data = r.URL.Query().Get("data")
		if tempoStr := r.URL.Query().Get("lamport_tempo"); tempoStr != "" {
			if v, err := strconv.ParseUint(tempoStr, 10, 64); err == nil {
				lamportTempo = v
			}
		}
	} else if r.Method == http.MethodPost {
		var req BuscaItinerariosReqDTO
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
			s.responderErro(w, http.StatusBadRequest, fmt.Sprintf("JSON de busca invalido: %v", err))
			return
		}
		origem = req.Origem
		destino = req.Destino
		data = req.Data
		lamportTempo = req.TimestampLamport.Tempo
	} else {
		s.responderErro(w, http.StatusMethodNotAllowed, "metodo HTTP nao permitido")
		return
	}

	if lamportTempo > 0 {
		s.relogio.Atualizar(lamportTempo)
	}

	itinerarios, err := s.gerenciador.BuscarItinerarios(origem, destino, data)
	if err != nil {
		s.responderErro(w, http.StatusBadRequest, err.Error())
		return
	}

	tsResp := s.relogio.Tick()
	resp := BuscaItinerariosRespDTO{
		NodoID:           s.nodoLocal.ID,
		Regiao:           s.nodoLocal.Regiao,
		Itinerarios:      itinerarios,
		TimestampLamport: tsResp,
	}

	s.responderJSON(w, http.StatusOK, resp)
}

// handleListarCaronas retorna o inventário bruto de caronas ativas deste nó (de qualquer empresa
// vinculada a ele), opcionalmente filtrado por data de viagem via "?data=AAAA-MM-DD". É este
// inventário bruto - sub-trechos individuais, não itinerários já combinados - que a busca federada
// do consórcio (CoordenadorDistribuido.BuscarItinerariosCombinados) usa como arestas do grafo para
// montar itinerários que atravessam caronas de empresas diferentes.
func (s *ServidorREST) handleListarCaronas(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.responderErro(w, http.StatusMethodNotAllowed, "metodo HTTP nao permitido")
		return
	}

	data := r.URL.Query().Get("data")

	caronas, err := s.gerenciador.ListarTodasCaronasAtivas(data)
	if err != nil {
		s.responderErro(w, http.StatusInternalServerError, err.Error())
		return
	}

	ts := s.relogio.Tick()
	s.responderJSON(w, http.StatusOK, RespostaREST{
		Sucesso:   true,
		Dados:     caronas,
		Timestamp: &ts,
	})
}

// handlePrepare2PC processa a Fase 1 (Prepare) do 2PC.
// Reserva provisoriamente assentos com TTL (Lease) e desempate por Lamport.
func (s *ServidorREST) handlePrepare2PC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.responderErro(w, http.StatusMethodNotAllowed, "metodo HTTP nao permitido")
		return
	}

	var req PrepareReqDTO
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		s.responderErro(w, http.StatusBadRequest, fmt.Sprintf("corpo de requisicao prepare invalido: %v", err))
		return
	}

	// Atualiza relógio lógico com o timestamp da requisição
	tsReq := req.TimestampLamport
	if tsReq.Tempo > 0 {
		s.relogio.Atualizar(tsReq.Tempo)
	}

	ttl := time.Duration(req.TTLMillis) * time.Millisecond
	if ttl <= 0 {
		ttl = 30 * time.Second
	}

	// Executa a Fase 1: CriarHoldInterno
	hold, err := s.gerenciador.CriarHoldInterno(req.PassageiroLogin, req.Trechos, ttl, req.TimestampLamport)
	tsResp := s.relogio.Tick()

	if err != nil {
		// Voto NÃO: Recusa com motivo
		resp := PrepareRespDTO{
			TransacaoID:      req.TransacaoID,
			ParticipanteID:   s.nodoLocal.ID,
			Voto:             false,
			Motivo:           err.Error(),
			TimestampLamport: tsResp,
		}
		s.responderJSON(w, http.StatusOK, resp)
		return
	}

	// Persiste o prepare no WAL se configurado
	if s.wal != nil {
		payloadBytes, _ := json.Marshal(hold)
		_, _ = s.wal.Gravar("2PC_PREPARE", payloadBytes, tsResp)
	}

	// Voto SIM: Assentos provisoriamente retidos
	resp := PrepareRespDTO{
		TransacaoID:      req.TransacaoID,
		ParticipanteID:   s.nodoLocal.ID,
		Voto:             true,
		HoldID:           hold.ID,
		TimestampLamport: tsResp,
	}
	s.responderJSON(w, http.StatusOK, resp)
}

// handleCommit2PC processa a Fase 2 (Commit) do 2PC.
// Efetiva a reserva de assentos definitivamente.
func (s *ServidorREST) handleCommit2PC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.responderErro(w, http.StatusMethodNotAllowed, "metodo HTTP nao permitido")
		return
	}

	var req CommitReqDTO
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		s.responderErro(w, http.StatusBadRequest, fmt.Sprintf("corpo de requisicao commit invalido: %v", err))
		return
	}

	if req.TimestampLamport.Tempo > 0 {
		s.relogio.Atualizar(req.TimestampLamport.Tempo)
	}

	reserva, err := s.gerenciador.ConfirmarHoldInterno(req.PassageiroLogin, req.HoldID)
	tsResp := s.relogio.Tick()

	if err != nil {
		resp := CommitRespDTO{
			TransacaoID:      req.TransacaoID,
			ParticipanteID:   s.nodoLocal.ID,
			Sucesso:          false,
			Mensagem:         err.Error(),
			TimestampLamport: tsResp,
		}
		s.responderJSON(w, http.StatusOK, resp)
		return
	}

	// Persiste o commit definitivo no WAL. A reserva já está confirmada em memória (ConfirmarHoldInterno
	// acima já rodou) - uma vez decidido o Commit em 2PC, ele não pode mais ser desfeito unilateralmente
	// por este participante só porque a escrita durável falhou (isso corromperia o protocolo para os
	// demais participantes que já podem ter commitado também). Por isso, uma falha aqui NÃO reverte a
	// confirmação nem o sucesso reportado ao coordenador - mas precisa ficar bem visível, porque sem o
	// registro no WAL essa reserva confirmada desaparece silenciosamente numa recuperação futura.
	if s.wal != nil {
		payloadBytes, _ := json.Marshal(reserva)
		if _, err := s.wal.Gravar("2PC_COMMIT", payloadBytes, tsResp); err != nil {
			log.Printf("[CRITICO][DURABILIDADE] reserva %s confirmada em memoria mas NAO persistida no WAL (desaparecera em uma recuperacao futura): %v", reserva.ID, err)
		}
	}

	resp := CommitRespDTO{
		TransacaoID:      req.TransacaoID,
		ParticipanteID:   s.nodoLocal.ID,
		Sucesso:          true,
		ReservaID:        reserva.ID,
		Data:             reserva.Data,
		Mensagem:         "Reserva confirmada definitivamente com sucesso",
		TimestampLamport: tsResp,
	}
	s.responderJSON(w, http.StatusOK, resp)
}

// handleAbort2PC processa a Fase 2 (Abort / Rollback / Compensação) do 2PC.
// Libera assentos provisoriamente retidos por falha ou desistência na orquestração.
func (s *ServidorREST) handleAbort2PC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.responderErro(w, http.StatusMethodNotAllowed, "metodo HTTP nao permitido")
		return
	}

	var req AbortReqDTO
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		s.responderErro(w, http.StatusBadRequest, fmt.Sprintf("corpo de requisicao abort invalido: %v", err))
		return
	}

	if req.TimestampLamport.Tempo > 0 {
		s.relogio.Atualizar(req.TimestampLamport.Tempo)
	}

	err := s.gerenciador.AbortarHoldInterno(req.PassageiroLogin, req.HoldID)
	tsResp := s.relogio.Tick()

	if s.wal != nil {
		dadosAbort, _ := json.Marshal(req)
		_, _ = s.wal.Gravar("2PC_ABORT", dadosAbort, tsResp)
	}

	if err != nil {
		resp := AbortRespDTO{
			TransacaoID:      req.TransacaoID,
			ParticipanteID:   s.nodoLocal.ID,
			Sucesso:          false,
			Mensagem:         err.Error(),
			TimestampLamport: tsResp,
		}
		s.responderJSON(w, http.StatusOK, resp)
		return
	}

	resp := AbortRespDTO{
		TransacaoID:      req.TransacaoID,
		ParticipanteID:   s.nodoLocal.ID,
		Sucesso:          true,
		Mensagem:         "Hold abortado e assentos liberados com sucesso",
		TimestampLamport: tsResp,
	}
	s.responderJSON(w, http.StatusOK, resp)
}

// handleSincronizarWAL fornece fatias do log Write-Ahead Log para nós defasados.
func (s *ServidorREST) handleSincronizarWAL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.responderErro(w, http.StatusMethodNotAllowed, "metodo HTTP nao permitido")
		return
	}

	if s.wal == nil {
		s.responderErro(w, http.StatusNotImplemented, "WAL nao configurado neste servidor")
		return
	}

	var req SincronizacaoWALReqDTO
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		s.responderErro(w, http.StatusBadRequest, fmt.Sprintf("corpo de requisicao sincronizar WAL invalido: %v", err))
		return
	}

	if req.TimestampLamport.Tempo > 0 {
		s.relogio.Atualizar(req.TimestampLamport.Tempo)
	}

	registros, err := s.wal.RecuperarAposIndice(req.UltimoIndiceConhecido)
	if err != nil {
		s.responderErro(w, http.StatusInternalServerError, fmt.Sprintf("falha ao recuperar WAL: %v", err))
		return
	}

	tsResp := s.relogio.Tick()
	var indiceInicial, indiceFinal uint64
	if len(registros) > 0 {
		indiceInicial = registros[0].Indice
		indiceFinal = registros[len(registros)-1].Indice
	}

	resp := SincronizacaoWALRespDTO{
		RemetenteID:      s.nodoLocal.ID,
		IndiceInicial:    indiceInicial,
		IndiceFinal:      indiceFinal,
		Registros:        registros,
		TimestampLamport: tsResp,
	}
	s.responderJSON(w, http.StatusOK, resp)
}

// Métodos auxiliares de resposta HTTP

func (s *ServidorREST) responderJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *ServidorREST) responderErro(w http.ResponseWriter, statusCode int, mensagem string) {
	s.responderJSON(w, statusCode, RespostaREST{
		Sucesso:  false,
		Mensagem: mensagem,
	})
}
