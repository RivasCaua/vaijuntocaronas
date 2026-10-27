package consorcio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"vaijunto/pkg/protocolo"
)

// ClienteREST provê chamadas HTTP síncronas e resilientes com timeouts estritos para nós pares.
type ClienteREST struct {
	httpClient    *http.Client
	nodoLocalID   string
	timeoutPadrao time.Duration
}

// ConfigClienteREST configurações de conexão e limites temporais para o cliente HTTP.
type ConfigClienteREST struct {
	NodoLocalID       string
	TimeoutPadrao     time.Duration
	MaxIdleConns      int
	IdleConnTimeout   time.Duration
	DisableKeepAlives bool
}

// NovoClienteREST instancia um novo cliente HTTP com transporte afinado para baixa latência.
func NovoClienteREST(cfg ConfigClienteREST) *ClienteREST {
	timeout := cfg.TimeoutPadrao
	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	maxIdle := cfg.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = 100
	}

	idleTimeout := cfg.IdleConnTimeout
	if idleTimeout <= 0 {
		idleTimeout = 90 * time.Second
	}

	transporte := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        maxIdle,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     idleTimeout,
		DisableKeepAlives:   cfg.DisableKeepAlives,
	}

	return &ClienteREST{
		httpClient: &http.Client{
			Transport: transporte,
			Timeout:   timeout,
		},
		nodoLocalID:   cfg.NodoLocalID,
		timeoutPadrao: timeout,
	}
}

// formatarURL assegura a formatação correta sem barras duplas.
func formatarURL(enderecoBase, path string) string {
	enderecoBase = strings.TrimRight(enderecoBase, "/")
	if !strings.HasPrefix(enderecoBase, "http://") && !strings.HasPrefix(enderecoBase, "https://") {
		enderecoBase = "http://" + enderecoBase
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return enderecoBase + path
}

// ObterInfoCluster consulta a visão topológica e status de um nó parceiro.
func (c *ClienteREST) ObterInfoCluster(ctx context.Context, enderecoBase string) (*InfoClusterDTO, error) {
	url := formatarURL(enderecoBase, "/api/v1/cluster/info")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("falha ao criar requisicao info cluster: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("erro de comunicacao com %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		corpo, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("resposta HTTP %d de %s: %s", resp.StatusCode, url, string(corpo))
	}

	var envelope RespostaREST
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("falha ao decodificar JSON de info: %w", err)
	}

	dadosBytes, err := json.Marshal(envelope.Dados)
	if err != nil {
		return nil, fmt.Errorf("falha ao processar dados de info: %w", err)
	}

	var info InfoClusterDTO
	if err := json.Unmarshal(dadosBytes, &info); err != nil {
		return nil, fmt.Errorf("falha ao deserializar InfoClusterDTO: %w", err)
	}

	return &info, nil
}

// EnviarHeartbeat envia o sinal periódico de batimento cardíaco para um nó parceiro.
func (c *ClienteREST) EnviarHeartbeat(ctx context.Context, enderecoBase string, hb HeartbeatDTO) (*RespostaREST, error) {
	url := formatarURL(enderecoBase, "/api/v1/cluster/heartbeat")
	corpoBytes, err := json.Marshal(hb)
	if err != nil {
		return nil, fmt.Errorf("falha ao serializar heartbeat: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(corpoBytes))
	if err != nil {
		return nil, fmt.Errorf("falha ao criar requisicao de heartbeat: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("falha ao enviar heartbeat para %s: %w", url, err)
	}
	defer resp.Body.Close()

	var envelope RespostaREST
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1024)).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("falha ao decodificar resposta de heartbeat: %w", err)
	}

	return &envelope, nil
}

// BuscarItinerarios envia requisição de busca para nó parceiro dentro da estratégia Scatter-Gather.
func (c *ClienteREST) BuscarItinerarios(ctx context.Context, enderecoBase string, reqDTO BuscaItinerariosReqDTO) (*BuscaItinerariosRespDTO, error) {
	url := formatarURL(enderecoBase, "/api/v1/itinerarios/buscar")
	corpoBytes, err := json.Marshal(reqDTO)
	if err != nil {
		return nil, fmt.Errorf("falha ao serializar busca de itinerarios: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(corpoBytes))
	if err != nil {
		return nil, fmt.Errorf("falha ao criar requisicao de busca: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("falha ao executar busca em %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		corpo, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("resposta HTTP %d em busca: %s", resp.StatusCode, string(corpo))
	}

	var respDTO BuscaItinerariosRespDTO
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&respDTO); err != nil {
		return nil, fmt.Errorf("falha ao decodificar resposta de itinerarios: %w", err)
	}

	return &respDTO, nil
}

// ListarCaronasDisponiveis consulta o inventário bruto de caronas ativas de um nó parceiro para uma
// data de viagem, usado pela busca federada para montar o grafo de sub-trechos combináveis entre
// empresas distintas do consórcio.
func (c *ClienteREST) ListarCaronasDisponiveis(ctx context.Context, enderecoBase, data string) ([]protocolo.CaronaDTO, error) {
	url := formatarURL(enderecoBase, "/api/v1/caronas")
	if data != "" {
		url = url + "?data=" + data
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("falha ao criar requisicao de caronas disponiveis: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("erro de comunicacao com %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		corpo, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("resposta HTTP %d de %s: %s", resp.StatusCode, url, string(corpo))
	}

	var envelope RespostaREST
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("falha ao decodificar JSON de caronas disponiveis: %w", err)
	}

	dadosBytes, err := json.Marshal(envelope.Dados)
	if err != nil {
		return nil, fmt.Errorf("falha ao processar dados de caronas disponiveis: %w", err)
	}

	var caronas []protocolo.CaronaDTO
	if err := json.Unmarshal(dadosBytes, &caronas); err != nil {
		return nil, fmt.Errorf("falha ao deserializar caronas disponiveis: %w", err)
	}

	return caronas, nil
}

// Prepare aciona a Fase 1 (Prepare / Hold provisório) do 2PC em um nó participante.
func (c *ClienteREST) Prepare(ctx context.Context, enderecoBase string, reqDTO PrepareReqDTO) (*PrepareRespDTO, error) {
	url := formatarURL(enderecoBase, "/api/v1/2pc/prepare")
	corpoBytes, err := json.Marshal(reqDTO)
	if err != nil {
		return nil, fmt.Errorf("falha ao serializar prepare 2PC: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(corpoBytes))
	if err != nil {
		return nil, fmt.Errorf("falha ao criar requisicao prepare 2PC: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("falha na comunicacao prepare com %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		corpo, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("resposta HTTP %d no prepare de %s: %s", resp.StatusCode, url, string(corpo))
	}

	var respDTO PrepareRespDTO
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&respDTO); err != nil {
		return nil, fmt.Errorf("falha ao decodificar resposta do prepare: %w", err)
	}

	return &respDTO, nil
}

// Commit aciona a Fase 2 (Commit definitivo) do 2PC em um nó participante.
func (c *ClienteREST) Commit(ctx context.Context, enderecoBase string, reqDTO CommitReqDTO) (*CommitRespDTO, error) {
	url := formatarURL(enderecoBase, "/api/v1/2pc/commit")
	corpoBytes, err := json.Marshal(reqDTO)
	if err != nil {
		return nil, fmt.Errorf("falha ao serializar commit 2PC: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(corpoBytes))
	if err != nil {
		return nil, fmt.Errorf("falha ao criar requisicao commit 2PC: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("falha na comunicacao commit com %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		corpo, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("resposta HTTP %d no commit de %s: %s", resp.StatusCode, url, string(corpo))
	}

	var respDTO CommitRespDTO
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&respDTO); err != nil {
		return nil, fmt.Errorf("falha ao decodificar resposta do commit: %w", err)
	}

	return &respDTO, nil
}

// Abort aciona a Fase 2 (Abort / Rollback de compensação) do 2PC em um nó participante.
func (c *ClienteREST) Abort(ctx context.Context, enderecoBase string, reqDTO AbortReqDTO) (*AbortRespDTO, error) {
	url := formatarURL(enderecoBase, "/api/v1/2pc/abort")
	corpoBytes, err := json.Marshal(reqDTO)
	if err != nil {
		return nil, fmt.Errorf("falha ao serializar abort 2PC: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(corpoBytes))
	if err != nil {
		return nil, fmt.Errorf("falha ao criar requisicao abort 2PC: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("falha na comunicacao abort com %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		corpo, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("resposta HTTP %d no abort de %s: %s", resp.StatusCode, url, string(corpo))
	}

	var respDTO AbortRespDTO
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&respDTO); err != nil {
		return nil, fmt.Errorf("falha ao decodificar resposta do abort: %w", err)
	}

	return &respDTO, nil
}

// SincronizarWAL solicita registros de recuperação a um nó parceiro.
func (c *ClienteREST) SincronizarWAL(ctx context.Context, enderecoBase string, reqDTO SincronizacaoWALReqDTO) (*SincronizacaoWALRespDTO, error) {
	url := formatarURL(enderecoBase, "/api/v1/wal/sincronizar")
	corpoBytes, err := json.Marshal(reqDTO)
	if err != nil {
		return nil, fmt.Errorf("falha ao serializar requisicao de sync WAL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(corpoBytes))
	if err != nil {
		return nil, fmt.Errorf("falha ao criar requisicao sync WAL: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("falha na comunicacao sync WAL com %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		corpo, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("resposta HTTP %d no sync WAL de %s: %s", resp.StatusCode, url, string(corpo))
	}

	var respDTO SincronizacaoWALRespDTO
	if err := json.NewDecoder(io.LimitReader(resp.Body, 10<<20)).Decode(&respDTO); err != nil {
		return nil, fmt.Errorf("falha ao decodificar resposta do sync WAL: %w", err)
	}

	return &respDTO, nil
}
