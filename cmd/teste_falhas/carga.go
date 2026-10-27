package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"vaijunto/pkg/protocolo"
)

// statNo acumula, de forma atomica, os resultados da carga de fundo contra um unico no.
type statNo struct {
	ok    int64
	falha int64
}

// ContadorCarga agrega as estatisticas de carga continua de todos os nos durante a janela de caos.
type ContadorCarga struct {
	mu    sync.Mutex
	porNo map[string]*statNo
}

// NovoContadorCarga inicializa o agregador de estatisticas de carga.
func NovoContadorCarga() *ContadorCarga {
	return &ContadorCarga{porNo: make(map[string]*statNo)}
}

func (c *ContadorCarga) registrar(nodoID string, sucesso bool) {
	c.mu.Lock()
	s, existe := c.porNo[nodoID]
	if !existe {
		s = &statNo{}
		c.porNo[nodoID] = s
	}
	c.mu.Unlock()

	if sucesso {
		atomic.AddInt64(&s.ok, 1)
	} else {
		atomic.AddInt64(&s.falha, 1)
	}
}

// Snapshot retorna uma copia estavel (ok, falha) por no, para impressao no relatorio final.
func (c *ContadorCarga) Snapshot() map[string][2]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make(map[string][2]int64, len(c.porNo))
	for nodo, s := range c.porNo {
		out[nodo] = [2]int64{atomic.LoadInt64(&s.ok), atomic.LoadInt64(&s.falha)}
	}
	return out
}

// geradorCargaContinua mantem um fluxo constante de buscas de itinerarios reais (somente leitura)
// contra um unico no do consorcio, do inicio da linha de base ate o fim da janela de caos. Serve
// como a "testemunha" que prova, com numeros, que os nos sobreviventes continuam respondendo
// normalmente enquanto o no abatido falha (e apenas ele).
func geradorCargaContinua(ctx context.Context, wg *sync.WaitGroup, spec nodeSpec, data string, stats *ContadorCarga) {
	defer wg.Done()

	origem, destino := rotaBaseline(spec.ID)
	endereco := fmt.Sprintf("127.0.0.1:%d", spec.PortaTCP)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		sucesso := executarUmaRequisicaoDeLeitura(endereco, origem, destino, data)
		stats.registrar(spec.ID, sucesso)

		select {
		case <-ctx.Done():
			return
		case <-time.After(120 * time.Millisecond):
		}
	}
}

func executarUmaRequisicaoDeLeitura(endereco, origem, destino, data string) bool {
	conn, err := net.DialTimeout("tcp", endereco, 800*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()

	scanner := protocolo.CriarScanner(conn)
	if err := protocolo.EnviarMensagem(conn, protocolo.Requisicao{
		Acao: protocolo.AcaoBuscarItinerarios, Origem: origem, Destino: destino, Data: data,
	}); err != nil {
		return false
	}

	var resp protocolo.Resposta
	if err := protocolo.LerMensagem(scanner, &resp); err != nil {
		return false
	}
	return resp.Status == protocolo.StatusOK
}
