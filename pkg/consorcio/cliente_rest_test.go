package consorcio_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"vaijunto/pkg/consorcio"
)

func TestClienteREST_TimeoutEstrito(t *testing.T) {
	// Cria servidor mock com delay maior que o timeout do cliente
	mockSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"sucesso":true}`))
	}))
	defer mockSrv.Close()

	// Cliente com timeout curto de 50ms
	cliente := consorcio.NovoClienteREST(consorcio.ConfigClienteREST{
		NodoLocalID:   "nodo-teste",
		TimeoutPadrao: 50 * time.Millisecond,
	})

	ctx := context.Background()
	_, err := cliente.ObterInfoCluster(ctx, mockSrv.URL)
	if err == nil {
		t.Fatalf("esperava erro de timeout, mas a chamada foi concluida sem erro")
	}
}

func TestClienteREST_CancelamentoPorContexto(t *testing.T) {
	mockSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer mockSrv.Close()

	cliente := consorcio.NovoClienteREST(consorcio.ConfigClienteREST{
		NodoLocalID:   "nodo-teste",
		TimeoutPadrao: 2 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	_, err := cliente.ObterInfoCluster(ctx, mockSrv.URL)
	if err == nil {
		t.Fatalf("esperava erro de cancelamento de contexto")
	}
}

func TestClienteREST_NoInacessivel(t *testing.T) {
	cliente := consorcio.NovoClienteREST(consorcio.ConfigClienteREST{
		NodoLocalID:   "nodo-teste",
		TimeoutPadrao: 100 * time.Millisecond,
	})

	// Endereço inexistente/fechado na porta 59999
	_, err := cliente.ObterInfoCluster(context.Background(), "http://127.0.0.1:59999")
	if err == nil {
		t.Fatalf("esperava erro de conexao recusada para no inacessivel")
	}
}
