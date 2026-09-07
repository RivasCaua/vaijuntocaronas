package estado

import (
	"sync"
	"testing"
	"vaijunto/pkg/protocolo"
)

func TestFluxoCaronaEReservaAtoma(t *testing.T) {
	g := NovoGerenciadorEstado()

	// 1. Cadastrar motorista e passageiros
	err := g.CadastrarUsuario("motorista1", "123", "Motorista Silva", protocolo.PapelMotorista)
	if err != nil {
		t.Fatalf("Erro ao cadastrar motorista: %v", err)
	}

	err = g.CadastrarUsuario("passageiro1", "123", "Passageiro João", protocolo.PapelPassageiro)
	if err != nil {
		t.Fatalf("Erro ao cadastrar passageiro 1: %v", err)
	}

	err = g.CadastrarUsuario("passageiro2", "123", "Passageiro Maria", protocolo.PapelPassageiro)
	if err != nil {
		t.Fatalf("Erro ao cadastrar passageiro 2: %v", err)
	}

	// 2. Autenticar
	tokenMot, _, _, err := g.Autenticar("motorista1", "123")
	if err != nil {
		t.Fatalf("Erro ao autenticar motorista: %v", err)
	}

	tokenPass1, _, _, err := g.Autenticar("passageiro1", "123")
	if err != nil {
		t.Fatalf("Erro ao autenticar passageiro 1: %v", err)
	}

	tokenPass2, _, _, err := g.Autenticar("passageiro2", "123")
	if err != nil {
		t.Fatalf("Erro ao autenticar passageiro 2: %v", err)
	}

	// 3. Publicar Carona: Salvador -> Amélia Rodrigues -> Feira de Santana (1 vaga livre por trecho)
	rota := []string{"Salvador", "Amélia Rodrigues", "Feira de Santana"}
	caronaID, err := g.PublicarCarona(tokenMot, rota, "2026-09-20", "08:00", 1, 25.0)
	if err != nil {
		t.Fatalf("Erro ao publicar carona: %v", err)
	}
	if caronaID == "" {
		t.Fatalf("ID de carona invalido")
	}

	// 4. Buscar itinerário de Salvador a Feira de Santana
	itinerarios, err := g.BuscarItinerarios("Salvador", "Feira de Santana", "2026-09-20")
	if err != nil {
		t.Fatalf("Erro ao buscar itinerarios: %v", err)
	}
	if len(itinerarios) != 1 {
		t.Fatalf("Esperava 1 itinerario, recebeu %d", len(itinerarios))
	}

	// 5. Testar Concorrência: Passageiro 1 e Passageiro 2 tentam reservar ao mesmo tempo a única vaga do itinerário completo
	var wg sync.WaitGroup
	wg.Add(2)

	var res1, res2 string
	var err1, err2 error

	go func() {
		defer wg.Done()
		res1, err1 = g.ReservarItinerario(tokenPass1, itinerarios[0].Trechos)
	}()

	go func() {
		defer wg.Done()
		res2, err2 = g.ReservarItinerario(tokenPass2, itinerarios[0].Trechos)
	}()

	wg.Wait()

	// Exatamente um deve ter sucesso e o outro deve falhar por falta de vagas (Reserva Atômica)
	if err1 == nil && err2 == nil {
		t.Fatalf("ERRO: Ambos os passageiros conseguiram reservar a mesma vaga! Falha de concorrência.")
	}

	if err1 != nil && err2 != nil {
		t.Fatalf("ERRO: Ambos falharam ao reservar! err1: %v, err2: %v", err1, err2)
	}

	if err1 == nil {
		t.Logf("[TESTE PASSOU] Passageiro 1 reservou com sucesso: %s. Passageiro 2 foi recusado: %v\n", res1, err2)
	} else {
		t.Logf("[TESTE PASSOU] Passageiro 2 reservou com sucesso: %s. Passageiro 1 foi recusado: %v\n", res2, err1)
	}
}
