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
	caronaID, err := g.PublicarCarona(tokenMot, rota, "2026-09-20", "08:00", "09:30", 1, 25.0)
	if err != nil || caronaID == "" {
		t.Fatalf("Erro ao publicar carona: %v", err)
	}

	// 4. Buscar itinerário de Salvador a Feira de Santana
	itinerarios, err := g.BuscarItinerarios("Salvador", "Feira de Santana", "2026-09-20")
	if err != nil {
		t.Fatalf("Erro ao buscar itinerarios: %v", err)
	}
	if len(itinerarios) != 1 {
		t.Fatalf("Esperava 1 itinerario, recebeu %d", len(itinerarios))
	}

	// 5. Testar Concorrência: Passageiro 1 e Passageiro 2 tentam reservar ao mesmo tempo
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

	if err1 == nil && err2 == nil {
		t.Fatalf("ERRO: Ambos os passageiros conseguiram reservar a mesma vaga! Falha de concorrência.")
	}

	reservaGanhadora := res1
	tokenGanhador := tokenPass1
	if err1 != nil {
		reservaGanhadora = res2
		tokenGanhador = tokenPass2
	}

	// 6. Testar Listagem de Reservas do Passageiro
	reservasPass, err := g.ListarReservasPassageiro(tokenGanhador)
	if err != nil || len(reservasPass) != 1 {
		t.Fatalf("Erro ao listar reservas do passageiro ganhador: %v", err)
	}

	// 7. Testar Cancelamento de Reserva (devolve vagas)
	err = g.CancelarReserva(tokenGanhador, reservaGanhadora)
	if err != nil {
		t.Fatalf("Erro ao cancelar reserva: %v", err)
	}

	// 8. Buscar novamente: com a vaga devolvida, a carona deve voltar a ficar disponível!
	itinerariosNovos, err := g.BuscarItinerarios("Salvador", "Feira de Santana", "2026-09-20")
	if err != nil || len(itinerariosNovos) != 1 {
		t.Fatalf("Esperava que a carona ficasse disponivel novamente apos o cancelamento! Recebeu: %d", len(itinerariosNovos))
	}

	// 9. Testar Listar e Cancelar Carona do Motorista
	caronasMot, err := g.ListarCaronasMotorista(tokenMot)
	if err != nil || len(caronasMot) != 1 {
		t.Fatalf("Erro ao listar caronas do motorista: %v", err)
	}

	err = g.CancelarCarona(tokenMot, caronaID)
	if err != nil {
		t.Fatalf("Erro ao cancelar carona pelo motorista: %v", err)
	}

	t.Logf("[TESTE COMPLETO PASSOU] Reserva atômica, listagem e cancelamentos (passageiro/motorista) funcionando perfeitamente.")
}
