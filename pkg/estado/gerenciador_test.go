package estado

import (
	"errors"
	"sync"
	"testing"
	"time"

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

// erroPersistenciaForcado simula uma falha de gravação no WAL (disco cheio, erro de I/O) sem
// precisar de um WAL real - usado pelos quatro testes abaixo para confirmar que CADA operação que
// muta estado e depois persiste reverte corretamente quando a persistência falha.
var erroPersistenciaForcado = errors.New("falha de persistencia forcada pelo teste")

func TestCadastrarUsuarioComPersistencia_RevertSeFalharPersistencia(t *testing.T) {
	g := NovoGerenciadorEstado()

	err := g.CadastrarUsuarioComPersistencia("mot1", "123", "Motorista", protocolo.PapelMotorista, func() error {
		return erroPersistenciaForcado
	})
	if err == nil {
		t.Fatalf("esperava erro quando a persistencia falha")
	}

	// O usuario NAO pode ter sobrado em memoria - senao um retentativa legitima do mesmo cadastro
	// falharia com "ja cadastrado", mesmo o cadastro original nunca tendo sido persistido.
	if errSegundo := g.CadastrarUsuario("mot1", "123", "Motorista", protocolo.PapelMotorista); errSegundo != nil {
		t.Fatalf("BUG: usuario ficou fantasma em memoria apos falha de persistencia - retentativa legitima falhou: %v", errSegundo)
	}
}

func TestPublicarCaronaComPersistencia_RevertSeFalharPersistencia(t *testing.T) {
	g := NovoGerenciadorEstado()
	_ = g.CadastrarUsuario("mot1", "123", "Motorista", protocolo.PapelMotorista)
	tokenMot, _, _, _ := g.Autenticar("mot1", "123")

	_, err := g.PublicarCaronaComPersistencia(tokenMot, []string{"Salvador", "Feira de Santana"}, "2026-11-20", "08:00", "09:30", 1, 20.0, func(string) error {
		return erroPersistenciaForcado
	})
	if err == nil {
		t.Fatalf("esperava erro quando a persistencia falha")
	}

	// A carona NAO pode ter sobrado em memoria: a busca deve continuar vazia.
	itin, errBusca := g.BuscarItinerarios("Salvador", "Feira de Santana", "2026-11-20")
	if errBusca != nil {
		t.Fatalf("erro inesperado ao buscar: %v", errBusca)
	}
	if len(itin) != 0 {
		t.Fatalf("BUG: carona ficou fantasma em memoria apos falha de persistencia (buscavel apesar de nunca persistida)")
	}
}

func TestCancelarCaronaComPersistencia_RevertSeFalharPersistencia(t *testing.T) {
	g := NovoGerenciadorEstado()
	_ = g.CadastrarUsuario("mot1", "123", "Motorista", protocolo.PapelMotorista)
	tokenMot, _, _, _ := g.Autenticar("mot1", "123")
	caronaID, _ := g.PublicarCarona(tokenMot, []string{"Salvador", "Feira de Santana"}, "2026-11-20", "08:00", "09:30", 1, 20.0)

	_ = g.CadastrarUsuario("pass1", "123", "Passageiro", protocolo.PapelPassageiro)
	tokenPass, _, _, _ := g.Autenticar("pass1", "123")
	itin, _ := g.BuscarItinerarios("Salvador", "Feira de Santana", "2026-11-20")
	reservaID, err := g.ReservarItinerario(tokenPass, itin[0].Trechos)
	if err != nil {
		t.Fatalf("falha ao reservar para preparar o teste: %v", err)
	}

	err = g.CancelarCaronaComPersistencia(tokenMot, caronaID, func() error {
		return erroPersistenciaForcado
	})
	if err == nil {
		t.Fatalf("esperava erro quando a persistencia falha")
	}

	// Nem a carona nem a reserva vinculada podem ter sido afetadas pela tentativa revertida.
	caronas, _ := g.ListarCaronasMotorista(tokenMot)
	if len(caronas) != 1 {
		t.Fatalf("BUG: carona nao foi reativada apos falha de persistencia no cancelamento")
	}
	reserva, err := g.ObterReserva(reservaID)
	if err != nil || reserva.Status != "ATIVA" {
		t.Fatalf("BUG: reserva vinculada deveria continuar ATIVA apos reversao do cancelamento de carona (status: %v, err: %v)", reserva, err)
	}
}

// TestCancelarReservaComPersistencia_NaoAbreJanelaDeCorrida reproduz, de forma determinística, o
// cenário exato que uma versão anterior deste código deixava escapar: uma reserva concorrente
// tentando ocupar a MESMA vaga sendo liberada por um cancelamento cuja persistência ainda não se
// sabe se vai suceder. Ao manter a trava presa durante TODO o ciclo (mutação + persistência) de
// CancelarReservaComPersistencia, a tentativa concorrente NUNCA pode ver a vaga livre antes do
// cancelamento original já ter decidido seu destino (sucesso ou reversão completa).
func TestCancelarReservaComPersistencia_NaoAbreJanelaDeCorrida(t *testing.T) {
	g := NovoGerenciadorEstado()
	_ = g.CadastrarUsuario("mot1", "123", "Motorista", protocolo.PapelMotorista)
	tokenMot, _, _, _ := g.Autenticar("mot1", "123")
	caronaID, _ := g.PublicarCarona(tokenMot, []string{"Salvador", "Feira de Santana"}, "2026-11-20", "08:00", "09:30", 1, 20.0)

	_ = g.CadastrarUsuario("passA", "123", "Passageiro A", protocolo.PapelPassageiro)
	tokenA, _, _, _ := g.Autenticar("passA", "123")
	itinA, _ := g.BuscarItinerarios("Salvador", "Feira de Santana", "2026-11-20")
	reservaA, err := g.ReservarItinerario(tokenA, itinA[0].Trechos)
	if err != nil {
		t.Fatalf("falha ao preparar reserva A: %v", err)
	}

	_ = g.CadastrarUsuario("passB", "123", "Passageiro B", protocolo.PapelPassageiro)
	tokenB, _, _, _ := g.Autenticar("passB", "123")

	liberarPersistencia := make(chan struct{})
	entrouNaPersistencia := make(chan struct{})
	var wg sync.WaitGroup
	var erroCancelamento error

	wg.Add(1)
	go func() {
		defer wg.Done()
		erroCancelamento = g.CancelarReservaComPersistencia(tokenA, reservaA, func() error {
			close(entrouNaPersistencia) // sinaliza que a trava ja foi adquirida e a mutacao ja ocorreu
			<-liberarPersistencia       // mantem a trava presa ate o teste mandar continuar
			return erroPersistenciaForcado
		})
	}()

	// So dispara a tentativa concorrente de B DEPOIS de confirmar que A ja esta com a trava presa
	// dentro da persistencia - caso contrario B poderia, por mero acaso de escalonamento, rodar
	// (e falhar rapido por falta de vaga) antes de A sequer comecar, o que seria um falso positivo
	// deste teste, nao uma violacao real da garantia.
	<-entrouNaPersistencia

	resultadoB := make(chan error, 1)
	go func() {
		_, errB := g.ReservarItinerario(tokenB, []protocolo.ItemTrecho{{CaronaID: caronaID, Origem: "Salvador", Destino: "Feira de Santana"}})
		resultadoB <- errB
	}()

	select {
	case <-resultadoB:
		t.Fatalf("BUG: a reserva concorrente de B conseguiu prosseguir ANTES do cancelamento de A ter decidido seu destino")
	case <-time.After(150 * time.Millisecond):
		// Esperado: B continua bloqueado, provando que a vaga nunca ficou visivelmente livre.
	}

	close(liberarPersistencia)
	wg.Wait()

	if erroCancelamento == nil {
		t.Fatalf("esperava que o cancelamento de A falhasse (persistencia forcada a falhar)")
	}

	// Como a persistencia falhou, o cancelamento de A foi revertido por completo: A continua dono
	// da unica vaga. B NUNCA deveria conseguir reservar - a vaga jamais ficou de fato livre, apenas
	// pareceu livre por um instante dentro da trava, e a trava impediu B de agir nesse instante.
	errB := <-resultadoB
	if errB == nil {
		t.Fatalf("BUG DE OVERBOOKING: B conseguiu reservar uma vaga que nunca ficou de fato livre (cancelamento de A foi revertido)")
	}

	reservaAFinal, _ := g.ObterReserva(reservaA)
	if reservaAFinal.Status != "ATIVA" {
		t.Fatalf("reserva de A deveria ter sido revertida para ATIVA, obteve '%s'", reservaAFinal.Status)
	}

	// A carona tem 1 vaga, ocupada por A: a busca deve continuar vazia (nenhuma vaga livre), e jamais
	// deve existir uma segunda reserva ATIVA (de B) para o mesmo trecho.
	itinFinal, _ := g.BuscarItinerarios("Salvador", "Feira de Santana", "2026-11-20")
	if len(itinFinal) != 0 {
		t.Fatalf("BUG: vaga aparece livre apos reversao do cancelamento, mas A deveria continuar ocupando-a")
	}
}
