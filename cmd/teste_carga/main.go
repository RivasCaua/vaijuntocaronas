package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"vaijunto/pkg/protocolo"
)

func main() {
	enderecoServidor := flag.String("servidor", "localhost:8080", "Endereco do servidor central (IP:Porta)")
	totalClientes := flag.Int("clientes", 20, "Numero de clientes passageiros concorrentes")
	vagasCarona := flag.Int("vagas", 5, "Numero de assentos disponiveis na carona do teste")
	flag.Parse()

	fmt.Println("================================================================")
	fmt.Println("   VaiJunto - Robô de Teste de Carga e Concorrência Massiva")
	fmt.Printf("Servidor: %s | Clientes Concorrentes: %d | Vagas na Carona: %d\n", *enderecoServidor, *totalClientes, *vagasCarona)
	fmt.Println("================================================================")

	// FASE 1: Preparar Motorista e Publicar Carona Única com Vagas Limitadas
	connMot, err := net.Dial("tcp", *enderecoServidor)
	if err != nil {
		fmt.Printf("[ERRO CRÍTICO] Nao foi possivel conectar ao servidor: %v\n", err)
		os.Exit(1)
	}
	defer connMot.Close()

	scannerMot := protocolo.CriarScanner(connMot)

	loginMot := fmt.Sprintf("mot_carga_%d", time.Now().UnixNano())
	protocolo.EnviarMensagem(connMot, protocolo.Requisicao{
		Acao:    protocolo.AcaoCadastrar,
		Usuario: loginMot,
		Senha:   "123",
		Nome:    "Motorista de Teste de Carga",
		Papel:   protocolo.PapelMotorista,
	})
	var respCadMot protocolo.Resposta
	protocolo.LerMensagem(scannerMot, &respCadMot)

	protocolo.EnviarMensagem(connMot, protocolo.Requisicao{
		Acao:    protocolo.AcaoAutenticar,
		Usuario: loginMot,
		Senha:   "123",
	})
	var respAuthMot protocolo.Resposta
	protocolo.LerMensagem(scannerMot, &respAuthMot)
	tokenMot := respAuthMot.Token

	dataTeste := "2026-10-15"
	rotaTeste := []string{"Salvador", "Amélia Rodrigues", "Feira de Santana"}

	protocolo.EnviarMensagem(connMot, protocolo.Requisicao{
		Acao:           protocolo.AcaoPublicarCarona,
		Token:          tokenMot,
		Rota:           rotaTeste,
		Data:           dataTeste,
		Horario:        "08:00",
		HorarioChegada: "09:30",
		Assentos:       *vagasCarona,
		PrecoPorTrecho: 20.0,
	})
	var respPubCarona protocolo.Resposta
	protocolo.LerMensagem(scannerMot, &respPubCarona)
	fmt.Printf("[PREPARAÇÃO] Carona de teste publicada com ID: %s (%d vagas para %d clientes)\n\n", respPubCarona.CaronaID, *vagasCarona, *totalClientes)

	// FASE 2: Início da Tempestade de Requisições Concorrentes por TCP
	var (
		reservasAprovadas int64
		reservasRecusadas int64
		falhasConexao     int64
		tempoTotalMs      int64
		wg                sync.WaitGroup
	)

	fmt.Printf("🚀 Disparando %d clientes simultaneos em goroutines para tentar reservar a vaga ao mesmo tempo...\n\n", *totalClientes)

	inicioSimulacao := time.Now()

	for i := 1; i <= *totalClientes; i++ {
		wg.Add(1)
		idPassageiro := i

		go func(id int) {
			defer wg.Done()

			inicioCliente := time.Now()
			connPass, err := net.Dial("tcp", *enderecoServidor)
			if err != nil {
				atomic.AddInt64(&falhasConexao, 1)
				return
			}
			defer connPass.Close()

			scannerPass := protocolo.CriarScanner(connPass)
			userPass := fmt.Sprintf("pass_carga_%d_%d", id, time.Now().UnixNano())

			// 1. Cadastrar Passageiro
			protocolo.EnviarMensagem(connPass, protocolo.Requisicao{
				Acao:    protocolo.AcaoCadastrar,
				Usuario: userPass,
				Senha:   "123",
				Nome:    fmt.Sprintf("Passageiro Carga #%d", id),
				Papel:   protocolo.PapelPassageiro,
			})
			var respCadPass protocolo.Resposta
			protocolo.LerMensagem(scannerPass, &respCadPass)

			// 2. Autenticar Passageiro
			protocolo.EnviarMensagem(connPass, protocolo.Requisicao{
				Acao:    protocolo.AcaoAutenticar,
				Usuario: userPass,
				Senha:   "123",
			})
			var respAuthPass protocolo.Resposta
			protocolo.LerMensagem(scannerPass, &respAuthPass)
			tokenPass := respAuthPass.Token

			// 3. Buscar Itinerários
			protocolo.EnviarMensagem(connPass, protocolo.Requisicao{
				Acao:    protocolo.AcaoBuscarItinerarios,
				Origem:  "Salvador",
				Destino: "Feira de Santana",
				Data:    dataTeste,
			})
			var respBusca protocolo.Resposta
			protocolo.LerMensagem(scannerPass, &respBusca)

			if len(respBusca.Itinerarios) > 0 {
				// 4. Tentativa de Reserva Atômica
				protocolo.EnviarMensagem(connPass, protocolo.Requisicao{
					Acao:    protocolo.AcaoReservar,
					Token:   tokenPass,
					Trechos: respBusca.Itinerarios[0].Trechos,
				})
				var respReserva protocolo.Resposta
				protocolo.LerMensagem(scannerPass, &respReserva)

				if respReserva.Status == protocolo.StatusOK {
					atomic.AddInt64(&reservasAprovadas, 1)
				} else {
					atomic.AddInt64(&reservasRecusadas, 1)
				}
			} else {
				atomic.AddInt64(&reservasRecusadas, 1)
			}

			duracaoMs := time.Since(inicioCliente).Milliseconds()
			atomic.AddInt64(&tempoTotalMs, duracaoMs)
		}(idPassageiro)
	}

	wg.Wait()
	duracaoTotalSimulacao := time.Since(inicioSimulacao)

	// FASE 3: Relatório da Carga e Auditoria de Concorrência
	fmt.Println("================================================================")
	fmt.Println("           RELATÓRIO DO TESTE DE CARGA E ESTRESSE")
	fmt.Println("================================================================")
	fmt.Printf("Tempo Total do Teste:         %v\n", duracaoTotalSimulacao)
	fmt.Printf("Total de Clientes Disparados: %d\n", *totalClientes)
	fmt.Printf("Vagas Iniciais na Carona:     %d\n", *vagasCarona)
	fmt.Printf("Reservas APROVADAS (OK):      %d\n", reservasAprovadas)
	fmt.Printf("Reservas RECUSADAS (ERRO):    %d\n", reservasRecusadas)
	fmt.Printf("Falhas de Conexão TCP:        %d\n", falhasConexao)

	if *totalClientes > 0 {
		latenciaMedia := float64(tempoTotalMs) / float64(*totalClientes)
		fmt.Printf("Latência Média por Cliente:   %.2f ms\n", latenciaMedia)
	}

	fmt.Println("----------------------------------------------------------------")
	fmt.Println("AUDITORIA DE CONCORRÊNCIA E PREVENÇÃO DE OVERBOOKING:")

	if reservasAprovadas == int64(*vagasCarona) {
		fmt.Printf("✅ SUCESSO ABSOLUTO: Exatamente %d vagas foram vendidas para %d candidatos.\n", reservasAprovadas, *totalClientes)
		fmt.Println("✅ Nenhuma vaga foi vendida em duplicidade (0 OVERBOOKING).")
		fmt.Println("✅ Exclusão Mútua (sync.RWMutex.Lock) e Reserva Atômica validadas sob carga!")
	} else if reservasAprovadas < int64(*vagasCarona) {
		fmt.Printf("⚠️ AVISO: Apenas %d vagas de %d foram reservadas.\n", reservasAprovadas, *vagasCarona)
	} else {
		fmt.Printf("❌ ERRO CRÍTICO DE CONCORRÊNCIA: Overbooking detectado! %d vagas vendidas para %d disponiveis!\n", reservasAprovadas, *vagasCarona)
	}
	fmt.Println("================================================================")
}
