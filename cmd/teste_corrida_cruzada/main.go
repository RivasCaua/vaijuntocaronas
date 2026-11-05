// Comando teste_corrida_cruzada audita especificamente o cenario que o item 4 do barema pede para
// ser demonstrado: "reservas simultaneas por empresas diferentes: so uma confirma". Diferente do
// cmd/teste_carga (que dispara todos os clientes contra o MESMO servidor), aqui cada passageiro
// concorrente se conecta a uma empresa de origem diferente do consorcio - exercitando a busca
// federada (item 3) e provando que a exclusao mutua do dono do trecho vale mesmo quando o pedido
// chega pela rede, originado de um processo de empresa distinta, e nao so entre clientes do mesmo
// servidor.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vaijunto/pkg/protocolo"
)

func main() {
	servidoresFlag := flag.String("servidores", "localhost:8080", "Enderecos IP:Porta das empresas do consorcio, separados por virgula (a 1a publica a carona disputada)")
	totalClientes := flag.Int("clientes", 12, "Numero de passageiros concorrentes, distribuidos entre as empresas informadas")
	vagas := flag.Int("vagas", 1, "Vagas da carona disputada (o 'ultimo assento' do enunciado)")
	flag.Parse()

	servidores := []string{}
	for _, s := range strings.Split(*servidoresFlag, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			servidores = append(servidores, s)
		}
	}
	if len(servidores) < 2 {
		fmt.Println("[ERRO CRITICO] Informe pelo menos 2 empresas em -servidores para caracterizar disputa CRUZADA entre empresas")
		os.Exit(1)
	}

	fmt.Println("================================================================")
	fmt.Println("   VaiJunto - Auditoria de Disputa CRUZADA entre Empresas (Item 4)")
	fmt.Println("================================================================")
	fmt.Printf("Empresa dona da carona:        %s\n", servidores[0])
	fmt.Printf("Empresas de origem dos clientes: %s\n", strings.Join(servidores, ", "))
	fmt.Printf("Clientes concorrentes:         %d\n", *totalClientes)
	fmt.Printf("Vagas na carona disputada:     %d\n", *vagas)
	fmt.Println("================================================================")

	origem, destino := "Salvador", "Feira de Santana"
	data := "2026-12-01"

	// FASE 1: motorista publica a carona disputada na empresa dona (servidores[0]).
	connMot, err := net.Dial("tcp", servidores[0])
	if err != nil {
		fmt.Printf("[ERRO CRITICO] Nao foi possivel conectar a empresa dona (%s): %v\n", servidores[0], err)
		os.Exit(1)
	}
	defer connMot.Close()
	scannerMot := protocolo.CriarScanner(connMot)

	loginMot := fmt.Sprintf("mot_cruzada_%d", time.Now().UnixNano())
	protocolo.EnviarMensagem(connMot, protocolo.Requisicao{
		Acao: protocolo.AcaoCadastrar, Usuario: loginMot, Senha: "123",
		Nome: "Motorista Disputa Cruzada", Papel: protocolo.PapelMotorista,
	})
	var respCad protocolo.Resposta
	protocolo.LerMensagem(scannerMot, &respCad)

	protocolo.EnviarMensagem(connMot, protocolo.Requisicao{Acao: protocolo.AcaoAutenticar, Usuario: loginMot, Senha: "123"})
	var respAuth protocolo.Resposta
	protocolo.LerMensagem(scannerMot, &respAuth)

	protocolo.EnviarMensagem(connMot, protocolo.Requisicao{
		Acao: protocolo.AcaoPublicarCarona, Token: respAuth.Token,
		Rota: []string{origem, destino}, Data: data,
		Horario: "09:00", HorarioChegada: "10:30",
		Assentos: *vagas, PrecoPorTrecho: 30.0,
	})
	var respPub protocolo.Resposta
	protocolo.LerMensagem(scannerMot, &respPub)
	fmt.Printf("\n[PREPARACAO] Carona disputada publicada em %s: %s -> %s, ID %s, %d vaga(s)\n\n",
		servidores[0], origem, destino, respPub.CaronaID, *vagas)

	// FASE 2: N passageiros concorrentes, cada um conectado a uma empresa de origem diferente
	// (round-robin pela lista de -servidores), buscando e reservando ao mesmo tempo.
	type resultadoOrigem struct {
		aprovadas int64
		recusadas int64
	}
	var (
		mu           sync.Mutex
		porOrigem    = map[string]*resultadoOrigem{}
		aprovTotal   int64
		recusTotal   int64
		falhasConn   int64
		wg           sync.WaitGroup
	)
	for _, s := range servidores {
		porOrigem[s] = &resultadoOrigem{}
	}

	fmt.Printf("🚀 Disparando %d passageiros, cada um entrando por uma empresa distinta, disputando o(s) %d assento(s)...\n\n", *totalClientes, *vagas)

	inicio := time.Now()
	for i := 1; i <= *totalClientes; i++ {
		wg.Add(1)
		id := i
		servidorOrigem := servidores[(id-1)%len(servidores)]

		go func(id int, origemServidor string) {
			defer wg.Done()

			conn, err := net.Dial("tcp", origemServidor)
			if err != nil {
				atomic.AddInt64(&falhasConn, 1)
				return
			}
			defer conn.Close()
			scanner := protocolo.CriarScanner(conn)
			usuario := fmt.Sprintf("pass_cruzada_%d_%d", id, time.Now().UnixNano())

			protocolo.EnviarMensagem(conn, protocolo.Requisicao{
				Acao: protocolo.AcaoCadastrar, Usuario: usuario, Senha: "123",
				Nome: fmt.Sprintf("Passageiro Cruzado #%d", id), Papel: protocolo.PapelPassageiro,
			})
			var rc protocolo.Resposta
			protocolo.LerMensagem(scanner, &rc)

			protocolo.EnviarMensagem(conn, protocolo.Requisicao{Acao: protocolo.AcaoAutenticar, Usuario: usuario, Senha: "123"})
			var ra protocolo.Resposta
			protocolo.LerMensagem(scanner, &ra)

			protocolo.EnviarMensagem(conn, protocolo.Requisicao{
				Acao: protocolo.AcaoBuscarItinerarios, Origem: origem, Destino: destino, Data: data,
			})
			var rb protocolo.Resposta
			protocolo.LerMensagem(scanner, &rb)

			aprovado := false
			if len(rb.Itinerarios) > 0 {
				protocolo.EnviarMensagem(conn, protocolo.Requisicao{
					Acao: protocolo.AcaoReservar, Token: ra.Token, Trechos: rb.Itinerarios[0].Trechos,
				})
				var rr protocolo.Resposta
				protocolo.LerMensagem(scanner, &rr)
				aprovado = rr.Status == protocolo.StatusOK
			}

			mu.Lock()
			if aprovado {
				atomic.AddInt64(&aprovTotal, 1)
				porOrigem[origemServidor].aprovadas++
			} else {
				atomic.AddInt64(&recusTotal, 1)
				porOrigem[origemServidor].recusadas++
			}
			mu.Unlock()
		}(id, servidorOrigem)
	}
	wg.Wait()
	duracao := time.Since(inicio)

	fmt.Println("================================================================")
	fmt.Println("        RELATORIO DA DISPUTA CRUZADA ENTRE EMPRESAS")
	fmt.Println("================================================================")
	fmt.Printf("Tempo total:                   %v\n", duracao)
	fmt.Printf("Clientes disparados:           %d\n", *totalClientes)
	fmt.Printf("Vagas iniciais:                %d\n", *vagas)
	fmt.Printf("Aprovadas / Recusadas:         %d / %d\n", aprovTotal, recusTotal)
	fmt.Printf("Falhas de conexao TCP:         %d\n", falhasConn)
	fmt.Println("----------------------------------------------------------------")
	fmt.Println("POR EMPRESA DE ORIGEM DO PASSAGEIRO:")
	for _, s := range servidores {
		r := porOrigem[s]
		fmt.Printf("  %-26s aprovadas=%-3d recusadas=%-3d\n", s, r.aprovadas, r.recusadas)
	}
	fmt.Println("----------------------------------------------------------------")
	if aprovTotal == int64(*vagas) {
		fmt.Printf("✅ SUCESSO: exatamente %d vaga(s) vendida(s), disputadas por passageiros entrando por %d empresas diferentes.\n", aprovTotal, len(servidores))
		fmt.Println("✅ Nenhuma venda dupla cruzando empresas — a exclusao mutua do dono do trecho vale mesmo com pedidos chegando pela rede.")
	} else if aprovTotal < int64(*vagas) {
		fmt.Printf("⚠️ AVISO: apenas %d de %d vagas foram vendidas.\n", aprovTotal, *vagas)
	} else {
		fmt.Printf("❌ ERRO CRITICO: overbooking cruzando empresas! %d vendidas para %d disponiveis.\n", aprovTotal, *vagas)
	}
	fmt.Println("================================================================")
}
