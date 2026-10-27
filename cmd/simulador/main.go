// Comando simulador implementa um robô de carga sustentada e autônomo: dezenas de passageiros
// virtuais, sem qualquer interface gráfica ou interativa, buscam e reservam itinerários reais
// contra um ou mais nós do consórcio VaiJunto durante um período configurável, reportando ao
// final throughput, taxa de sucesso e latência (média/p50/p95/p99) por tipo de operação.
//
// Diferente do cmd/teste_carga (que audita condição de corrida disparando N clientes contra
// UMA única carona no mesmo instante), o simulador reproduz um padrão de uso realista e
// prolongado: motoristas semeiam o inventário de caronas em todos os nós informados, e então
// passageiros autônomos buscam, reservam e cancelam continuamente, em ciclos aleatórios de
// "tempo de pensar", distribuindo a carga entre as empresas do consórcio.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vaijunto/pkg/estado"
	"vaijunto/pkg/protocolo"
)

func main() {
	servidoresFlag := flag.String("servidores", "localhost:8080", "Enderecos IP:Porta dos nos do consorcio, separados por virgula")
	totalPassageiros := flag.Int("passageiros", 50, "Numero de passageiros virtuais autonomos e concorrentes")
	totalMotoristas := flag.Int("motoristas", 6, "Numero de motoristas usados para semear caronas antes da carga")
	caronasPorMotorista := flag.Int("caronas-por-motorista", 2, "Caronas publicadas por cada motorista semeador")
	duracao := flag.Duration("duracao", 30*time.Second, "Duracao total da simulacao de carga sustentada")
	tempoPensar := flag.Duration("intervalo", 250*time.Millisecond, "Tempo medio de 'pensar' entre acoes de cada passageiro")
	probReserva := flag.Float64("prob-reserva", 0.5, "Probabilidade de tentar reservar apos uma busca com resultados")
	probCancelamento := flag.Float64("prob-cancelamento", 0.2, "Probabilidade de uma reserva ativa ser cancelada depois")
	dataViagem := flag.String("data", "2026-11-10", "Data de viagem usada nas caronas semeadas e nas buscas")
	relatorioIntervalo := flag.Duration("relatorio-intervalo", 5*time.Second, "Intervalo do status parcial impresso durante a execucao (0 desativa)")
	seedFlag := flag.Int64("seed", 0, "Semente do gerador aleatorio (0 = baseado no horario atual)")
	flag.Parse()

	enderecos := interpretarEnderecos(*servidoresFlag)
	if len(enderecos) == 0 {
		fmt.Println("[ERRO CRITICO] Nenhum endereco de servidor valido informado em -servidores")
		os.Exit(1)
	}

	semente := *seedFlag
	if semente == 0 {
		semente = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(semente))

	fmt.Println("================================================================")
	fmt.Println("   VaiJunto - Simulador Autonomo de Passageiros (Carga Sustentada)")
	fmt.Println("================================================================")
	fmt.Printf("Nos do consorcio alvo:        %s\n", strings.Join(enderecos, ", "))
	fmt.Printf("Passageiros virtuais:         %d\n", *totalPassageiros)
	fmt.Printf("Motoristas semeadores:        %d (%d caronas cada)\n", *totalMotoristas, *caronasPorMotorista)
	fmt.Printf("Duracao da simulacao:         %s\n", duracao.String())
	fmt.Printf("Tempo medio de 'pensar':      %s\n", tempoPensar.String())
	fmt.Printf("Prob. reserva / cancelamento: %.0f%% / %.0f%%\n", *probReserva*100, *probCancelamento*100)
	fmt.Printf("Semente aleatoria:            %d\n", semente)
	fmt.Println("================================================================")

	metricas := NovasMetricas()

	// --- FASE 1: SEMEADURA DO INVENTARIO DE CARONAS ---
	fmt.Println("\n[SEED] Semeando caronas em todos os nos do consorcio...")
	inicioSeed := time.Now()
	caronasSemeadas := semearCaronas(enderecos, *totalMotoristas, *caronasPorMotorista, *dataViagem, rng, metricas)
	if caronasSemeadas == 0 {
		fmt.Println("[ERRO CRITICO] Nenhuma carona pode ser semeada. Verifique se os servidores estao no ar.")
		os.Exit(1)
	}
	fmt.Printf("[SEED] %d caronas publicadas com sucesso em %s.\n", caronasSemeadas, time.Since(inicioSeed).Round(time.Millisecond))

	// --- FASE 2: DISPARO DOS PASSAGEIROS AUTONOMOS ---
	fmt.Printf("\n[CARGA] Iniciando %d passageiros autonomos por %s...\n\n", *totalPassageiros, duracao.String())

	fimSimulacao := time.Now().Add(*duracao)
	pararStatus := make(chan struct{})
	var wgStatus sync.WaitGroup
	if *relatorioIntervalo > 0 {
		wgStatus.Add(1)
		go imprimirStatusParcial(metricas, fimSimulacao, *relatorioIntervalo, pararStatus, &wgStatus)
	}

	var wg sync.WaitGroup
	inicioCarga := time.Now()

	for i := 1; i <= *totalPassageiros; i++ {
		wg.Add(1)
		idPassageiro := i
		enderecoEscolhido := enderecos[rng.Intn(len(enderecos))]
		localSeed := rng.Int63()

		go func(id int, endereco string, seedLocal int64) {
			defer wg.Done()
			executarPassageiroAutonomo(id, endereco, *dataViagem, fimSimulacao, *tempoPensar, *probReserva, *probCancelamento, seedLocal, metricas)
		}(idPassageiro, enderecoEscolhido, localSeed)
	}

	wg.Wait()
	duracaoReal := time.Since(inicioCarga)

	if *relatorioIntervalo > 0 {
		close(pararStatus)
		wgStatus.Wait()
	}

	imprimirRelatorioFinal(metricas, duracaoReal, enderecos, *totalPassageiros, caronasSemeadas)
}

// interpretarEnderecos converte a flag "-servidores" (lista separada por virgula) em um slice limpo.
func interpretarEnderecos(flagValor string) []string {
	var enderecos []string
	for _, parte := range strings.Split(flagValor, ",") {
		parte = strings.TrimSpace(parte)
		if parte != "" {
			enderecos = append(enderecos, parte)
		}
	}
	return enderecos
}

// --- SEMEADURA DE CARONAS ---

type caronaSeeded struct {
	Endereco string
	CaronaID string
	Origem   string
	Destino  string
}

// semearCaronas cadastra motoristas virtuais e publica caronas regionais aleatorias e validas
// distribuidas round-robin entre todos os nos informados, garantindo inventario para a carga.
func semearCaronas(enderecos []string, totalMotoristas, caronasPorMotorista int, data string, rng *rand.Rand, m *Metricas) int {
	cidades := estado.ObterCidadesRegionais()
	total := 0

	for i := 1; i <= totalMotoristas; i++ {
		endereco := enderecos[(i-1)%len(enderecos)]

		conn, err := net.DialTimeout("tcp", endereco, 5*time.Second)
		if err != nil {
			fmt.Printf("[SEED][AVISO] Motorista #%d nao conseguiu conectar em %s: %v\n", i, endereco, err)
			continue
		}

		scanner := protocolo.CriarScanner(conn)
		login := fmt.Sprintf("mot_sim_%d_%d", i, time.Now().UnixNano())

		if !requisitarEsperandoOK(conn, scanner, protocolo.Requisicao{
			Acao: protocolo.AcaoCadastrar, Usuario: login, Senha: "123",
			Nome: fmt.Sprintf("Motorista Simulado #%d", i), Papel: protocolo.PapelMotorista,
		}, m, "CADASTRAR_MOTORISTA") {
			conn.Close()
			continue
		}

		var respAuth protocolo.Resposta
		if !autenticar(conn, scanner, login, "123", &respAuth, m) {
			conn.Close()
			continue
		}
		token := respAuth.Token

		for c := 0; c < caronasPorMotorista; c++ {
			origem, destino := escolherRotaValida(cidades, rng)
			rota := []string{origem, destino}
			assentos := 3 + rng.Intn(6)
			preco := 10.0 + float64(rng.Intn(30))
			horario := fmt.Sprintf("%02d:%02d", 5+rng.Intn(14), []int{0, 15, 30, 45}[rng.Intn(4)])

			inicio := time.Now()
			protocolo.EnviarMensagem(conn, protocolo.Requisicao{
				Acao: protocolo.AcaoPublicarCarona, Token: token, Rota: rota,
				Data: data, Horario: horario, HorarioChegada: horario, Assentos: assentos, PrecoPorTrecho: preco,
			})
			var resp protocolo.Resposta
			errLer := protocolo.LerMensagem(scanner, &resp)
			sucesso := errLer == nil && resp.Status == protocolo.StatusOK
			m.Registrar("PUBLICAR_CARONA", sucesso, time.Since(inicio))

			if sucesso {
				total++
			}
		}

		conn.Close()
	}

	return total
}

// escolherRotaValida sorteia um par origem/destino distinto do catalogo regional.
func escolherRotaValida(cidades []string, rng *rand.Rand) (string, string) {
	origem := cidades[rng.Intn(len(cidades))]
	destino := origem
	for destino == origem {
		destino = cidades[rng.Intn(len(cidades))]
	}
	return origem, destino
}

// --- PASSAGEIRO AUTONOMO ---

// executarPassageiroAutonomo mantem uma conexao TCP persistente e, em laco, realiza buscas,
// reservas e eventuais cancelamentos ate o horario limite da simulacao ser atingido.
func executarPassageiroAutonomo(id int, endereco, data string, fim time.Time, tempoPensar time.Duration, probReserva, probCancelamento float64, seed int64, m *Metricas) {
	rng := rand.New(rand.NewSource(seed))
	cidades := estado.ObterCidadesRegionais()

	conn, err := net.DialTimeout("tcp", endereco, 5*time.Second)
	if err != nil {
		atomic.AddInt64(&m.falhasConexao, 1)
		return
	}
	defer conn.Close()

	scanner := protocolo.CriarScanner(conn)
	login := fmt.Sprintf("pass_sim_%d_%d", id, time.Now().UnixNano())

	if !requisitarEsperandoOK(conn, scanner, protocolo.Requisicao{
		Acao: protocolo.AcaoCadastrar, Usuario: login, Senha: "123",
		Nome: fmt.Sprintf("Passageiro Simulado #%d", id), Papel: protocolo.PapelPassageiro,
	}, m, "CADASTRAR_PASSAGEIRO") {
		return
	}

	var respAuth protocolo.Resposta
	if !autenticar(conn, scanner, login, "123", &respAuth, m) {
		return
	}
	token := respAuth.Token

	var reservasAtivas []string

	for time.Now().Before(fim) {
		origem, destino := escolherRotaValida(cidades, rng)

		inicioBusca := time.Now()
		protocolo.EnviarMensagem(conn, protocolo.Requisicao{
			Acao: protocolo.AcaoBuscarItinerarios, Origem: origem, Destino: destino, Data: data,
		})
		var respBusca protocolo.Resposta
		errBusca := protocolo.LerMensagem(scanner, &respBusca)
		sucessoBusca := errBusca == nil && respBusca.Status == protocolo.StatusOK
		m.Registrar("BUSCAR_ITINERARIOS", sucessoBusca, time.Since(inicioBusca))

		if errBusca != nil {
			atomic.AddInt64(&m.falhasConexao, 1)
			return
		}

		if sucessoBusca && len(respBusca.Itinerarios) > 0 && rng.Float64() < probReserva {
			escolhido := respBusca.Itinerarios[rng.Intn(len(respBusca.Itinerarios))]

			inicioReserva := time.Now()
			protocolo.EnviarMensagem(conn, protocolo.Requisicao{
				Acao: protocolo.AcaoReservar, Token: token, Trechos: escolhido.Trechos,
			})
			var respReserva protocolo.Resposta
			errReserva := protocolo.LerMensagem(scanner, &respReserva)
			sucessoReserva := errReserva == nil && respReserva.Status == protocolo.StatusOK
			m.Registrar("RESERVAR", sucessoReserva, time.Since(inicioReserva))

			if errReserva != nil {
				atomic.AddInt64(&m.falhasConexao, 1)
				return
			}
			if sucessoReserva {
				reservasAtivas = append(reservasAtivas, respReserva.ReservaID)
			}
		}

		if len(reservasAtivas) > 0 && rng.Float64() < probCancelamento {
			idx := rng.Intn(len(reservasAtivas))
			reservaID := reservasAtivas[idx]

			inicioCancel := time.Now()
			protocolo.EnviarMensagem(conn, protocolo.Requisicao{
				Acao: protocolo.AcaoCancelarReserva, Token: token, ReservaID: reservaID,
			})
			var respCancel protocolo.Resposta
			errCancel := protocolo.LerMensagem(scanner, &respCancel)
			sucessoCancel := errCancel == nil && respCancel.Status == protocolo.StatusOK
			m.Registrar("CANCELAR_RESERVA", sucessoCancel, time.Since(inicioCancel))

			if errCancel != nil {
				atomic.AddInt64(&m.falhasConexao, 1)
				return
			}
			if sucessoCancel {
				reservasAtivas = append(reservasAtivas[:idx], reservasAtivas[idx+1:]...)
			}
		}

		jitter := time.Duration(rng.Int63n(int64(tempoPensar) + 1))
		time.Sleep(tempoPensar/2 + jitter/2)
	}
}

// --- HELPERS DE REQUISICAO/RESPOSTA ---

// autenticar executa o fluxo AUTENTICAR e registra a metrica correspondente, preenchendo dest.
func autenticar(conn net.Conn, scanner *bufio.Scanner, login, senha string, dest *protocolo.Resposta, m *Metricas) bool {
	return requisitarComResposta(conn, scanner, protocolo.Requisicao{
		Acao: protocolo.AcaoAutenticar, Usuario: login, Senha: senha,
	}, dest, m, "AUTENTICAR")
}

// requisitarComResposta envia uma requisicao, decodifica a resposta em dest e registra a metrica.
func requisitarComResposta(conn net.Conn, scanner *bufio.Scanner, req protocolo.Requisicao, dest *protocolo.Resposta, m *Metricas, nomeMetrica string) bool {
	inicio := time.Now()
	if err := protocolo.EnviarMensagem(conn, req); err != nil {
		m.Registrar(nomeMetrica, false, time.Since(inicio))
		return false
	}
	err := protocolo.LerMensagem(scanner, dest)
	sucesso := err == nil && dest.Status == protocolo.StatusOK
	m.Registrar(nomeMetrica, sucesso, time.Since(inicio))
	return sucesso
}

// requisitarEsperandoOK envia uma requisicao e confirma status OK, registrando a metrica da acao.
func requisitarEsperandoOK(conn net.Conn, scanner *bufio.Scanner, req protocolo.Requisicao, m *Metricas, nomeMetrica string) bool {
	inicio := time.Now()
	if err := protocolo.EnviarMensagem(conn, req); err != nil {
		m.Registrar(nomeMetrica, false, time.Since(inicio))
		return false
	}
	var resp protocolo.Resposta
	err := protocolo.LerMensagem(scanner, &resp)
	sucesso := err == nil && resp.Status == protocolo.StatusOK
	m.Registrar(nomeMetrica, sucesso, time.Since(inicio))
	return sucesso
}

// --- RELATORIOS E STATUS PARCIAL ---

func imprimirStatusParcial(m *Metricas, fim time.Time, intervalo time.Duration, parar chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(intervalo)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			restante := time.Until(fim)
			if restante < 0 {
				restante = 0
			}
			total, sucesso, falha := m.Totais()
			fmt.Printf("[EM ANDAMENTO] restam %s | operacoes: %d | sucesso: %d | falhas: %d\n",
				restante.Round(time.Second), total, sucesso, falha)
		case <-parar:
			return
		}
	}
}

func imprimirRelatorioFinal(m *Metricas, duracaoReal time.Duration, enderecos []string, totalPassageiros, caronasSemeadas int) {
	fmt.Println("\n================================================================")
	fmt.Println("        RELATORIO FINAL DO SIMULADOR DE CARGA SUSTENTADA")
	fmt.Println("================================================================")
	fmt.Printf("Nos do consorcio:             %s\n", strings.Join(enderecos, ", "))
	fmt.Printf("Caronas semeadas:             %d\n", caronasSemeadas)
	fmt.Printf("Passageiros virtuais:         %d\n", totalPassageiros)
	fmt.Printf("Duracao real da carga:        %s\n", duracaoReal.Round(time.Millisecond))

	totalGeral, sucessoGeral, falhaGeral := m.Totais()
	throughput := float64(totalGeral) / duracaoReal.Seconds()

	fmt.Println("----------------------------------------------------------------")
	fmt.Printf("Total de operacoes:           %d\n", totalGeral)
	fmt.Printf("Sucessos / Falhas:            %d / %d\n", sucessoGeral, falhaGeral)
	if totalGeral > 0 {
		fmt.Printf("Taxa de sucesso global:       %.2f%%\n", float64(sucessoGeral)/float64(totalGeral)*100)
	}
	fmt.Printf("Throughput sustentado:        %.2f ops/s\n", throughput)
	fmt.Printf("Falhas de conexao TCP:        %d\n", atomic.LoadInt64(&m.falhasConexao))

	fmt.Println("----------------------------------------------------------------")
	fmt.Println("DETALHAMENTO POR OPERACAO (latencia em milissegundos):")
	fmt.Printf("%-22s %8s %8s %8s %10s %10s %10s %10s\n", "ACAO", "TENT.", "OK", "ERRO", "MEDIA", "P50", "P95", "P99")

	for _, nome := range m.NomesOrdenados() {
		s := m.snapshot(nome)
		fmt.Printf("%-22s %8d %8d %8d %10.2f %10.2f %10.2f %10.2f\n",
			nome, s.tentativas, s.sucessos, s.falhas, s.media, s.p50, s.p95, s.p99)
	}
	fmt.Println("================================================================")
}
