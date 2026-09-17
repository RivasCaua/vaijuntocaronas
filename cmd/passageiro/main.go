package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"vaijunto/pkg/protocolo"
)

type SessaoCliente struct {
	Token   string
	Usuario string
	Nome    string
	Papel   string
}

func main() {
	enderecoServidor := flag.String("servidor", "localhost:8080", "Endereco do servidor central (IP:Porta)")
	flag.Parse()

	fmt.Println("================================================================")
	fmt.Println("VaiJunto - Modulo Passageiro (Feira de Santana & Salvador)")
	fmt.Printf("Conectando ao servidor em: %s...\n", *enderecoServidor)
	fmt.Println("================================================================")

	conn, err := net.Dial("tcp", *enderecoServidor)
	if err != nil {
		fmt.Printf("[ERRO] Nao foi possivel conectar ao servidor em %s: %v\n", *enderecoServidor, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("[STATUS] Conexao estabelecida com sucesso.")

	scannerRede := protocolo.CriarScanner(conn)
	scannerTeclado := bufio.NewScanner(os.Stdin)

	var sessao *SessaoCliente

	for {
		if sessao == nil {
			fmt.Println("\n--- MENU PASSAGEIRO ---")
			fmt.Println("1. Cadastrar novo passageiro")
			fmt.Println("2. Entrar (Login)")
			fmt.Println("3. Sair")
			fmt.Print("Opcao: ")

			opcao := lerLinha(scannerTeclado)
			switch opcao {
			case "1":
				cadastrar(conn, scannerRede, scannerTeclado)
			case "2":
				sessao = login(conn, scannerRede, scannerTeclado)
			case "3", "sair":
				fmt.Println("[INFO] Aplicacao encerrada.")
				return
			default:
				fmt.Println("[AVISO] Opcao invalida.")
			}
		} else {
			fmt.Printf("\n--- PAINEL DO PASSAGEIRO [%s | Login: %s] ---\n", sessao.Nome, sessao.Usuario)
			fmt.Println("1. Buscar e Reservar Vaga")
			fmt.Println("2. Minhas reservas ativas")
			fmt.Println("3. Cancelar reserva")
			fmt.Println("4. Logout")
			fmt.Println("5. Encerrar aplicacao")
			fmt.Print("Opcao: ")

			opcao := lerLinha(scannerTeclado)
			switch opcao {
			case "1":
				buscarEReservar(conn, scannerRede, scannerTeclado, sessao.Token)
			case "2":
				listarMinhasReservas(conn, scannerRede, sessao.Token)
			case "3":
				cancelarReserva(conn, scannerRede, scannerTeclado, sessao.Token)
			case "4":
				fmt.Printf("[INFO] Logout efetuado para o usuario '%s'.\n", sessao.Usuario)
				sessao = nil
			case "5", "sair":
				fmt.Println("[INFO] Aplicacao encerrada.")
				return
			default:
				fmt.Println("[AVISO] Opcao invalida.")
			}
		}
	}
}

func buscarEReservar(conn net.Conn, scannerRede, scannerTeclado *bufio.Scanner, token string) {
	cidades := obterCidades(conn, scannerRede)
	if len(cidades) == 0 {
		fmt.Println("[ERRO] Nao foi possivel obter o catalogo de cidades do servidor.")
		return
	}

	fmt.Println("\n--- BUSCA DE VIAGENS ---")
	origem := escolherCidade(scannerTeclado, cidades, "Selecione a ORIGEM")
	destino := escolherCidade(scannerTeclado, cidades, "Selecione o DESTINO")

	fmt.Print("Data da viagem (AAAA-MM-DD): ")
	data := lerLinha(scannerTeclado)

	req := protocolo.Requisicao{
		Acao:    protocolo.AcaoBuscarItinerarios,
		Origem:  origem,
		Destino: destino,
		Data:    data,
	}

	if err := protocolo.EnviarMensagem(conn, req); err != nil {
		fmt.Printf("[ERRO] Falha ao enviar busca: %v\n", err)
		return
	}

	var resp protocolo.Resposta
	if err := protocolo.LerMensagem(scannerRede, &resp); err != nil {
		fmt.Printf("[ERRO] Falha ao ler resposta da busca: %v\n", err)
		return
	}

	if resp.Status != protocolo.StatusOK || len(resp.Itinerarios) == 0 {
		fmt.Println("[INFO] Nenhuma viagem disponivel para este trecho e data.")
		return
	}

	fmt.Println("\n--- ITINERARIOS ENCONTRADOS (Ordenados por Menor Preco) ---")
	for i, itin := range resp.Itinerarios {
		tagDestaque := ""
		if i == 0 {
			tagDestaque = " [MELHOR OFERTA - MAIS BARATA]"
		}

		infoHorario := ""
		if itin.HorarioPartida != "" {
			infoHorario = fmt.Sprintf(" | Partida: %s", itin.HorarioPartida)
			if itin.HorarioChegada != "" {
				infoHorario += fmt.Sprintf(" -> Chegada Prevista: %s", itin.HorarioChegada)
			}
		}

		fmt.Printf("\nOpcao [%d]%s - Preco Total: R$ %.2f%s\n", i+1, tagDestaque, itin.PrecoTotal, infoHorario)
		for _, t := range itin.Trechos {
			fmt.Printf("   -> Carona %s: %s -> %s (R$ %.2f)\n", t.CaronaID, t.Origem, t.Destino, t.Preco)
		}
	}

	fmt.Print("\nDigite o numero da opcao desejada para reservar (ou 0 para cancelar): ")
	op, _ := strconv.Atoi(lerLinha(scannerTeclado))

	if op > 0 && op <= len(resp.Itinerarios) {
		escolhido := resp.Itinerarios[op-1]

		reqReserva := protocolo.Requisicao{
			Acao:    protocolo.AcaoReservar,
			Token:   token,
			Trechos: escolhido.Trechos,
		}

		if err := protocolo.EnviarMensagem(conn, reqReserva); err != nil {
			fmt.Printf("[ERRO] Falha no envio da reserva: %v\n", err)
			return
		}

		var respReserva protocolo.Resposta
		if err := protocolo.LerMensagem(scannerRede, &respReserva); err != nil {
			fmt.Printf("[ERRO] Falha na leitura da resposta da reserva: %v\n", err)
			return
		}

		if respReserva.Status == protocolo.StatusOK {
			fmt.Printf("\n[SUCESSO] Reserva %s realizada com sucesso!\n", respReserva.ReservaID)
		} else {
			fmt.Printf("\n[FALHA ATOMICA] %s\n", respReserva.Erro)
		}
	}
}

func listarMinhasReservas(conn net.Conn, scannerRede *bufio.Scanner, token string) {
	req := protocolo.Requisicao{
		Acao:  protocolo.AcaoListarReservas,
		Token: token,
	}

	if err := protocolo.EnviarMensagem(conn, req); err != nil {
		fmt.Printf("[ERRO] Falha ao buscar reservas: %v\n", err)
		return
	}

	var resp protocolo.Resposta
	if err := protocolo.LerMensagem(scannerRede, &resp); err != nil {
		fmt.Printf("[ERRO] Falha ao ler resposta: %v\n", err)
		return
	}

	if resp.Status != protocolo.StatusOK || len(resp.Reservas) == 0 {
		fmt.Println("[INFO] Voce nao possui reservas cadastradas.")
		return
	}

	fmt.Println("\n====================================================")
	fmt.Println("             MINHAS RESERVAS DE VIAGEM")
	fmt.Println("====================================================")

	for _, r := range resp.Reservas {
		fmt.Printf("\n🎫 Reserva ID: %s | Data: %s | Status: [%s] | Total: R$ %.2f\n", r.ReservaID, r.Data, r.Status, r.PrecoTotal)
		fmt.Println("   Trechos reservados:")
		for _, t := range r.Trechos {
			fmt.Printf("      - Carona %s: %s -> %s (R$ %.2f)\n", t.CaronaID, t.Origem, t.Destino, t.Preco)
		}
	}
}

func cancelarReserva(conn net.Conn, scannerRede, scannerTeclado *bufio.Scanner, token string) {
	listarMinhasReservas(conn, scannerRede, token)

	fmt.Print("\nDigite o ID da reserva que deseja cancelar (ex: r-1) ou pressione Enter para voltar: ")
	id := lerLinha(scannerTeclado)
	if id == "" {
		return
	}

	req := protocolo.Requisicao{
		Acao:      protocolo.AcaoCancelarReserva,
		Token:     token,
		ReservaID: id,
	}

	if err := protocolo.EnviarMensagem(conn, req); err != nil {
		fmt.Printf("[ERRO] Falha ao enviar cancelamento: %v\n", err)
		return
	}

	var resp protocolo.Resposta
	if err := protocolo.LerMensagem(scannerRede, &resp); err != nil {
		fmt.Printf("[ERRO] Falha ao ler resposta: %v\n", err)
		return
	}

	if resp.Status == protocolo.StatusOK {
		fmt.Printf("\n[SUCESSO] %s\n", resp.Mensagem)
	} else {
		fmt.Printf("\n[ERRO] %s\n", resp.Erro)
	}
}

func escolherCidade(scanner *bufio.Scanner, cidades []string, titulo string) string {
	fmt.Printf("\n--- %s ---\n", titulo)
	for i, c := range cidades {
		fmt.Printf("[%d] %s\n", i+1, c)
	}
	for {
		fmt.Print("Escolha o numero ou digite o nome: ")
		entrada := lerLinha(scanner)
		if idx, err := strconv.Atoi(entrada); err == nil && idx >= 1 && idx <= len(cidades) {
			return cidades[idx-1]
		}
		for _, c := range cidades {
			if strings.EqualFold(c, entrada) {
				return c
			}
		}
		fmt.Println("[AVISO] Cidade invalida. Tente novamente.")
	}
}

func obterCidades(conn net.Conn, scannerRede *bufio.Scanner) []string {
	req := protocolo.Requisicao{Acao: protocolo.AcaoObterCidades}
	if err := protocolo.EnviarMensagem(conn, req); err != nil {
		return nil
	}
	var resp protocolo.Resposta
	if err := protocolo.LerMensagem(scannerRede, &resp); err != nil {
		return nil
	}
	return resp.Cidades
}

func cadastrar(conn net.Conn, scannerRede *bufio.Scanner, scannerTeclado *bufio.Scanner) {
	fmt.Println("\n--- CADASTRO DE PASSAGEIRO ---")
	fmt.Print("Nome completo: ")
	nome := lerLinha(scannerTeclado)
	fmt.Print("Nome de usuario (login): ")
	usuario := lerLinha(scannerTeclado)
	fmt.Print("Senha: ")
	senha := lerLinha(scannerTeclado)

	req := protocolo.Requisicao{
		Acao:    protocolo.AcaoCadastrar,
		Nome:    nome,
		Usuario: usuario,
		Senha:   senha,
		Papel:   protocolo.PapelPassageiro,
	}

	if err := protocolo.EnviarMensagem(conn, req); err != nil {
		fmt.Printf("[ERRO] Falha no envio da requisicao: %v\n", err)
		return
	}

	var resp protocolo.Resposta
	if err := protocolo.LerMensagem(scannerRede, &resp); err != nil {
		fmt.Printf("[ERRO] Falha ao ler resposta do servidor: %v\n", err)
		return
	}

	if resp.Status == protocolo.StatusOK {
		fmt.Printf("[SUCESSO] %s\n", resp.Mensagem)
	} else {
		fmt.Printf("[ERRO] %s\n", resp.Erro)
	}
}

func login(conn net.Conn, scannerRede *bufio.Scanner, scannerTeclado *bufio.Scanner) *SessaoCliente {
	fmt.Println("\n--- LOGIN DE PASSAGEIRO ---")
	fmt.Print("Usuario: ")
	usuario := lerLinha(scannerTeclado)
	fmt.Print("Senha: ")
	senha := lerLinha(scannerTeclado)

	req := protocolo.Requisicao{
		Acao:    protocolo.AcaoAutenticar,
		Usuario: usuario,
		Senha:   senha,
	}

	if err := protocolo.EnviarMensagem(conn, req); err != nil {
		fmt.Printf("[ERRO] Falha no envio das credenciais: %v\n", err)
		return nil
	}

	var resp protocolo.Resposta
	if err := protocolo.LerMensagem(scannerRede, &resp); err != nil {
		fmt.Printf("[ERRO] Falha ao ler resposta do servidor: %v\n", err)
		return nil
	}

	if resp.Status == protocolo.StatusOK {
		if resp.Papel != protocolo.PapelPassageiro {
			fmt.Printf("[AVISO] Atencao: Conta cadastrada como '%s', em modulo de passageiro.\n", resp.Papel)
		}
		fmt.Printf("[SUCESSO] %s\n", resp.Mensagem)
		return &SessaoCliente{
			Token:   resp.Token,
			Usuario: usuario,
			Nome:    resp.Nome,
			Papel:   resp.Papel,
		}
	}

	fmt.Printf("[ERRO] %s\n", resp.Erro)
	return nil
}

func lerLinha(scanner *bufio.Scanner) string {
	if scanner.Scan() {
		return strings.TrimSpace(scanner.Text())
	}
	return ""
}