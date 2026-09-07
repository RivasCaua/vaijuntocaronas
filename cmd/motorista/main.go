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
	fmt.Println("VaiJunto - Modulo Motorista (Feira de Santana & Salvador)")
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
			fmt.Println("\n--- MENU MOTORISTA ---")
			fmt.Println("1. Cadastrar novo motorista")
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
			fmt.Printf("\n--- PAINEL DO MOTORISTA [%s | Login: %s] ---\n", sessao.Nome, sessao.Usuario)
			fmt.Println("1. Publicar carona (Estilo Uber: Inicio -> Fim -> Paradas)")
			fmt.Println("2. Logout")
			fmt.Println("3. Encerrar aplicacao")
			fmt.Print("Opcao: ")

			opcao := lerLinha(scannerTeclado)
			switch opcao {
			case "1":
				publicarCarona(conn, scannerRede, scannerTeclado, sessao.Token)
			case "2":
				fmt.Printf("[INFO] Logout efetuado para o usuario '%s'.\n", sessao.Usuario)
				sessao = nil
			case "3", "sair":
				fmt.Println("[INFO] Aplicacao encerrada.")
				return
			default:
				fmt.Println("[AVISO] Opcao invalida.")
			}
		}
	}
}

func publicarCarona(conn net.Conn, scannerRede, scannerTeclado *bufio.Scanner, token string) {
	cidades := obterCidades(conn, scannerRede)
	if len(cidades) == 0 {
		fmt.Println("[ERRO] Nao foi possivel obter o catalogo de cidades do servidor.")
		return
	}

	fmt.Println("\n====================================================")
	fmt.Println("   PUBLICAR CARONA (Estilo Uber / Passo a Passo)")
	fmt.Println("====================================================")

	// Passo 1: Origem
	origem := escolherCidade(scannerTeclado, cidades, "Ponto de INICIO (Origem)")

	// Passo 2: Destino Final
	destino := escolherCidade(scannerTeclado, cidades, "Ponto FIM (Destino Final)")
	for destino == origem {
		fmt.Println("[AVISO] O destino nao pode ser igual a origem!")
		destino = escolherCidade(scannerTeclado, cidades, "Ponto FIM (Destino Final)")
	}

	rota := []string{origem}

	// Passo 3: Adicionar Paradas Intermediárias
	fmt.Printf("\nDeseja adicionar paradas intermediarias entre %s e %s? (s/n): ", origem, destino)
	if resp := lerLinha(scannerTeclado); strings.ToLower(resp) == "s" || strings.ToLower(resp) == "sim" {
		fmt.Print("Quantas paradas intermediarias deseja adicionar? ")
		qtd, _ := strconv.Atoi(lerLinha(scannerTeclado))

		for i := 1; i <= qtd; i++ {
			prompt := fmt.Sprintf("Escolha a Parada Intermediaria #%d", i)
			parada := escolherCidade(scannerTeclado, cidades, prompt)
			rota = append(rota, parada)
		}
	}

	rota = append(rota, destino)

	fmt.Printf("\n--> Rota configurada: %v\n", rota)

	fmt.Print("Data da viagem (AAAA-MM-DD): ")
	data := lerLinha(scannerTeclado)

	fmt.Print("Horario de partida (HH:MM): ")
	horario := lerLinha(scannerTeclado)

	fmt.Print("Quantidade de assentos livres: ")
	assentos, _ := strconv.Atoi(lerLinha(scannerTeclado))

	fmt.Print("Preco por trecho (R$): ")
	preco, _ := strconv.ParseFloat(lerLinha(scannerTeclado), 64)

	req := protocolo.Requisicao{
		Acao:           protocolo.AcaoPublicarCarona,
		Token:          token,
		Rota:           rota,
		Data:           data,
		Horario:        horario,
		Assentos:       assentos,
		PrecoPorTrecho: preco,
	}

	if err := protocolo.EnviarMensagem(conn, req); err != nil {
		fmt.Printf("[ERRO] Falha ao enviar requisicao: %v\n", err)
		return
	}

	var resp protocolo.Resposta
	if err := protocolo.LerMensagem(scannerRede, &resp); err != nil {
		fmt.Printf("[ERRO] Falha ao ler resposta do servidor: %v\n", err)
		return
	}

	if resp.Status == protocolo.StatusOK {
		fmt.Printf("\n[SUCESSO] %s (ID: %s)\n", resp.Mensagem, resp.CaronaID)
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
	fmt.Println("\n--- CADASTRO DE MOTORISTA ---")
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
		Papel:   protocolo.PapelMotorista,
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
	fmt.Println("\n--- LOGIN DE MOTORISTA ---")
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
		if resp.Papel != protocolo.PapelMotorista {
			fmt.Printf("[AVISO] Atencao: Conta cadastrada como '%s', em modulo de motorista.\n", resp.Papel)
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