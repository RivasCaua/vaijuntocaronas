package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
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
	fmt.Println("VaiJunto - Modulo Motorista")
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
			fmt.Println("1. Publicar carona (Fase 2)")
			fmt.Println("2. Minhas caronas (Fase 2)")
			fmt.Println("3. Cancelar carona (Fase 2)")
			fmt.Println("4. Logout")
			fmt.Println("5. Encerrar aplicacao")
			fmt.Print("Opcao: ")

			opcao := lerLinha(scannerTeclado)
			switch opcao {
			case "1", "2", "3":
				fmt.Println("[INFO] Funcionalidade prevista para a Fase 2.")
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