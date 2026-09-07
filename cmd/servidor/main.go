package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"vaijunto/pkg/estado"
	"vaijunto/pkg/protocolo"
)

var contadorClientes int64

func main() {
	porta := flag.String("porta", "8080", "Porta TCP para o servidor escutar")
	flag.Parse()

	endereco := ":" + *porta
	gerenciador := estado.NovoGerenciadorEstado()

	listener, err := net.Listen("tcp", endereco)
	if err != nil {
		log.Fatalf("[ERRO] Falha ao abrir socket TCP em %s: %v", endereco, err)
	}
	defer listener.Close()

	fmt.Println("================================================================")
	fmt.Println("VaiJunto - Servidor Central de Caronas (Feira de Santana & Salvador)")
	fmt.Printf("Status: Escutando conexoes TCP no endereco %s\n", endereco)
	fmt.Println("Concorrencia: Goroutines e sync.RWMutex ativos")
	fmt.Println("================================================================")

	canalSinal := make(chan os.Signal, 1)
	signal.Notify(canalSinal, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-canalSinal
		fmt.Println("\n[INFO] Encerrando servidor graciosamente...")
		listener.Close()
		os.Exit(0)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-canalSinal:
				return
			default:
				log.Printf("[AVISO] Falha ao aceitar conexao TCP: %v", err)
				continue
			}
		}

		idCliente := atomic.AddInt64(&contadorClientes, 1)
		go tratarCliente(conn, gerenciador, idCliente)
	}
}

func tratarCliente(conn net.Conn, gerenciador *estado.GerenciadorEstado, idCliente int64) {
	defer conn.Close()

	ipRemoto := conn.RemoteAddr().String()
	log.Printf("[CONEXAO ABERTA] Cliente #%d conectado de %s", idCliente, ipRemoto)

	scanner := protocolo.CriarScanner(conn)

	for {
		var req protocolo.Requisicao

		err := protocolo.LerMensagem(scanner, &req)
		if err != nil {
			log.Printf("[CONEXAO FECHADA] Cliente #%d desconectado (%s)", idCliente, ipRemoto)
			return
		}

		resp := processarRequisicao(req, gerenciador, idCliente)

		if err := protocolo.EnviarMensagem(conn, resp); err != nil {
			log.Printf("[ERRO] Falha ao enviar resposta para cliente #%d: %v", idCliente, err)
			return
		}
	}
}

func processarRequisicao(req protocolo.Requisicao, gerenciador *estado.GerenciadorEstado, idCliente int64) protocolo.Resposta {
	log.Printf("[REQUISICAO] Cliente #%d | Acao: '%s' | Usuario: '%s'", idCliente, req.Acao, req.Usuario)

	switch req.Acao {
	case protocolo.AcaoObterCidades:
		return protocolo.Resposta{
			Status:  protocolo.StatusOK,
			Cidades: estado.ObterCidadesRegionais(),
		}

	case protocolo.AcaoCadastrar:
		err := gerenciador.CadastrarUsuario(req.Usuario, req.Senha, req.Nome, req.Papel)
		if err != nil {
			log.Printf("[CADASTRO RECUSADO] Cliente #%d | Usuario: '%s' | Motivo: %v", idCliente, req.Usuario, err)
			return protocolo.Resposta{
				Status: protocolo.StatusErro,
				Erro:   err.Error(),
			}
		}
		log.Printf("[CADASTRO SUCESSO] Cliente #%d | Usuario: '%s' | Papel: %s", idCliente, req.Usuario, req.Papel)
		return protocolo.Resposta{
			Status:   protocolo.StatusOK,
			Mensagem: fmt.Sprintf("Usuario '%s' (%s) cadastrado com sucesso.", req.Usuario, req.Papel),
		}

	case protocolo.AcaoAutenticar:
		token, papel, nome, err := gerenciador.Autenticar(req.Usuario, req.Senha)
		if err != nil {
			log.Printf("[LOGIN RECUSADO] Cliente #%d | Usuario: '%s' | Motivo: %v", idCliente, req.Usuario, err)
			return protocolo.Resposta{
				Status: protocolo.StatusErro,
				Erro:   err.Error(),
			}
		}
		log.Printf("[LOGIN SUCESSO] Cliente #%d | Usuario: '%s' | Papel: %s", idCliente, req.Usuario, papel)
		return protocolo.Resposta{
			Status:   protocolo.StatusOK,
			Token:    token,
			Papel:    papel,
			Nome:     nome,
			Mensagem: fmt.Sprintf("Autenticacao realizada com sucesso. Bem-vindo(a), %s.", nome),
		}

	case protocolo.AcaoPublicarCarona:
		id, err := gerenciador.PublicarCarona(req.Token, req.Rota, req.Data, req.Horario, req.Assentos, req.PrecoPorTrecho)
		if err != nil {
			log.Printf("[CARONA RECUSADA] Cliente #%d | Motivo: %v", idCliente, err)
			return protocolo.Resposta{
				Status: protocolo.StatusErro,
				Erro:   err.Error(),
			}
		}
		log.Printf("[CARONA PUBLICADA] Cliente #%d | ID: %s | Rota: %v", idCliente, id, req.Rota)
		return protocolo.Resposta{
			Status:   protocolo.StatusOK,
			CaronaID: id,
			Mensagem: fmt.Sprintf("Carona '%s' publicada com sucesso!", id),
		}

	case protocolo.AcaoBuscarItinerarios:
		itinerarios, err := gerenciador.BuscarItinerarios(req.Origem, req.Destino, req.Data)
		if err != nil {
			log.Printf("[BUSCA ERRO] Cliente #%d | Motivo: %v", idCliente, err)
			return protocolo.Resposta{
				Status: protocolo.StatusErro,
				Erro:   err.Error(),
			}
		}
		log.Printf("[BUSCA SUCESSO] Cliente #%d | Origem: %s | Destino: %s | Resultados: %d", idCliente, req.Origem, req.Destino, len(itinerarios))
		return protocolo.Resposta{
			Status:      protocolo.StatusOK,
			Itinerarios: itinerarios,
		}

	case protocolo.AcaoReservar:
		id, err := gerenciador.ReservarItinerario(req.Token, req.Trechos)
		if err != nil {
			log.Printf("[RESERVA RECUSADA] Cliente #%d | Motivo: %v", idCliente, err)
			return protocolo.Resposta{
				Status: protocolo.StatusErro,
				Erro:   err.Error(),
			}
		}
		log.Printf("[RESERVA CONFIRMADA] Cliente #%d | ID: %s", idCliente, id)
		return protocolo.Resposta{
			Status:    protocolo.StatusOK,
			ReservaID: id,
			Mensagem:  fmt.Sprintf("Reserva '%s' realizada com sucesso!", id),
		}

	default:
		log.Printf("[ACAO INVALIDA] Cliente #%d | Acao nao reconhecida: '%s'", idCliente, req.Acao)
		return protocolo.Resposta{
			Status: protocolo.StatusErro,
			Erro:   fmt.Sprintf("Acao '%s' nao reconhecida.", req.Acao),
		}
	}
}