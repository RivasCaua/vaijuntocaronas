package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"vaijunto/pkg/consorcio"
	"vaijunto/pkg/estado"
	"vaijunto/pkg/protocolo"
)

var contadorClientes int64

// ServidorContexto agrupa as dependencias compartilhadas por todas as goroutines de atendimento
// TCP: o estado de dominio em memoria, o Write-Ahead Log durável, o relogio logico de Lamport
// (usado para ordenar eventos entre as empresas do consorcio) e a identidade desta instancia.
type ServidorContexto struct {
	Gerenciador *estado.GerenciadorEstado
	WAL         *consorcio.WAL
	Relogio     *consorcio.RelogioLamport
	NodoLocal   consorcio.NodoDTO
	Coordenador *consorcio.CoordenadorDistribuido
}

func main() {
	empresaID := flag.String("empresa-id", "empresa-local", "Identificador unico desta empresa/instancia no consorcio VaiJunto")
	regiao := flag.String("regiao", "", "Regiao geografica atendida por esta instancia (ex: RMS, Portal do Sertao)")
	porta := flag.String("porta", "8080", "Porta TCP para atendimento de motoristas e passageiros")
	portaREST := flag.Int("porta-rest", 9090, "Porta HTTP/REST para comunicacao inter-servidores do consorcio")
	endereco := flag.String("endereco", "", "Hostname/IP pelo qual esta instancia e alcancada pelas empresas parceiras")
	caminhoWAL := flag.String("wal", "", "Caminho do arquivo de Write-Ahead Log (padrao: dados/<empresa-id>.wal)")
	membrosFlag := flag.String("membros", "", "Empresas parceiras do consorcio no formato id@host:porta_rest separadas por virgula")
	heartbeatIntervalo := flag.Duration("heartbeat-intervalo", 2*time.Second, "Intervalo entre heartbeats automaticos enviados aos parceiros do consorcio")
	flag.Parse()

	if strings.TrimSpace(*caminhoWAL) == "" {
		*caminhoWAL = fmt.Sprintf("dados/%s.wal", *empresaID)
	}

	fmt.Println("================================================================")
	fmt.Printf("VaiJunto - Servidor Multi-Instancia [%s]\n", *empresaID)
	fmt.Println("================================================================")

	// --- 1. ABERTURA DO WAL E EXECUCAO DO PROTOCOLO DE RECUPERACAO POS-QUEDA ---
	// O WAL e aberto (ou criado) antes de qualquer outra coisa: e a unica fonte de verdade
	// durante o boot. Nenhuma porta e aberta para clientes ate o estado estar reconstruido.
	wal, err := consorcio.NovoWAL(*caminhoWAL)
	if err != nil {
		log.Fatalf("[ERRO] Falha ao abrir Write-Ahead Log em %s: %v", *caminhoWAL, err)
	}

	gerenciador := estado.NovoGerenciadorEstado()
	maiorLamport, err := recuperarEstadoViaWAL(wal, gerenciador)
	if err != nil {
		log.Fatalf("[ERRO] Falha critica na recuperacao via WAL (log possivelmente corrompido): %v", err)
	}

	// O relogio logico retoma a partir do maior timestamp ja observado no log local, garantindo
	// que esta instancia nunca "retroceda" no tempo logico do consorcio apos reiniciar.
	relogio := consorcio.NovoRelogioLamport(*empresaID, maiorLamport)

	// --- 2. TOPOLOGIA DO CONSORCIO (MULTI-EMPRESA) ---
	membros, err := interpretarMembros(*membrosFlag)
	if err != nil {
		log.Fatalf("[ERRO] Flag -membros invalida: %v", err)
	}

	portaTCPInt, err := strconv.Atoi(*porta)
	if err != nil {
		log.Fatalf("[ERRO] Porta TCP invalida '%s': %v", *porta, err)
	}

	nodoLocal := consorcio.NodoDTO{
		ID:          *empresaID,
		Endereco:    *endereco,
		PortaTCP:    portaTCPInt,
		PortaREST:   *portaREST,
		Regiao:      *regiao,
		Cidades:     estado.ObterCidadesRegionais(),
		Status:      consorcio.StatusNodoOnline,
		UltimoVisto: time.Now(),
	}

	// --- 3. COORDENADOR DISTRIBUIDO (SCATTER-GATHER, 2PC E HEARTBEAT AUTOMATICO) ---
	// Criado ANTES do listener REST porque e a fonte unica de verdade sobre o status dos membros
	// do consorcio: o ServidorREST abaixo passa a ler/escrever no MESMO mapa de membros do
	// coordenador (em vez de manter uma copia paralela), tanto para /cluster/info quanto para
	// heartbeats recebidos. O mesmo coordenador atende buscas federadas e reservas: para trechos
	// inteiramente locais, ExecutarReserva2PC resolve tudo em memoria (sem round-trip de rede),
	// preservando a mesma semantica Check-Then-Act de antes; para trechos de parceiros, aciona
	// o protocolo completo de Two-Phase Commit com compensacao automatica em caso de falha.
	coordenador := consorcio.NovoCoordenadorDistribuido(consorcio.ConfigCoordenador{
		NodoLocal:   nodoLocal,
		Membros:     membros,
		Gerenciador: gerenciador,
		Relogio:     relogio,
		WAL:         wal,
	})

	if len(membros) > 0 {
		coordenador.IniciarRotinaHeartbeat(*heartbeatIntervalo, 1, 3)
	}

	// Limpeza automatica de holds (leases do 2PC) orfaos: sem isso, um assento provisoriamente
	// retido na Fase 1 (Prepare) so e devolvido ao inventario na proxima vez que ALGUEM mais tentar
	// um hold/commit no mesmo trecho - em um no de baixo trafego, um cliente que cai entre Prepare e
	// Commit/Abort pode deixar o assento bloqueado por tempo indefinido sem essa rotina.
	gerenciador.IniciarRotinaExpiracao(500 * time.Millisecond)

	// --- 4. LISTENER HTTP/REST (COMUNICACAO INTER-SERVIDORES DO CONSORCIO) ---
	// Iniciar() abre o socket e despacha o atendimento HTTP em uma goroutine propria,
	// retornando imediatamente: a partir daqui o listener REST ja esta ativo e concorrente
	// com o listener TCP de clientes que sera aberto na sequencia.
	servidorREST := consorcio.NovoServidorREST(consorcio.ConfigServidorREST{
		NodoLocal:   nodoLocal,
		Membros:     membros,
		Gerenciador: gerenciador,
		Relogio:     relogio,
		WAL:         wal,
		Coordenador: coordenador,
	})
	if err := servidorREST.Iniciar(*portaREST); err != nil {
		log.Fatalf("[ERRO] Falha ao iniciar listener REST na porta %d: %v", *portaREST, err)
	}

	ctx := &ServidorContexto{
		Gerenciador: gerenciador,
		WAL:         wal,
		Relogio:     relogio,
		NodoLocal:   nodoLocal,
		Coordenador: coordenador,
	}

	// --- 5. LISTENER TCP (ATENDIMENTO DE MOTORISTAS E PASSAGEIROS) ---
	enderecoTCP := ":" + *porta
	listener, err := net.Listen("tcp", enderecoTCP)
	if err != nil {
		log.Fatalf("[ERRO] Falha ao abrir socket TCP em %s: %v", enderecoTCP, err)
	}
	defer listener.Close()

	fmt.Printf("Empresa:          %s (regiao: %s)\n", *empresaID, valorOuPadrao(*regiao, "-"))
	fmt.Printf("TCP (clientes):   %s\n", enderecoTCP)
	fmt.Printf("REST (consorcio): porta %d\n", servidorREST.ObterPorta())
	fmt.Printf("Parceiras:        %d configuradas\n", len(membros))
	fmt.Printf("WAL:              %s (ultimo LSN: %d | relogio Lamport: %d)\n", *caminhoWAL, wal.UltimoIndice(), maiorLamport)
	if len(membros) > 0 {
		fmt.Printf("Heartbeat:        automatico a cada %s (SUSPEITO apos 1 falha, OFFLINE apos 3)\n", heartbeatIntervalo.String())
	}
	fmt.Println("Concorrencia:     Goroutines e sync.RWMutex ativos")
	fmt.Println("================================================================")

	canalSinal := make(chan os.Signal, 1)
	signal.Notify(canalSinal, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-canalSinal
		fmt.Println("\n[INFO] Encerrando servidor graciosamente...")
		listener.Close()
		coordenador.PararRotinaHeartbeat()
		gerenciador.PararRotinaExpiracao()

		ctxParar, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := servidorREST.Parar(ctxParar); err != nil {
			log.Printf("[AVISO] Falha ao encerrar listener REST: %v", err)
		}
		if err := wal.Fechar(); err != nil {
			log.Printf("[AVISO] Falha ao fechar WAL: %v", err)
		}
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
		go tratarCliente(conn, ctx, idCliente)
	}
}

// valorOuPadrao retorna o padrao quando o valor informado estiver vazio (apenas para exibicao).
func valorOuPadrao(valor, padrao string) string {
	if strings.TrimSpace(valor) == "" {
		return padrao
	}
	return valor
}

// interpretarMembros converte a flag "-membros" (formato "id@host:porta,id2@host2:porta2")
// na lista de parceiros do consorcio usada pelo ServidorREST e pelo CoordenadorDistribuido.
func interpretarMembros(flagValor string) ([]consorcio.NodoDTO, error) {
	flagValor = strings.TrimSpace(flagValor)
	if flagValor == "" {
		return nil, nil
	}

	var membros []consorcio.NodoDTO
	for _, parte := range strings.Split(flagValor, ",") {
		parte = strings.TrimSpace(parte)
		if parte == "" {
			continue
		}

		campos := strings.SplitN(parte, "@", 2)
		if len(campos) != 2 {
			return nil, fmt.Errorf("formato invalido '%s' (esperado id@host:porta)", parte)
		}

		id := strings.TrimSpace(campos[0])
		host, portaStr, err := net.SplitHostPort(strings.TrimSpace(campos[1]))
		if err != nil {
			return nil, fmt.Errorf("endereco invalido para o membro '%s': %w", id, err)
		}

		portaMembro, err := strconv.Atoi(portaStr)
		if err != nil {
			return nil, fmt.Errorf("porta invalida para o membro '%s': %w", id, err)
		}

		membros = append(membros, consorcio.NodoDTO{
			ID:        id,
			Endereco:  host,
			PortaREST: portaMembro,
			Status:    consorcio.StatusNodoOnline,
		})
	}

	return membros, nil
}

// recuperarEstadoViaWAL implementa o protocolo de recuperacao pos-queda: le o WAL do inicio ao
// fim (a integridade de cada registro ja foi validada por checksum CRC32 dentro de wal.Recuperar),
// e reaplica em ordem estrita de LSN (Log Sequence Number) cada operacao sobre um GerenciadorEstado
// recem-criado, reconstruindo o estado exatamente como estava no instante da queda.
// Retorna o maior timestamp de Lamport observado em todo o log, usado para reiniciar o relogio
// logico local sem jamais retroceder em relacao a eventos ja emitidos antes do crash.
func recuperarEstadoViaWAL(wal *consorcio.WAL, gerenciador *estado.GerenciadorEstado) (uint64, error) {
	registros, err := wal.Recuperar()
	if err != nil {
		return 0, fmt.Errorf("integridade do WAL comprometida: %w", err)
	}

	if len(registros) == 0 {
		fmt.Println("[RECUPERACAO] WAL vazio: primeira execucao desta instancia (estado inicial zerado).")
		return 0, nil
	}

	inicio := time.Now()
	var maiorLamport uint64
	contagemPorOperacao := make(map[string]int)
	ignorados := 0

	for _, registro := range registros {
		if registro.Timestamp.Tempo > maiorLamport {
			maiorLamport = registro.Timestamp.Tempo
		}

		if err := aplicarRegistroWAL(registro, gerenciador); err != nil {
			log.Printf("[RECUPERACAO][AVISO] LSN %d (%s) ignorado: %v", registro.Indice, registro.Operacao, err)
			ignorados++
			continue
		}
		contagemPorOperacao[registro.Operacao]++
	}

	fmt.Printf("[RECUPERACAO] %d registros lidos do WAL, reaplicados em %s (ultimo LSN=%d)\n",
		len(registros), time.Since(inicio), wal.UltimoIndice())
	for operacao, total := range contagemPorOperacao {
		fmt.Printf("   - %-28s %d\n", operacao, total)
	}
	if ignorados > 0 {
		fmt.Printf("   - %-28s %d\n", "(ignorados/transitorios)", ignorados)
	}
	fmt.Printf("[RECUPERACAO] Estado reconstruido: %d usuarios | %d caronas | %d reservas\n",
		gerenciador.ObterTotalUsuarios(), gerenciador.ObterTotalCaronas(), gerenciador.ObterTotalReservas())

	return maiorLamport, nil
}

// payloadReservaConfirmada espelha o formato gravado pelo ServidorREST ao confirmar a Fase 2
// (Commit) de uma transacao 2PC (vaijunto/pkg/consorcio.ServidorREST.handleCommit2PC grava o
// proprio objeto estado.Reserva no WAL, cujas tags JSON diferem de PayloadReservarDTO).
type payloadReservaConfirmada struct {
	ID              string                 `json:"id"`
	PassageiroLogin string                 `json:"passageiro_login"`
	Trechos         []protocolo.ItemTrecho `json:"trechos"`
	PrecoTotal      float64                `json:"preco_total"`
	Data            string                 `json:"data"`
}

// aplicarRegistroWAL interpreta uma unica entrada do WAL e a projeta sobre o GerenciadorEstado.
//
// Operacoes de dominio ja confirmadas (CADASTRAR_USUARIO, PUBLICAR_CARONA, RESERVAR,
// CANCELAR_RESERVA, CANCELAR_CARONA e 2PC_COMMIT) representam fatos definitivos e sao reaplicadas
// incondicionalmente, na ordem exata em que ocorreram.
//
// Entradas de fases transitorias do protocolo Two-Phase Commit (2PC_PREPARE, 2PC_ABORT e os
// marcadores de controle do coordenador) sao deliberadamente ignoradas na recuperacao: um
// PREPARE sem o correspondente COMMIT indica uma transacao que nunca chegou a uma decisao
// duravel antes da queda. Como os Holds possuem TTL curto e o servidor pode ter ficado
// indisponivel por um periodo arbitrario, a politica adotada e a de aborto implicito - os
// assentos permanecem livres apos a recuperacao, em vez de arriscar reconstituir uma reserva
// fantasma que o cliente nunca viu ser confirmada.
func aplicarRegistroWAL(registro *consorcio.RegistroWAL, gerenciador *estado.GerenciadorEstado) error {
	switch registro.Operacao {

	case consorcio.OpCadastrarUsuario:
		var p consorcio.PayloadCadastroUsuarioDTO
		if err := json.Unmarshal(registro.Payload, &p); err != nil {
			return fmt.Errorf("payload CADASTRAR_USUARIO invalido: %w", err)
		}
		return gerenciador.AplicarCadastroUsuario(p.Login, p.Senha, p.Nome, p.Papel)

	case consorcio.OpPublicarCarona:
		var p consorcio.PayloadPublicarCaronaDTO
		if err := json.Unmarshal(registro.Payload, &p); err != nil {
			return fmt.Errorf("payload PUBLICAR_CARONA invalido: %w", err)
		}
		return gerenciador.AplicarPublicacaoCarona(p.CaronaID, p.EmpresaID, p.MotoristaLogin, p.Rota, p.Data, p.Horario, p.HorarioChegada, p.Assentos, p.PrecoPorTrecho)

	case consorcio.OpReservar:
		var p consorcio.PayloadReservarDTO
		if err := json.Unmarshal(registro.Payload, &p); err != nil {
			return fmt.Errorf("payload RESERVAR invalido: %w", err)
		}
		return gerenciador.AplicarReserva(p.ReservaID, p.PassageiroLogin, trechosParaEstado(p.Trechos), p.PrecoTotal, p.Data)

	case "2PC_COMMIT":
		var p payloadReservaConfirmada
		if err := json.Unmarshal(registro.Payload, &p); err != nil {
			return fmt.Errorf("payload 2PC_COMMIT invalido: %w", err)
		}
		return gerenciador.AplicarReserva(p.ID, p.PassageiroLogin, trechosParaEstado(p.Trechos), p.PrecoTotal, p.Data)

	case consorcio.OpCancelarReserva:
		var p consorcio.PayloadCancelarReservaDTO
		if err := json.Unmarshal(registro.Payload, &p); err != nil {
			return fmt.Errorf("payload CANCELAR_RESERVA invalido: %w", err)
		}
		return gerenciador.AplicarCancelamentoReserva(p.ReservaID, p.PassageiroLogin)

	case consorcio.OpCancelarCarona:
		var p consorcio.PayloadCancelarCaronaDTO
		if err := json.Unmarshal(registro.Payload, &p); err != nil {
			return fmt.Errorf("payload CANCELAR_CARONA invalido: %w", err)
		}
		return gerenciador.AplicarCancelamentoCarona(p.CaronaID, p.MotoristaLogin)

	case "2PC_PREPARE", "2PC_ABORT", "2PC_COORDENADOR_INICIO", "2PC_COORDENADOR_ABORT", "2PC_COORDENADOR_COMMIT":
		return nil // fase transitoria do 2PC: nao representa, por si so, um fato final e duravel

	default:
		return fmt.Errorf("operacao desconhecida no WAL: '%s'", registro.Operacao)
	}
}

// trechosParaEstado converte o DTO de rede (pkg/protocolo) para o formato interno de persistencia
// usado pelo GerenciadorEstado (pkg/estado), tanto no caminho ao vivo quanto na recuperacao.
func trechosParaEstado(itens []protocolo.ItemTrecho) []estado.ItemTrechoReserva {
	convertidos := make([]estado.ItemTrechoReserva, 0, len(itens))
	for _, item := range itens {
		convertidos = append(convertidos, estado.ItemTrechoReserva{
			CaronaID:  item.CaronaID,
			EmpresaID: item.EmpresaID,
			Origem:    item.Origem,
			Destino:   item.Destino,
			Preco:     item.Preco,
		})
	}
	return convertidos
}

// registrarOperacao avanca o relogio de Lamport local e grava, de forma durável (com fsync), o
// fato de dominio no WAL. So deve ser chamada DEPOIS que a mutacao correspondente ja foi aplicada
// com sucesso ao GerenciadorEstado em memoria, e a resposta de sucesso so deve ser enviada ao
// cliente SE esta chamada tambem for bem sucedida - e esse encadeamento que garante que nenhuma
// operacao seja confirmada ao cliente sem antes estar segura contra quedas do processo.
func registrarOperacao(ctx *ServidorContexto, operacao string, payload any) error {
	dados, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("falha ao serializar payload do WAL: %w", err)
	}

	timestamp := ctx.Relogio.Tick()
	if _, err := ctx.WAL.Gravar(operacao, dados, timestamp); err != nil {
		return fmt.Errorf("falha ao persistir no WAL: %w", err)
	}
	return nil
}

func tratarCliente(conn net.Conn, ctx *ServidorContexto, idCliente int64) {
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

		resp := processarRequisicao(req, ctx, idCliente)

		if err := protocolo.EnviarMensagem(conn, resp); err != nil {
			log.Printf("[ERRO] Falha ao enviar resposta para cliente #%d: %v", idCliente, err)
			return
		}
	}
}

func processarRequisicao(req protocolo.Requisicao, ctx *ServidorContexto, idCliente int64) protocolo.Resposta {
	gerenciador := ctx.Gerenciador

	req.Acao = strings.ToUpper(strings.TrimSpace(req.Acao))
	log.Printf("[REQUISICAO] Cliente #%d | Acao: '%s' | Usuario: '%s'", idCliente, req.Acao, req.Usuario)

	switch req.Acao {
	case protocolo.AcaoObterCidades:
		return protocolo.Resposta{
			Status:  protocolo.StatusOK,
			Cidades: estado.ObterCidadesRegionais(),
		}

	case protocolo.AcaoCadastrar:
		loginNorm := strings.TrimSpace(req.Usuario)
		senhaNorm := strings.TrimSpace(req.Senha)
		nomeNorm := strings.TrimSpace(req.Nome)
		papelNorm := strings.ToLower(strings.TrimSpace(req.Papel))

		// A persistencia roda DENTRO da mesma trava da mutacao (ver gerenciador.CadastrarUsuarioComPersistencia):
		// se o WAL falhar, o cadastro e desfeito atomicamente antes de qualquer outro cliente poder ve-lo.
		err := gerenciador.CadastrarUsuarioComPersistencia(req.Usuario, req.Senha, req.Nome, req.Papel, func() error {
			return registrarOperacao(ctx, consorcio.OpCadastrarUsuario, consorcio.PayloadCadastroUsuarioDTO{
				Login: loginNorm, Senha: senhaNorm, Nome: nomeNorm, Papel: papelNorm,
			})
		})
		if err != nil {
			log.Printf("[CADASTRO RECUSADO] Cliente #%d | Usuario: '%s' | Motivo: %v", idCliente, req.Usuario, err)
			return protocolo.Resposta{Status: protocolo.StatusErro, Erro: err.Error()}
		}

		log.Printf("[CADASTRO SUCESSO] Cliente #%d | Usuario: '%s' | Papel: %s", idCliente, req.Usuario, req.Papel)
		return protocolo.Resposta{
			Status:   protocolo.StatusOK,
			Mensagem: fmt.Sprintf("Usuario '%s' (%s) cadastrado com sucesso.", req.Usuario, req.Papel),
		}

	case protocolo.AcaoAutenticar:
		// Autenticacao nao muta o estado de dominio durável (apenas cria uma sessao em memoria),
		// portanto nao gera registro no WAL.
		token, papel, nome, err := gerenciador.Autenticar(req.Usuario, req.Senha)
		if err != nil {
			log.Printf("[LOGIN RECUSADO] Cliente #%d | Usuario: '%s' | Motivo: %v", idCliente, req.Usuario, err)
			return protocolo.Resposta{Status: protocolo.StatusErro, Erro: err.Error()}
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
		usuario, err := gerenciador.ValidarToken(req.Token)
		if err != nil {
			return protocolo.Resposta{Status: protocolo.StatusErro, Erro: err.Error()}
		}

		// A carona publicada pertence sempre a empresa que opera este servidor: o cliente nao
		// escolhe a empresa, ela e definida pela instancia (-empresa-id) a qual ele se conectou.
		rotaNormalizada, _ := estado.ValidarRota(req.Rota) // revalidada aqui so para obter a forma normalizada usada no registro do WAL

		// A persistencia roda DENTRO da mesma trava da mutacao: se o WAL falhar, a carona recem-criada
		// e removida atomicamente antes de qualquer outro cliente poder busca-la ou reserva-la.
		caronaID, err := gerenciador.PublicarCaronaComPersistencia(req.Token, req.Rota, req.Data, req.Horario, req.HorarioChegada, req.Assentos, req.PrecoPorTrecho, func(caronaID string) error {
			return registrarOperacao(ctx, consorcio.OpPublicarCarona, consorcio.PayloadPublicarCaronaDTO{
				CaronaID:       caronaID,
				EmpresaID:      ctx.NodoLocal.ID,
				MotoristaLogin: usuario.Login,
				Rota:           rotaNormalizada,
				Data:           strings.TrimSpace(req.Data),
				Horario:        strings.TrimSpace(req.Horario),
				HorarioChegada: strings.TrimSpace(req.HorarioChegada),
				Assentos:       req.Assentos,
				PrecoPorTrecho: req.PrecoPorTrecho,
			})
		}, ctx.NodoLocal.ID)
		if err != nil {
			log.Printf("[CARONA RECUSADA] Cliente #%d | Motivo: %v", idCliente, err)
			return protocolo.Resposta{Status: protocolo.StatusErro, Erro: err.Error()}
		}

		log.Printf("[CARONA PUBLICADA] Cliente #%d | ID: %s | Rota: %v", idCliente, caronaID, req.Rota)
		return protocolo.Resposta{
			Status:   protocolo.StatusOK,
			CaronaID: caronaID,
			Mensagem: fmt.Sprintf("Carona '%s' publicada com sucesso!", caronaID),
		}

	case protocolo.AcaoBuscarItinerarios:
		// Busca combinada entre empresas: monta um grafo com o inventário bruto de sub-trechos
		// deste nó E de todos os parceiros do consórcio, encadeando caronas de empresas
		// diferentes sempre que compartilharem uma cidade (ex.: Salvador->Feira nesta empresa,
		// Feira->Vitória da Conquista em outra, Vitória da Conquista->Montes Claros em uma
		// terceira). A reserva de um itinerário resultante, abaixo, já aciona o 2PC completo com
		// compensação automática quando os trechos pertencem a mais de uma empresa.
		ctxBusca, cancelBusca := context.WithTimeout(context.Background(), 3*time.Second)
		itinerarios, err := ctx.Coordenador.BuscarItinerariosCombinados(ctxBusca, req.Origem, req.Destino, req.Data, 3*time.Second)
		cancelBusca()
		if err != nil {
			log.Printf("[BUSCA ERRO] Cliente #%d | Motivo: %v", idCliente, err)
			return protocolo.Resposta{Status: protocolo.StatusErro, Erro: err.Error()}
		}
		log.Printf("[BUSCA SUCESSO] Cliente #%d | Origem: %s | Destino: %s | Resultados: %d", idCliente, req.Origem, req.Destino, len(itinerarios))
		return protocolo.Resposta{
			Status:      protocolo.StatusOK,
			Itinerarios: itinerarios,
		}

	case protocolo.AcaoReservar:
		usuario, err := gerenciador.ValidarToken(req.Token)
		if err != nil {
			return protocolo.Resposta{Status: protocolo.StatusErro, Erro: err.Error()}
		}

		// Toda reserva, local ou entre empresas, passa pelo mesmo Two-Phase Commit: para
		// trechos inteiramente locais o coordenador resolve tudo em memoria (Fase 1 e Fase 2
		// acontecem sem round-trip de rede, mesma semantica Check-Then-Act de sempre); para
		// trechos de um parceiro, aciona o protocolo completo com compensacao automatica caso
		// algum participante recuse ou esteja fora do ar. A durabilidade no WAL (local e/ou
		// remota) ja acontece dentro do proprio coordenador/handler REST antes de retornar.
		ctxReserva, cancelReserva := context.WithTimeout(context.Background(), 8*time.Second)
		reserva, err := ctx.Coordenador.ExecutarReserva2PC(ctxReserva, usuario.Login, req.Trechos, 15*time.Second)
		cancelReserva()
		if err != nil {
			log.Printf("[RESERVA RECUSADA] Cliente #%d | Motivo: %v", idCliente, err)
			return protocolo.Resposta{Status: protocolo.StatusErro, Erro: err.Error()}
		}

		log.Printf("[RESERVA CONFIRMADA] Cliente #%d | ID: %s", idCliente, reserva.ReservaID)
		return protocolo.Resposta{
			Status:    protocolo.StatusOK,
			ReservaID: reserva.ReservaID,
			Mensagem:  fmt.Sprintf("Reserva '%s' realizada com sucesso!", reserva.ReservaID),
		}

	case protocolo.AcaoListarCaronas:
		caronas, err := gerenciador.ListarCaronasMotorista(req.Token)
		if err != nil {
			return protocolo.Resposta{Status: protocolo.StatusErro, Erro: err.Error()}
		}
		return protocolo.Resposta{Status: protocolo.StatusOK, Caronas: caronas}

	case protocolo.AcaoCancelarCarona:
		usuario, err := gerenciador.ValidarToken(req.Token)
		if err != nil {
			return protocolo.Resposta{Status: protocolo.StatusErro, Erro: err.Error()}
		}

		// A persistencia roda DENTRO da mesma trava da mutacao: se o WAL falhar, o cancelamento (e
		// as reservas que ele afetou) e revertido atomicamente antes de qualquer outro cliente ver.
		err = gerenciador.CancelarCaronaComPersistencia(req.Token, req.CaronaID, func() error {
			return registrarOperacao(ctx, consorcio.OpCancelarCarona, consorcio.PayloadCancelarCaronaDTO{
				CaronaID: req.CaronaID, MotoristaLogin: usuario.Login,
			})
		})
		if err != nil {
			log.Printf("[WAL][ERRO] Cliente #%d | CANCELAR_CARONA nao persistido: %v", idCliente, err)
			return protocolo.Resposta{Status: protocolo.StatusErro, Erro: err.Error()}
		}

		log.Printf("[CARONA CANCELADA] Cliente #%d | ID: %s", idCliente, req.CaronaID)
		return protocolo.Resposta{Status: protocolo.StatusOK, Mensagem: fmt.Sprintf("Carona '%s' cancelada com sucesso.", req.CaronaID)}

	case protocolo.AcaoListarReservas:
		reservas, err := gerenciador.ListarReservasPassageiro(req.Token)
		if err != nil {
			return protocolo.Resposta{Status: protocolo.StatusErro, Erro: err.Error()}
		}
		return protocolo.Resposta{Status: protocolo.StatusOK, Reservas: reservas}

	case protocolo.AcaoCancelarReserva:
		usuario, err := gerenciador.ValidarToken(req.Token)
		if err != nil {
			return protocolo.Resposta{Status: protocolo.StatusErro, Erro: err.Error()}
		}

		// A persistencia roda DENTRO da mesma trava que devolve a vaga ao inventario (ver
		// gerenciador.CancelarReservaComPersistencia): se o WAL falhar, a vaga nunca chega a ficar
		// visivelmente livre para um concorrente - o cancelamento e desfeito atomicamente ali mesmo.
		err = gerenciador.CancelarReservaComPersistencia(req.Token, req.ReservaID, func() error {
			return registrarOperacao(ctx, consorcio.OpCancelarReserva, consorcio.PayloadCancelarReservaDTO{
				ReservaID: req.ReservaID, PassageiroLogin: usuario.Login,
			})
		})
		if err != nil {
			log.Printf("[WAL][ERRO] Cliente #%d | CANCELAR_RESERVA nao persistido: %v", idCliente, err)
			return protocolo.Resposta{Status: protocolo.StatusErro, Erro: err.Error()}
		}

		log.Printf("[RESERVA CANCELADA] Cliente #%d | ID: %s", idCliente, req.ReservaID)
		return protocolo.Resposta{Status: protocolo.StatusOK, Mensagem: fmt.Sprintf("Reserva '%s' cancelada com sucesso. Vagas devolvidas.", req.ReservaID)}

	default:
		log.Printf("[ACAO INVALIDA] Cliente #%d | Acao nao reconhecida: '%s'", idCliente, req.Acao)
		return protocolo.Resposta{
			Status: protocolo.StatusErro,
			Erro:   fmt.Sprintf("Acao '%s' nao reconhecida.", req.Acao),
		}
	}
}
