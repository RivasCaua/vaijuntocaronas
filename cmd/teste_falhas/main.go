// Comando teste_falhas e um robo de CAOS: ele compila o binario real de cmd/servidor, sobe uma
// topologia completa de 3 empresas do consorcio VaiJunto (processos de SO independentes, cada
// qual com seu proprio Write-Ahead Log em disco), gera carga real sobre elas via TCP/REST, mata
// ABRUPTAMENTE (SIGKILL, sem desligamento gracioso) um dos nos no meio da operacao, audita o
// comportamento do restante do cluster durante a queda e, por fim, reinicia o no abatido para
// verificar se o protocolo de recuperacao via WAL reconstroi o estado anterior bit-a-bit.
//
// Este robo nao substitui os testes unitarios de pkg/consorcio (que validam o Scatter-Gather e o
// 2PC em memoria, com listeners efemeros de teste): aqui a falha e um processo de sistema
// operacional de verdade sendo morto de verdade, exatamente o cenario de "queda de no" que a
// arquitetura do consorcio (WAL + Lamport + 2PC com compensacao) foi desenhada para tolerar.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vaijunto/pkg/consorcio"
)

// nodeSpec descreve a configuracao estatica de uma empresa/no do consorcio simulado.
type nodeSpec struct {
	ID        string
	Regiao    string
	PortaTCP  int
	PortaREST int
	WALPath   string
}

// nodeProc e a instancia de runtime (processo de SO vivo ou morto) de um nodeSpec.
type nodeProc struct {
	spec    nodeSpec
	cmd     *exec.Cmd
	logFile *os.File
	logPath string
	vivo    atomic.Bool
}

func main() {
	goBin := flag.String("go-bin", "go", "Caminho do executavel da toolchain Go usado para compilar o cmd/servidor")
	raizModulo := flag.String("raiz-modulo", ".", "Diretorio raiz do modulo Go (contendo go.mod)")
	dirTrabalho := flag.String("dir-trabalho", "", "Diretorio de trabalho para WAL/binario/logs (vazio = diretorio temporario descartavel)")
	manterArquivos := flag.Bool("manter-arquivos", false, "Nao remove o diretorio de trabalho ao final (para inspecao manual)")
	portaTCPBase := flag.Int("porta-tcp-base", 18080, "Porta TCP inicial (nos recebem base, base+1, base+2)")
	portaRESTBase := flag.Int("porta-rest-base", 19090, "Porta REST inicial (nos recebem base, base+1, base+2)")
	noAlvo := flag.String("no-alvo", "nodo-feira", "ID do no que sofrera a queda abrupta (nodo-salvador|nodo-feira|nodo-reconcavo)")
	tempoQueda := flag.Duration("tempo-queda", 4*time.Second, "Por quanto tempo o no abatido permanece fora do ar antes de ser reiniciado")
	clientesConcorrentes := flag.Int("clientes-concorrentes", 12, "Clientes de leitura concorrentes gerando trafego durante toda a janela de caos")
	dataViagem := flag.String("data", "2026-11-20", "Data de viagem usada nas caronas e reservas de linha de base")
	flag.Parse()

	fmt.Println("================================================================")
	fmt.Println("   VaiJunto - Robo de Caos: Injecao de Falhas e Queda de Nos")
	fmt.Println("================================================================")

	relatorio := NovoRelatorioCaos()

	// --- PREPARACAO DO AMBIENTE DE TRABALHO ---
	workDir, limpezaWorkDir, err := prepararDiretorioTrabalho(*dirTrabalho, *manterArquivos)
	if err != nil {
		fmt.Printf("[ERRO CRITICO] %v\n", err)
		os.Exit(1)
	}
	defer limpezaWorkDir()
	fmt.Printf("[SETUP] Diretorio de trabalho: %s\n", workDir)

	binPath := filepath.Join(workDir, "servidor_bin")
	fmt.Println("[SETUP] Compilando o binario real de cmd/servidor...")
	if err := compilarServidor(*goBin, *raizModulo, binPath); err != nil {
		fmt.Printf("[ERRO CRITICO] Falha ao compilar cmd/servidor: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[SETUP] Binario compilado com sucesso: %s\n", binPath)

	especificacoes := []nodeSpec{
		{ID: "nodo-salvador", Regiao: consorcio.RegiaoRMS, PortaTCP: *portaTCPBase, PortaREST: *portaRESTBase, WALPath: filepath.Join(workDir, "nodo-salvador.wal")},
		{ID: "nodo-feira", Regiao: consorcio.RegiaoPortalDoSertao, PortaTCP: *portaTCPBase + 1, PortaREST: *portaRESTBase + 1, WALPath: filepath.Join(workDir, "nodo-feira.wal")},
		{ID: "nodo-reconcavo", Regiao: consorcio.RegiaoReconcavo, PortaTCP: *portaTCPBase + 2, PortaREST: *portaRESTBase + 2, WALPath: filepath.Join(workDir, "nodo-reconcavo.wal")},
	}

	alvoValido := false
	for _, e := range especificacoes {
		if e.ID == *noAlvo {
			alvoValido = true
		}
	}
	if !alvoValido {
		fmt.Printf("[ERRO CRITICO] -no-alvo '%s' invalido. Use um de: nodo-salvador, nodo-feira, nodo-reconcavo\n", *noAlvo)
		os.Exit(1)
	}

	// --- FASE 1: BOOT DA TOPOLOGIA COMPLETA (3 PROCESSOS REAIS) ---
	fmt.Println("\n[FASE 1] Subindo os 3 nos do consorcio como processos de SO independentes...")
	nodos := make(map[string]*nodeProc)
	for _, spec := range especificacoes {
		np, err := iniciarNodo(binPath, workDir, spec, especificacoes)
		if err != nil {
			fmt.Printf("[ERRO CRITICO] Falha ao iniciar %s: %v\n", spec.ID, err)
			encerrarTodos(nodos)
			os.Exit(1)
		}
		nodos[spec.ID] = np
	}
	defer encerrarTodos(nodos)

	for _, spec := range especificacoes {
		if err := aguardarSaude(spec.PortaREST, 10*time.Second); err != nil {
			fmt.Printf("[ERRO CRITICO] %s nunca respondeu /health: %v\n%s\n", spec.ID, err, lerLogTail(nodos[spec.ID]))
			os.Exit(1)
		}
		fmt.Printf("[FASE 1] %-16s ONLINE  (tcp=:%d rest=:%d wal=%s)\n", spec.ID, spec.PortaTCP, spec.PortaREST, filepath.Base(spec.WALPath))
	}
	relatorio.MarcarBootCompleto()

	// --- FASE 2: LINHA DE BASE (CADASTRO, PUBLICACAO, RESERVA REAL EM CADA NO) ---
	fmt.Println("\n[FASE 2] Gerando linha de base: cadastro, publicacao de caronas e reservas reais em cada no...")
	baselines := make(map[string]*baselineNo)
	for _, spec := range especificacoes {
		bl, err := gerarLinhaDeBase(spec, *dataViagem)
		if err != nil {
			fmt.Printf("[ERRO CRITICO] Falha na linha de base de %s: %v\n", spec.ID, err)
			os.Exit(1)
		}
		baselines[spec.ID] = bl
		fmt.Printf("[FASE 2] %-16s carona=%s rota=%s->%s vagas_apos_reserva=%d reserva=%s\n",
			spec.ID, bl.CaronaID, bl.Origem, bl.Destino, bl.VagasRestantes, bl.ReservaID)
	}
	relatorio.MarcarBaselineCompleto(baselines)

	// --- FASE 3: CARGA CONCORRENTE CONTINUA DURANTE TODA A JANELA DE CAOS ---
	fmt.Println("\n[FASE 3] Iniciando geradores de carga concorrente de fundo (ler antes/durante/depois da queda)...")
	cargaCtx, pararCarga := context.WithCancel(context.Background())
	var wgCarga sync.WaitGroup
	cargaStats := NovoContadorCarga()
	for _, spec := range especificacoes {
		for i := 0; i < *clientesConcorrentes; i++ {
			wgCarga.Add(1)
			go geradorCargaContinua(cargaCtx, &wgCarga, spec, *dataViagem, cargaStats)
		}
	}
	time.Sleep(500 * time.Millisecond) // garante que a carga baseline (pre-queda) ja esta fluindo

	// --- FASE 4: O CAOS - QUEDA ABRUPTA (SIGKILL) DO NO ALVO ---
	alvo := nodos[*noAlvo]
	fmt.Printf("\n[FASE 4] *** INJETANDO FALHA: matando %s com SIGKILL (queda abrupta, sem shutdown gracioso) ***\n", *noAlvo)
	tInicioQueda := time.Now()
	if err := alvo.cmd.Process.Kill(); err != nil {
		fmt.Printf("[AVISO] Falha ao enviar SIGKILL a %s: %v\n", *noAlvo, err)
	}
	_, _ = alvo.cmd.Process.Wait()
	alvo.vivo.Store(false)
	fmt.Printf("[FASE 4] %s morto em %s\n", *noAlvo, time.Since(tInicioQueda).Round(time.Millisecond))

	// Confirma falha rapida (connection refused), sem hang, ao tentar falar com o no morto.
	tDeteccao, erroDeteccao := medirFalhaRapida(alvo.spec)
	relatorio.MarcarQuedaDetectada(*noAlvo, tInicioQueda, tDeteccao, erroDeteccao)
	fmt.Printf("[FASE 4] Falha de conexao com %s detectada em %s (erro: %v)\n", *noAlvo, tDeteccao.Round(time.Millisecond), erroDeteccao)

	// --- FASE 5: AUDITORIA DE ISOLAMENTO - OS SOBREVIVENTES CONTINUAM SAUDAVEIS? ---
	fmt.Println("\n[FASE 5] Auditando isolamento de falha: os nos sobreviventes continuam respondendo normalmente?")
	for _, spec := range especificacoes {
		if spec.ID == *noAlvo {
			continue
		}
		ok, latencia := verificarSobrevivente(spec)
		relatorio.MarcarSobrevivente(spec.ID, ok, latencia)
		status := "OK"
		if !ok {
			status = "FALHOU"
		}
		fmt.Printf("[FASE 5] %-16s respondeu /health em %s -> %s\n", spec.ID, latencia.Round(time.Millisecond), status)
	}

	// --- FASE 6: AUDITORIA DE COMPENSACAO 2PC SOB FALHA DE PARTICIPANTE ---
	fmt.Println("\n[FASE 6] Auditando compensacao do 2PC: reserva cruzada envolvendo o no abatido deve abortar e devolver vagas...")
	resultado2PC := auditar2PCComParticipanteCaido(especificacoes, baselines, *noAlvo)
	relatorio.MarcarAuditoria2PC(resultado2PC)
	fmt.Printf("[FASE 6] %s\n", resultado2PC.Resumo)

	// --- FASE 7: A QUEDA PERSISTE PELO TEMPO CONFIGURADO, SOB CARGA CONTINUA ---
	fmt.Printf("\n[FASE 7] Mantendo %s fora do ar por %s sob carga continua nos sobreviventes...\n", *noAlvo, tempoQueda.String())
	time.Sleep(*tempoQueda)

	pararCarga()
	wgCarga.Wait()
	relatorio.MarcarCarga(cargaStats)

	// --- FASE 8: RECUPERACAO - REINICIA O NO ABATIDO COM O MESMO WAL EM DISCO ---
	fmt.Printf("\n[FASE 8] Reiniciando %s (mesmo WAL em disco) para validar recuperacao pos-queda...\n", *noAlvo)
	tInicioRecuperacao := time.Now()
	novoProc, err := iniciarNodo(binPath, workDir, alvo.spec, especificacoes)
	if err != nil {
		fmt.Printf("[ERRO CRITICO] Falha ao reiniciar %s: %v\n", *noAlvo, err)
		os.Exit(1)
	}
	nodos[*noAlvo] = novoProc

	if err := aguardarSaude(alvo.spec.PortaREST, 10*time.Second); err != nil {
		fmt.Printf("[ERRO CRITICO] %s nao voltou ao ar apos reinicio: %v\n%s\n", *noAlvo, err, lerLogTail(novoProc))
		os.Exit(1)
	}
	tempoRecuperacao := time.Since(tInicioRecuperacao)
	fmt.Printf("[FASE 8] %s ONLINE novamente em %s\n", *noAlvo, tempoRecuperacao.Round(time.Millisecond))

	linhasRecuperacao := extrairLinhasRecuperacao(novoProc)
	for _, linha := range linhasRecuperacao {
		fmt.Printf("           %s\n", linha)
	}
	relatorio.MarcarRecuperacao(tempoRecuperacao, linhasRecuperacao)

	// --- FASE 9: VERIFICACAO DE INTEGRIDADE POS-RECUPERACAO (SEM PERDA, SEM DUPLICACAO) ---
	fmt.Println("\n[FASE 9] Verificando integridade do estado reconstruido via WAL apos a recuperacao...")
	integridadeOK, detalhesIntegridade := verificarIntegridadePosRecuperacao(alvo.spec, baselines[*noAlvo])
	relatorio.MarcarIntegridade(integridadeOK, detalhesIntegridade)
	for _, d := range detalhesIntegridade {
		fmt.Printf("           %s\n", d)
	}

	// --- RELATORIO FINAL ---
	relatorio.Imprimir(especificacoes, *noAlvo)

	if !relatorio.Sucesso() {
		os.Exit(1)
	}
}

// --- INFRAESTRUTURA DE PROCESSOS ---

func prepararDiretorioTrabalho(caminho string, manter bool) (string, func(), error) {
	if strings.TrimSpace(caminho) != "" {
		if err := os.MkdirAll(caminho, 0755); err != nil {
			return "", nil, fmt.Errorf("nao foi possivel criar -dir-trabalho '%s': %w", caminho, err)
		}
		return caminho, func() {}, nil
	}

	dir, err := os.MkdirTemp("", "vaijunto-teste-falhas-*")
	if err != nil {
		return "", nil, fmt.Errorf("nao foi possivel criar diretorio temporario: %w", err)
	}

	limpeza := func() {
		if manter {
			fmt.Printf("\n[LIMPEZA] -manter-arquivos ativo: diretorio preservado em %s\n", dir)
			return
		}
		_ = os.RemoveAll(dir)
	}
	return dir, limpeza, nil
}

func compilarServidor(goBin, raizModulo, binPath string) error {
	cmd := exec.Command(goBin, "build", "-o", binPath, "./cmd/servidor")
	cmd.Dir = raizModulo
	saida, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w\n%s", err, string(saida))
	}
	return nil
}

// iniciarNodo sobe um processo real do binario servidor_bin com a topologia completa do consorcio.
func iniciarNodo(binPath, workDir string, spec nodeSpec, todas []nodeSpec) (*nodeProc, error) {
	var membros []string
	for _, outra := range todas {
		if outra.ID == spec.ID {
			continue
		}
		membros = append(membros, fmt.Sprintf("%s@127.0.0.1:%d", outra.ID, outra.PortaREST))
	}

	args := []string{
		"-empresa-id=" + spec.ID,
		"-regiao=" + spec.Regiao,
		"-endereco=127.0.0.1",
		fmt.Sprintf("-porta=%d", spec.PortaTCP),
		fmt.Sprintf("-porta-rest=%d", spec.PortaREST),
		"-wal=" + spec.WALPath,
		"-membros=" + strings.Join(membros, ","),
	}

	logPath := filepath.Join(workDir, spec.ID+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("falha ao criar log de %s: %w", spec.ID, err)
	}

	cmd := exec.Command(binPath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, fmt.Errorf("falha ao iniciar processo de %s: %w", spec.ID, err)
	}

	np := &nodeProc{spec: spec, cmd: cmd, logFile: logFile, logPath: logPath}
	np.vivo.Store(true)
	return np, nil
}

func encerrarTodos(nodos map[string]*nodeProc) {
	for _, np := range nodos {
		if np.vivo.Load() && np.cmd.Process != nil {
			_ = np.cmd.Process.Kill()
			_, _ = np.cmd.Process.Wait()
		}
		if np.logFile != nil {
			np.logFile.Close()
		}
	}
}

func lerLogTail(np *nodeProc) string {
	dados, err := os.ReadFile(np.logPath)
	if err != nil {
		return "(log indisponivel)"
	}
	linhas := strings.Split(strings.TrimRight(string(dados), "\n"), "\n")
	inicio := 0
	if len(linhas) > 15 {
		inicio = len(linhas) - 15
	}
	return "--- ultimas linhas do log ---\n" + strings.Join(linhas[inicio:], "\n")
}

func extrairLinhasRecuperacao(np *nodeProc) []string {
	dados, err := os.ReadFile(np.logPath)
	if err != nil {
		return nil
	}
	var linhas []string
	for _, l := range strings.Split(string(dados), "\n") {
		if strings.Contains(l, "[RECUPERACAO]") {
			linhas = append(linhas, strings.TrimSpace(l))
		}
	}
	return linhas
}

func aguardarSaude(portaREST int, timeout time.Duration) error {
	cliente := &http.Client{Timeout: 500 * time.Millisecond}
	limite := time.Now().Add(timeout)
	url := fmt.Sprintf("http://127.0.0.1:%d/health", portaREST)

	var ultimoErro error
	for time.Now().Before(limite) {
		resp, err := cliente.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		} else {
			ultimoErro = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout aguardando saude em %s: %v", url, ultimoErro)
}

func medirFalhaRapida(spec nodeSpec) (time.Duration, error) {
	inicio := time.Now()
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", spec.PortaTCP), 2*time.Second)
	duracao := time.Since(inicio)
	if err == nil {
		conn.Close()
		return duracao, fmt.Errorf("conexao TCP inesperadamente aceita pelo no supostamente morto")
	}
	return duracao, err
}

func verificarSobrevivente(spec nodeSpec) (bool, time.Duration) {
	inicio := time.Now()
	cliente := &http.Client{Timeout: 2 * time.Second}
	resp, err := cliente.Get(fmt.Sprintf("http://127.0.0.1:%d/health", spec.PortaREST))
	duracao := time.Since(inicio)
	if err != nil {
		return false, duracao
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK, duracao
}
