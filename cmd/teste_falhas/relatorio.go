package main

import (
	"fmt"
	"sort"
	"time"
)

// RelatorioCaos acumula o veredito de cada fase do robo de caos para a impressao final consolidada.
type RelatorioCaos struct {
	bootCompleto     bool
	baselineCompleto bool
	baselines        map[string]*baselineNo

	noAlvo        string
	tempoDeteccao time.Duration
	erroDeteccao  error

	sobreviventes         map[string]bool
	sobreviventesLatencia map[string]time.Duration

	resultado2PC Resultado2PC

	cargaSnapshot map[string][2]int64

	tempoRecuperacao  time.Duration
	linhasRecuperacao []string

	integridadeOK       bool
	detalhesIntegridade []string
}

// NovoRelatorioCaos inicializa um relatorio vazio pronto para ser preenchido fase a fase.
func NovoRelatorioCaos() *RelatorioCaos {
	return &RelatorioCaos{
		sobreviventes:         make(map[string]bool),
		sobreviventesLatencia: make(map[string]time.Duration),
	}
}

func (r *RelatorioCaos) MarcarBootCompleto() { r.bootCompleto = true }

func (r *RelatorioCaos) MarcarBaselineCompleto(baselines map[string]*baselineNo) {
	r.baselineCompleto = true
	r.baselines = baselines
}

func (r *RelatorioCaos) MarcarQuedaDetectada(noAlvo string, _ time.Time, tempoDeteccao time.Duration, erro error) {
	r.noAlvo = noAlvo
	r.tempoDeteccao = tempoDeteccao
	r.erroDeteccao = erro
}

func (r *RelatorioCaos) MarcarSobrevivente(id string, ok bool, latencia time.Duration) {
	r.sobreviventes[id] = ok
	r.sobreviventesLatencia[id] = latencia
}

func (r *RelatorioCaos) MarcarAuditoria2PC(res Resultado2PC) { r.resultado2PC = res }

func (r *RelatorioCaos) MarcarCarga(c *ContadorCarga) { r.cargaSnapshot = c.Snapshot() }

func (r *RelatorioCaos) MarcarRecuperacao(duracao time.Duration, linhas []string) {
	r.tempoRecuperacao = duracao
	r.linhasRecuperacao = linhas
}

func (r *RelatorioCaos) MarcarIntegridade(ok bool, detalhes []string) {
	r.integridadeOK = ok
	r.detalhesIntegridade = detalhes
}

// Sucesso consolida o veredito geral: TODAS as verificacoes criticas precisam ter passado.
func (r *RelatorioCaos) Sucesso() bool {
	if !r.bootCompleto || !r.baselineCompleto {
		return false
	}
	if r.erroDeteccao == nil {
		return false // o no morto deveria ter se tornado inalcancavel
	}
	for _, ok := range r.sobreviventes {
		if !ok {
			return false
		}
	}
	if !r.resultado2PC.Sucesso {
		return false
	}
	return r.integridadeOK
}

func marcaOK(ok bool) string {
	if ok {
		return "✅ PASS"
	}
	return "❌ FALHA"
}

// Imprimir exibe o relatorio consolidado final do robo de caos, no mesmo estilo de auditoria
// textual usado por cmd/teste_carga.
func (r *RelatorioCaos) Imprimir(especificacoes []nodeSpec, noAlvo string) {
	fmt.Println("\n================================================================")
	fmt.Println("           RELATORIO FINAL DO ROBO DE CAOS (INJECAO DE FALHAS)")
	fmt.Println("================================================================")

	fmt.Printf("Topologia testada:            %d nos (processos de SO reais e independentes)\n", len(especificacoes))
	for _, e := range especificacoes {
		marcador := "sobrevivente"
		if e.ID == noAlvo {
			marcador = "ALVO DA QUEDA"
		}
		fmt.Printf("   - %-16s regiao=%-16s tcp=:%-5d rest=:%-5d [%s]\n", e.ID, e.Regiao, e.PortaTCP, e.PortaREST, marcador)
	}

	fmt.Println("----------------------------------------------------------------")
	fmt.Printf("1. Boot da topologia completa:            %s\n", marcaOK(r.bootCompleto))
	fmt.Printf("2. Linha de base real (TCP+WAL) por no:    %s\n", marcaOK(r.baselineCompleto))

	fmt.Println("----------------------------------------------------------------")
	fmt.Printf("3. Queda abrupta (SIGKILL) de '%s':\n", noAlvo)
	fmt.Printf("   Falha de conexao detectada em:          %s\n", r.tempoDeteccao.Round(time.Millisecond))
	fmt.Printf("   Natureza da falha observada:             %v\n", r.erroDeteccao)
	fmt.Printf("   Resultado:                                %s (conexao deveria falhar rapido, sem hang)\n", marcaOK(r.erroDeteccao != nil))

	fmt.Println("----------------------------------------------------------------")
	fmt.Println("4. Isolamento de falha - nos sobreviventes continuam saudaveis:")
	idsSobreviventes := make([]string, 0, len(r.sobreviventes))
	for id := range r.sobreviventes {
		idsSobreviventes = append(idsSobreviventes, id)
	}
	sort.Strings(idsSobreviventes)
	for _, id := range idsSobreviventes {
		fmt.Printf("   %-16s latencia=%-10s %s\n", id, r.sobreviventesLatencia[id].Round(time.Millisecond), marcaOK(r.sobreviventes[id]))
	}

	fmt.Println("----------------------------------------------------------------")
	fmt.Println("5. Auditoria de compensacao do 2PC sob falha de participante:")
	fmt.Printf("   %s\n", r.resultado2PC.Resumo)
	fmt.Printf("   Resultado:                                %s\n", marcaOK(r.resultado2PC.Sucesso))

	fmt.Println("----------------------------------------------------------------")
	fmt.Println("6. Carga continua de leitura durante toda a janela de caos:")
	nodosCarga := make([]string, 0, len(r.cargaSnapshot))
	for id := range r.cargaSnapshot {
		nodosCarga = append(nodosCarga, id)
	}
	sort.Strings(nodosCarga)
	for _, id := range nodosCarga {
		par := r.cargaSnapshot[id]
		ok, falha := par[0], par[1]
		total := ok + falha
		taxa := 0.0
		if total > 0 {
			taxa = float64(ok) / float64(total) * 100
		}
		observacao := ""
		if id == noAlvo {
			observacao = " (esperado: maioria de falhas enquanto esteve fora do ar)"
		}
		fmt.Printf("   %-16s requisicoes=%-6d sucesso=%-6d falha=%-6d taxa_sucesso=%6.2f%%%s\n",
			id, total, ok, falha, taxa, observacao)
	}

	fmt.Println("----------------------------------------------------------------")
	fmt.Printf("7. Recuperacao pos-queda (reinicio com o mesmo WAL em disco):\n")
	fmt.Printf("   Tempo ate voltar a responder /health:    %s\n", r.tempoRecuperacao.Round(time.Millisecond))
	if len(r.linhasRecuperacao) == 0 {
		fmt.Println("   ⚠️  Nenhuma linha [RECUPERACAO] encontrada no log (WAL pode ter sido tratado como vazio)")
	}
	for _, l := range r.linhasRecuperacao {
		fmt.Printf("   %s\n", l)
	}

	fmt.Println("----------------------------------------------------------------")
	fmt.Println("8. Integridade do estado apos recuperacao via WAL:")
	for _, d := range r.detalhesIntegridade {
		fmt.Printf("   %s\n", d)
	}
	fmt.Printf("   Resultado:                                %s\n", marcaOK(r.integridadeOK))

	fmt.Println("================================================================")
	if r.Sucesso() {
		fmt.Println("✅ VEREDITO GERAL: o consorcio tolerou a queda abrupta de um no sem perda de dados,")
		fmt.Println("   sem overbooking logico e sem contaminar os nos sobreviventes (ZERO REGRESSOES).")
	} else {
		fmt.Println("❌ VEREDITO GERAL: uma ou mais verificacoes criticas de resiliencia falharam.")
		fmt.Println("   Revise as secoes marcadas com FALHA acima antes de considerar o sistema resiliente.")
	}
	fmt.Println("================================================================")
}
