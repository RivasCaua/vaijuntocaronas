// Comando seed_demo deixa o consorcio pronto para a apresentacao: publica a rota em cadeia
// Salvador -> Feira de Santana -> Vitoria da Conquista -> Montes Claros, uma perna em cada uma
// das 3 empresas informadas, e cadastra uma conta de passageiro pronta para logar ao vivo. A
// reserva em si NAO e feita por este comando - fica para ser demonstrada ao vivo na sala, já que
// o proprio checklist pede os testes/transacoes visiveis durante a apresentacao, nao pre-prontos.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"vaijunto/pkg/protocolo"
)

type perna struct {
	origem, destino string
	horario, chegada string
	assentos        int
	preco           float64
}

func main() {
	servidoresFlag := flag.String("servidores", "", "3 enderecos IP:Porta, em ordem Empresa A,B,C (ex: 172.16.201.3:8080,172.16.201.2:8080,172.16.201.16:8080)")
	data := flag.String("data", "2026-12-01", "Data (AAAA-MM-DD) usada nas 3 caronas semeadas")
	senha := flag.String("senha", "demo123", "Senha usada em todas as contas semeadas (motoristas e passageiro)")
	flag.Parse()

	servidores := []string{}
	for _, s := range strings.Split(*servidoresFlag, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			servidores = append(servidores, s)
		}
	}
	if len(servidores) != 3 {
		fmt.Println("[ERRO CRITICO] Informe exatamente 3 enderecos em -servidores, na ordem Empresa A,B,C")
		os.Exit(1)
	}

	pernas := []perna{
		{"Salvador", "Feira de Santana", "08:00", "10:00", 3, 25.0},
		{"Feira de Santana", "Vitória da Conquista", "11:00", "14:00", 3, 40.0},
		{"Vitória da Conquista", "Montes Claros", "15:00", "19:00", 3, 60.0},
	}
	nomesMotorista := []string{"motorista_empresa_a", "motorista_empresa_b", "motorista_empresa_c"}

	fmt.Println("================================================================")
	fmt.Println("   VaiJunto - Semeadura de Demonstração (rota em cadeia 3 empresas)")
	fmt.Println("================================================================")

	for i, srv := range servidores {
		p := pernas[i]
		caronaID, err := publicarCarona(srv, nomesMotorista[i], *senha, p, *data)
		if err != nil {
			fmt.Printf("[ERRO] Empresa %d (%s): %v\n", i+1, srv, err)
			continue
		}
		fmt.Printf("[OK] Empresa %d (%s): carona %s — %s → %s, %s-%s, %d vagas, R$%.2f/trecho\n",
			i+1, srv, caronaID, p.origem, p.destino, p.horario, p.chegada, p.assentos, p.preco)
	}

	passageiroUser := "passageiro_demo"
	if err := cadastrarPassageiro(servidores[0], passageiroUser, *senha); err != nil {
		fmt.Printf("[ERRO] Cadastro do passageiro de demonstração em %s: %v\n", servidores[0], err)
	} else {
		fmt.Printf("[OK] Passageiro de demonstração cadastrado em %s: usuário '%s'\n", servidores[0], passageiroUser)
	}

	fmt.Println("----------------------------------------------------------------")
	fmt.Println("PRONTO PARA A APRESENTAÇÃO — anote isto:")
	fmt.Printf("  Rota completa:     Salvador → Feira de Santana → Vitória da Conquista → Montes Claros\n")
	fmt.Printf("  Preço total:       R$%.2f (R$%.2f + R$%.2f + R$%.2f)\n", 25.0+40.0+60.0, 25.0, 40.0, 60.0)
	fmt.Printf("  Data usada:        %s\n", *data)
	fmt.Printf("  Login do passageiro ao vivo: usuário '%s', senha '%s', conectando em %s\n", passageiroUser, *senha, servidores[0])
	fmt.Println("  A reserva (2PC entre as 3 empresas) fica para ser feita AO VIVO na sala.")
	fmt.Println("================================================================")
}

func publicarCarona(servidor, usuario, senha string, p perna, data string) (string, error) {
	conn, err := net.Dial("tcp", servidor)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	scanner := protocolo.CriarScanner(conn)

	protocolo.EnviarMensagem(conn, protocolo.Requisicao{
		Acao: protocolo.AcaoCadastrar, Usuario: usuario, Senha: senha,
		Nome: "Motorista Demo", Papel: protocolo.PapelMotorista,
	})
	var respCad protocolo.Resposta
	protocolo.LerMensagem(scanner, &respCad)
	// usuário pode já existir de uma semeadura anterior - segue para o login mesmo assim

	protocolo.EnviarMensagem(conn, protocolo.Requisicao{Acao: protocolo.AcaoAutenticar, Usuario: usuario, Senha: senha})
	var respAuth protocolo.Resposta
	if err := protocolo.LerMensagem(scanner, &respAuth); err != nil {
		return "", err
	}
	if respAuth.Status != protocolo.StatusOK {
		return "", fmt.Errorf("login falhou: %s", respAuth.Erro)
	}

	protocolo.EnviarMensagem(conn, protocolo.Requisicao{
		Acao: protocolo.AcaoPublicarCarona, Token: respAuth.Token,
		Rota: []string{p.origem, p.destino}, Data: data,
		Horario: p.horario, HorarioChegada: p.chegada,
		Assentos: p.assentos, PrecoPorTrecho: p.preco,
	})
	var respPub protocolo.Resposta
	if err := protocolo.LerMensagem(scanner, &respPub); err != nil {
		return "", err
	}
	if respPub.Status != protocolo.StatusOK {
		return "", fmt.Errorf("publicação falhou: %s", respPub.Erro)
	}
	return respPub.CaronaID, nil
}

func cadastrarPassageiro(servidor, usuario, senha string) error {
	conn, err := net.Dial("tcp", servidor)
	if err != nil {
		return err
	}
	defer conn.Close()
	scanner := protocolo.CriarScanner(conn)

	protocolo.EnviarMensagem(conn, protocolo.Requisicao{
		Acao: protocolo.AcaoCadastrar, Usuario: usuario, Senha: senha,
		Nome: "Passageiro Demo", Papel: protocolo.PapelPassageiro,
	})
	var resp protocolo.Resposta
	if err := protocolo.LerMensagem(scanner, &resp); err != nil {
		return err
	}
	if resp.Status != protocolo.StatusOK && !strings.Contains(strings.ToLower(resp.Erro), "já existe") && !strings.Contains(strings.ToLower(resp.Erro), "existe") {
		return fmt.Errorf("%s", resp.Erro)
	}
	return nil
}
