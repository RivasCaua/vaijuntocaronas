package estado

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Este arquivo implementa o lado "replay" do protocolo de recuperação pós-queda.
// Cada método Aplicar* projeta diretamente sobre os mapas internos do GerenciadorEstado
// um fato JÁ DECIDIDO e persistido anteriormente no Write-Ahead Log (WAL) do servidor
// (pacote vaijunto/pkg/consorcio). Diferente dos métodos públicos "ao vivo" (CadastrarUsuario,
// PublicarCarona, ReservarItinerario, ...), estes NÃO validam token de sessão nem geram novos
// identificadores: eles reconstroem o estado usando exatamente os mesmos IDs que já haviam sido
// gerados e confirmados ao cliente antes da queda, mantendo o resultado final bit-a-bit idêntico.
//
// Todos os métodos são idempotentes: aplicar o mesmo registro duas vezes (por exemplo, caso o
// processo de recuperação seja interrompido e reiniciado) não duplica efeitos colaterais.

// avancarSequencia garante que o contador de geração de IDs ao vivo nunca colida com um ID
// já usado por um registro recuperado do WAL (ex: depois de recuperar "c-7", seqCaronaId >= 7).
func avancarSequencia(seq *int, id, prefixo string) {
	numero, err := strconv.Atoi(strings.TrimPrefix(id, prefixo))
	if err == nil && numero > *seq {
		*seq = numero
	}
}

// AplicarCadastroUsuario reconstrói um usuário a partir de um registro CADASTRAR_USUARIO do WAL.
func (g *GerenciadorEstado) AplicarCadastroUsuario(login, senha, nome, papel string) error {
	login = strings.TrimSpace(login)
	if login == "" {
		return fmt.Errorf("registro de cadastro invalido: login vazio")
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if _, existe := g.usuarios[login]; existe {
		return nil // ja aplicado anteriormente (replay idempotente)
	}

	g.usuarios[login] = &Usuario{
		Login: login,
		Senha: senha,
		Nome:  nome,
		Papel: papel,
	}
	return nil
}

// AplicarPublicacaoCarona reconstrói uma carona a partir de um registro PUBLICAR_CARONA do WAL,
// preservando o CaronaID original e recalculando o inventário de vagas por sub-trecho do zero.
func (g *GerenciadorEstado) AplicarPublicacaoCarona(caronaID, empresaID, motoristaLogin string, rota []string, data, horario, horarioChegada string, assentos int, preco float64) error {
	if strings.TrimSpace(caronaID) == "" {
		return fmt.Errorf("registro de carona invalido: ID vazio")
	}
	if len(rota) < 2 {
		return fmt.Errorf("registro da carona %s invalido: rota com menos de 2 cidades", caronaID)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if _, existe := g.caronas[caronaID]; existe {
		return nil // ja aplicado anteriormente (replay idempotente)
	}

	vagasPorTrecho := make(map[string]int)
	passageirosPorTrecho := make(map[string][]string)
	for i := 0; i < len(rota)-1; i++ {
		chave := fmt.Sprintf("%s->%s", rota[i], rota[i+1])
		vagasPorTrecho[chave] = assentos
		passageirosPorTrecho[chave] = make([]string, 0)
	}

	g.caronas[caronaID] = &Carona{
		ID:                   caronaID,
		EmpresaID:            empresaID,
		MotoristaLogin:       motoristaLogin,
		Rota:                 rota,
		Data:                 data,
		Horario:              horario,
		HorarioChegada:       horarioChegada,
		AssentosTotais:       assentos,
		PrecoPorTrecho:       preco,
		VagasPorTrecho:       vagasPorTrecho,
		PassageirosPorTrecho: passageirosPorTrecho,
		Ativa:                true,
	}

	avancarSequencia(&g.seqCaronaId, caronaID, "c-")
	return nil
}

// AplicarReserva reconstrói uma reserva a partir de um registro RESERVAR (ou 2PC_COMMIT) do WAL,
// debitando as vagas correspondentes nas caronas já reconstruídas pelos registros anteriores.
func (g *GerenciadorEstado) AplicarReserva(reservaID, passageiroLogin string, trechos []ItemTrechoReserva, precoTotal float64, data string) error {
	if strings.TrimSpace(reservaID) == "" {
		return fmt.Errorf("registro de reserva invalido: ID vazio")
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if _, existe := g.reservas[reservaID]; existe {
		return nil // ja aplicado anteriormente (replay idempotente)
	}

	for _, item := range trechos {
		carona, ok := g.caronas[item.CaronaID]
		if !ok {
			continue // carona proveniente de uma empresa parceira remota; nao ha inventario local a debitar
		}
		chave := fmt.Sprintf("%s->%s", item.Origem, item.Destino)
		if carona.VagasPorTrecho[chave] > 0 {
			carona.VagasPorTrecho[chave]--
		}
		carona.PassageirosPorTrecho[chave] = append(carona.PassageirosPorTrecho[chave], passageiroLogin)
	}

	g.reservas[reservaID] = &Reserva{
		ID:              reservaID,
		PassageiroLogin: passageiroLogin,
		Trechos:         trechos,
		PrecoTotal:      precoTotal,
		Data:            data,
		Status:          "ATIVA",
		CriadoEm:        time.Now(),
	}

	avancarSequencia(&g.seqReservaId, reservaID, "r-")
	return nil
}

// AplicarCancelamentoReserva reconstrói o efeito de um registro CANCELAR_RESERVA do WAL,
// devolvendo as vagas correspondentes e marcando a reserva como CANCELADA.
func (g *GerenciadorEstado) AplicarCancelamentoReserva(reservaID, passageiroLogin string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	r, existe := g.reservas[reservaID]
	if !existe || r.Status != "ATIVA" {
		return nil // ja cancelada ou inexistente nesta instancia (replay idempotente/tolerante)
	}

	r.Status = "CANCELADA"

	for _, item := range r.Trechos {
		carona, ok := g.caronas[item.CaronaID]
		if !ok {
			continue
		}
		chave := fmt.Sprintf("%s->%s", item.Origem, item.Destino)
		carona.VagasPorTrecho[chave]++

		restantes := make([]string, 0, len(carona.PassageirosPorTrecho[chave]))
		for _, login := range carona.PassageirosPorTrecho[chave] {
			if login != passageiroLogin {
				restantes = append(restantes, login)
			}
		}
		carona.PassageirosPorTrecho[chave] = restantes
	}

	return nil
}

// AplicarCancelamentoCarona reconstrói o efeito de um registro CANCELAR_CARONA do WAL,
// desativando a carona e marcando como canceladas as reservas ativas vinculadas a ela.
func (g *GerenciadorEstado) AplicarCancelamentoCarona(caronaID, motoristaLogin string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	c, existe := g.caronas[caronaID]
	if !existe || !c.Ativa {
		return nil // ja cancelada ou inexistente nesta instancia (replay idempotente/tolerante)
	}

	c.Ativa = false

	for _, r := range g.reservas {
		if r.Status != "ATIVA" {
			continue
		}
		for _, item := range r.Trechos {
			if item.CaronaID == caronaID {
				r.Status = "CANCELADA_PELO_MOTORISTA"
				break
			}
		}
	}

	return nil
}
