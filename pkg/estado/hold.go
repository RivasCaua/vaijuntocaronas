package estado

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"vaijunto/pkg/protocolo"
)

// CriarHold implementa a Fase 1 (Prepare) do 2PC com Leases temporários.
// Reserva provisoriamente os assentos informados por um tempo de vida útil (TTL).
// Em caso de disputa pela última vaga, aplica o desempate determinístico de Lamport.
// CriarHold valida o token de sessao local e invoca a Fase 1 (Prepare) do 2PC.
func (g *GerenciadorEstado) CriarHold(token string, trechosSolicitados []protocolo.ItemTrecho, ttl time.Duration, ts protocolo.Timestamp) (*Hold, error) {
	u, err := g.ValidarToken(token)
	if err != nil {
		return nil, err
	}
	return g.CriarHoldInterno(u.Login, trechosSolicitados, ttl, ts)
}

// CriarHoldInterno implementa a Fase 1 (Prepare) do 2PC com Leases temporários.
// Permite que o Coordenador Distribuído acione reservas locais em nome de um passageiro autenticado na origem.
// Reserva provisoriamente os assentos informados por um tempo de vida útil (TTL).
// Em caso de disputa pela última vaga, aplica o desempate determinístico de Lamport.
func (g *GerenciadorEstado) CriarHoldInterno(login string, trechosSolicitados []protocolo.ItemTrecho, ttl time.Duration, ts protocolo.Timestamp) (*Hold, error) {
	if strings.TrimSpace(login) == "" {
		return nil, errors.New("login do passageiro obrigatorio para hold")
	}

	if len(trechosSolicitados) == 0 {
		return nil, errors.New("nenhum trecho informado para hold/reserva")
	}

	if ttl <= 0 {
		ttl = 30 * time.Second
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// 1. Limpa previamente quaisquer assentos órfãos que já tenham expirado
	g.expirarHoldsOrfaosTravado()

	// 2. FASE 1 (CHECK): Verifica disponibilidade e resolve conflitos de concorrência com Lamport
	// Rastreia holds preemptados para efetivar apenas se todos os trechos forem válidos
	holdsParaPreemptar := make(map[string]*Hold)

	for _, item := range trechosSolicitados {
		carona, existe := g.caronas[item.CaronaID]
		if !existe || !carona.Ativa {
			return nil, fmt.Errorf("carona %s nao disponivel", item.CaronaID)
		}

		chave := fmt.Sprintf("%s->%s", item.Origem, item.Destino)
		vagasDisponiveis := carona.VagasPorTrecho[chave]

		// Se não há vagas imediatas, avalia se há Hold em PREPARADO disputando a vaga
		if vagasDisponiveis <= 0 {
			holdConcorrente := g.encontrarHoldPreparadoConcorrenteTravado(item.CaronaID, chave)
			if holdConcorrente != nil && ts.Tempo > 0 {
				// Avalia regra de precedência total de Lamport (T1 < T2 ou empate com NodoID1 < NodoID2)
				if ts.MenorQue(holdConcorrente.TimestampLamport) {
					// O solicitante atual possui precedência lógica sobre o hold concorrente ainda não commitado!
					holdsParaPreemptar[holdConcorrente.ID] = holdConcorrente
				} else {
					return nil, fmt.Errorf("conflito concorrente no trecho %s: assento retido por transacao precedente (Lamport %s)", chave, holdConcorrente.TimestampLamport)
				}
			} else {
				return nil, fmt.Errorf("sem vagas no trecho %s", chave)
			}
		}
	}

	// 3. Aplica a preempção aos holds concorrentes que perderam na ordem lógica de Lamport
	for _, holdPerdedor := range holdsParaPreemptar {
		holdPerdedor.Status = StatusHoldPreemptado
		for _, item := range holdPerdedor.Trechos {
			if c, ok := g.caronas[item.CaronaID]; ok {
				chave := fmt.Sprintf("%s->%s", item.Origem, item.Destino)
				c.VagasPorTrecho[chave]++
			}
		}
	}

	// 4. FASE 1 (ACT - HOLD / LEASE): Bloqueia os assentos temporariamente
	itemsReserva := make([]ItemTrechoReserva, 0, len(trechosSolicitados))
	precoTotal := 0.0
	dataViagem := ""

	for _, item := range trechosSolicitados {
		carona := g.caronas[item.CaronaID]
		chave := fmt.Sprintf("%s->%s", item.Origem, item.Destino)

		carona.VagasPorTrecho[chave]--
		precoTotal += carona.PrecoPorTrecho
		dataViagem = carona.Data

		itemsReserva = append(itemsReserva, ItemTrechoReserva{
			CaronaID:  item.CaronaID,
			EmpresaID: carona.EmpresaID,
			Origem:    item.Origem,
			Destino:   item.Destino,
			Preco:     carona.PrecoPorTrecho,
		})
	}

	g.seqHoldId++
	holdID := fmt.Sprintf("h-%d", g.seqHoldId)
	agora := time.Now()

	novoHold := &Hold{
		ID:               holdID,
		PassageiroLogin:  login,
		Trechos:          itemsReserva,
		PrecoTotal:       precoTotal,
		Data:             dataViagem,
		TimestampLamport: ts,
		Status:           StatusHoldPreparado,
		CriadoEm:         agora,
		ExpiraEm:         agora.Add(ttl),
	}

	g.holds[holdID] = novoHold

	// Devolve uma CÓPIA, nunca o ponteiro interno: o Hold armazenado em g.holds pode ser mutado por
	// uma chamada CONCORRENTE (ex.: preemptado por uma transação de Lamport menor vinda de outro
	// participante) a qualquer momento após este Unlock. O chamador (handlePrepare2PC) usa o valor
	// retornado fora da trava - inclusive para serializar em JSON e persistir no WAL - e sem esta
	// cópia essa leitura corre sem sincronização contra a escrita de preempção (condição de corrida
	// real, detectada pelo "go test -race" em TestServidorREST_2PC_ConcorrenciaEntreCoordenadoresDeEmpresasDiferentes).
	copiaHold := *novoHold
	return &copiaHold, nil
}

// ConfirmarHold valida o token de sessao local e executa a Fase 2 (Commit) do 2PC.
func (g *GerenciadorEstado) ConfirmarHold(token, holdID string) (*Reserva, error) {
	u, err := g.ValidarToken(token)
	if err != nil {
		return nil, err
	}
	return g.ConfirmarHoldInterno(u.Login, holdID)
}

// ConfirmarHoldInterno implementa a Fase 2 (Commit) do 2PC sem exigir sessao local.
// Efetiva definitivamente a reserva se o hold estiver ativo e dentro do prazo do Lease.
func (g *GerenciadorEstado) ConfirmarHoldInterno(login, holdID string) (*Reserva, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.expirarHoldsOrfaosTravado()

	hold, existe := g.holds[holdID]
	if !existe {
		return nil, fmt.Errorf("hold %s nao encontrado", holdID)
	}

	if strings.TrimSpace(login) != "" && hold.PassageiroLogin != login {
		return nil, errors.New("voce nao tem permissao para confirmar este hold")
	}

	// Retentativa idempotente: se este EXATO hold já foi confirmado antes (ex.: o coordenador
	// reenviou o Commit porque a resposta da primeira tentativa se perdeu na rede, embora o commit
	// original tenha sido aplicado com sucesso), devolve a MESMA reserva já criada em vez de um erro
	// - sem isso, uma retentativa de commit bem-intencionada reportaria falha para uma reserva que na
	// verdade já está confirmada, arriscando o coordenador tratar uma transação bem-sucedida como
	// se tivesse falhado parcialmente.
	if hold.Status == StatusHoldConfirmado {
		if reservaID, ok := g.holdParaReserva[holdID]; ok {
			if r, existeReserva := g.reservas[reservaID]; existeReserva {
				copia := *r
				return &copia, nil
			}
		}
	}

	if hold.Status != StatusHoldPreparado {
		return nil, fmt.Errorf("hold %s nao esta disponivel para confirmacao (status: %s)", holdID, hold.Status)
	}

	if time.Now().After(hold.ExpiraEm) {
		g.devolverAssentosHoldTravado(hold, StatusHoldExpirado)
		return nil, errors.New("hold expirado por tempo limite (TTL)")
	}

	// Registra o passageiro nos trechos correspondentes
	for _, item := range hold.Trechos {
		if c, ok := g.caronas[item.CaronaID]; ok {
			chave := fmt.Sprintf("%s->%s", item.Origem, item.Destino)
			c.PassageirosPorTrecho[chave] = append(c.PassageirosPorTrecho[chave], hold.PassageiroLogin)
		}
	}

	g.seqReservaId++
	reservaID := fmt.Sprintf("r-%d", g.seqReservaId)

	novaReserva := &Reserva{
		ID:              reservaID,
		PassageiroLogin: hold.PassageiroLogin,
		Trechos:         hold.Trechos,
		PrecoTotal:      hold.PrecoTotal,
		Data:            hold.Data,
		Status:          "ATIVA",
		CriadoEm:        time.Now(),
	}

	g.reservas[reservaID] = novaReserva
	g.holdParaReserva[holdID] = reservaID
	hold.Status = StatusHoldConfirmado

	// Mesmo motivo do CriarHoldInterno acima: devolve uma cópia, nunca o ponteiro interno guardado
	// em g.reservas, que pode ser mutado por uma chamada concorrente (ex.: CancelarReserva) a
	// qualquer momento após este Unlock, enquanto o chamador ainda a usa fora da trava.
	copiaReserva := *novaReserva
	return &copiaReserva, nil
}

// AbortarHold valida o token de sessao local e executa a Fase 2 (Abort / Rollback) do 2PC.
func (g *GerenciadorEstado) AbortarHold(token, holdID string) error {
	u, err := g.ValidarToken(token)
	if err != nil {
		return err
	}
	return g.AbortarHoldInterno(u.Login, holdID)
}

// AbortarHoldInterno implementa a Fase 2 (Abort / Rollback / Compensação) do 2PC.
// Libera imediatamente os assentos provisoriamente retidos de volta ao inventário livre.
func (g *GerenciadorEstado) AbortarHoldInterno(login, holdID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	hold, existe := g.holds[holdID]
	if !existe {
		return fmt.Errorf("hold %s nao encontrado", holdID)
	}

	if strings.TrimSpace(login) != "" && hold.PassageiroLogin != login {
		return errors.New("voce nao tem permissao para abortar este hold")
	}

	if hold.Status != StatusHoldPreparado {
		return fmt.Errorf("hold %s nao esta em estado PREPARADO (status: %s)", holdID, hold.Status)
	}

	g.devolverAssentosHoldTravado(hold, StatusHoldAbortado)
	return nil
}

// ExpirarHoldsOrfaos executa uma varredura atômica devolvendo assentos órfãos cujo TTL expirou.
func (g *GerenciadorEstado) ExpirarHoldsOrfaos() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.expirarHoldsOrfaosTravado()
}

// expirarHoldsOrfaosTravado realiza a expiração assumindo que a trava g.mu já está adquirida.
func (g *GerenciadorEstado) expirarHoldsOrfaosTravado() int {
	agora := time.Now()
	totalExpirados := 0

	for _, hold := range g.holds {
		if hold.Status == StatusHoldPreparado && agora.After(hold.ExpiraEm) {
			g.devolverAssentosHoldTravado(hold, StatusHoldExpirado)
			totalExpirados++
		}
	}

	return totalExpirados
}

// devolverAssentosHoldTravado incrementa as vagas nos sub-trechos e altera o status do hold.
func (g *GerenciadorEstado) devolverAssentosHoldTravado(hold *Hold, novoStatus string) {
	hold.Status = novoStatus
	for _, item := range hold.Trechos {
		if c, ok := g.caronas[item.CaronaID]; ok {
			chave := fmt.Sprintf("%s->%s", item.Origem, item.Destino)
			c.VagasPorTrecho[chave]++
		}
	}
}

// encontrarHoldPreparadoConcorrenteTravado localiza um hold em estado PREPARADO disputando um trecho específico.
func (g *GerenciadorEstado) encontrarHoldPreparadoConcorrenteTravado(caronaID, trechoChave string) *Hold {
	var holdCandidato *Hold

	for _, h := range g.holds {
		if h.Status != StatusHoldPreparado {
			continue
		}

		for _, item := range h.Trechos {
			chave := fmt.Sprintf("%s->%s", item.Origem, item.Destino)
			if item.CaronaID == caronaID && chave == trechoChave {
				// Retorna o hold com maior Lamport para avaliar se o novo pode preemptá-lo
				if holdCandidato == nil || h.TimestampLamport.MaiorQue(holdCandidato.TimestampLamport) {
					holdCandidato = h
				}
			}
		}
	}

	return holdCandidato
}

// IniciarRotinaExpiracao inicia uma goroutine em segundo plano para limpeza contínua de assentos órfãos.
func (g *GerenciadorEstado) IniciarRotinaExpiracao(intervalo time.Duration) {
	g.mu.Lock()
	if g.cleanerAtivo {
		g.mu.Unlock()
		return
	}
	if intervalo <= 0 {
		intervalo = 500 * time.Millisecond
	}
	g.cleanerAtivo = true
	g.stopCleaner = make(chan struct{})
	g.mu.Unlock()

	go func() {
		ticker := time.NewTicker(intervalo)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				g.ExpirarHoldsOrfaos()
			case <-g.stopCleaner:
				return
			}
		}
	}()
}

// PararRotinaExpiracao finaliza com segurança a goroutine de limpeza de assentos órfãos.
func (g *GerenciadorEstado) PararRotinaExpiracao() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.cleanerAtivo {
		return
	}

	close(g.stopCleaner)
	g.cleanerAtivo = false
}

// ObterHold retorna uma cópia de consulta de um Hold por ID.
func (g *GerenciadorEstado) ObterHold(holdID string) (*Hold, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	hold, existe := g.holds[holdID]
	if !existe {
		return nil, fmt.Errorf("hold %s nao encontrado", holdID)
	}

	copia := *hold
	return &copia, nil
}

// ListarHoldsPassageiro retorna os holds pertencentes ao passageiro autenticado.
func (g *GerenciadorEstado) ListarHoldsPassageiro(token string) ([]protocolo.HoldDTO, error) {
	u, err := g.ValidarToken(token)
	if err != nil {
		return nil, err
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	var lista []protocolo.HoldDTO

	for _, h := range g.holds {
		if h.PassageiroLogin == u.Login {
			var trechosDTO []protocolo.ItemTrecho
			for _, t := range h.Trechos {
				trechosDTO = append(trechosDTO, protocolo.ItemTrecho{
					CaronaID:  t.CaronaID,
					EmpresaID: t.EmpresaID,
					Origem:    t.Origem,
					Destino:   t.Destino,
					Preco:     t.Preco,
				})
			}

			lista = append(lista, protocolo.HoldDTO{
				HoldID:         h.ID,
				Passageiro:     h.PassageiroLogin,
				Trechos:        trechosDTO,
				PrecoTotal:     h.PrecoTotal,
				Data:           h.Data,
				Status:         h.Status,
				LamportTempo:   h.TimestampLamport.Tempo,
				LamportNodoID:  h.TimestampLamport.NodoID,
				ExpiraEmUnixMs: h.ExpiraEm.UnixMilli(),
			})
		}
	}

	return lista, nil
}
