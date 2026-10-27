package estado

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// VersaoSnapshot identifica a versão do formato de persistência
const VersaoSnapshot = "1.0"

// SnapshotEstado consolida o estado completo do gerenciador para persistência durável em JSON.
type SnapshotEstado struct {
	Versao       string              `json:"versao"`
	SalvoEm      time.Time           `json:"salvo_em"`
	Usuarios     map[string]*Usuario `json:"usuarios"`
	Caronas      map[string]*Carona  `json:"caronas"`
	Reservas     map[string]*Reserva `json:"reservas"`
	Holds        map[string]*Hold    `json:"holds"`
	SeqCaronaId  int                 `json:"seq_carona_id"`
	SeqReservaId int                 `json:"seq_reserva_id"`
	SeqHoldId    int                 `json:"seq_hold_id"`
}

// SalvarJSON persiste o estado completo em arquivo JSON de forma atômica e segura.
// Utiliza escrita em arquivo temporário com Sync() seguido de os.Rename para prevenção contra corrupção.
func (g *GerenciadorEstado) SalvarJSON(caminhoArquivo string) error {
	caminhoArquivo = strings.TrimSpace(caminhoArquivo)
	if caminhoArquivo == "" {
		return errors.New("caminho de arquivo para persistencia JSON nao pode ser vazio")
	}

	g.mu.RLock()

	// Gera cópia profunda para serialização segura fora de contenção
	usuariosCopia := make(map[string]*Usuario, len(g.usuarios))
	for k, v := range g.usuarios {
		u := *v
		usuariosCopia[k] = &u
	}

	caronasCopia := make(map[string]*Carona, len(g.caronas))
	for k, v := range g.caronas {
		vagas := make(map[string]int, len(v.VagasPorTrecho))
		for vk, vv := range v.VagasPorTrecho {
			vagas[vk] = vv
		}
		pass := make(map[string][]string, len(v.PassageirosPorTrecho))
		for pk, pv := range v.PassageirosPorTrecho {
			passCop := make([]string, len(pv))
			copy(passCop, pv)
			pass[pk] = passCop
		}
		rotaCop := make([]string, len(v.Rota))
		copy(rotaCop, v.Rota)

		caronasCopia[k] = &Carona{
			ID:                   v.ID,
			EmpresaID:            v.EmpresaID,
			MotoristaLogin:       v.MotoristaLogin,
			Rota:                 rotaCop,
			Data:                 v.Data,
			Horario:              v.Horario,
			HorarioChegada:       v.HorarioChegada,
			AssentosTotais:       v.AssentosTotais,
			PrecoPorTrecho:       v.PrecoPorTrecho,
			VagasPorTrecho:       vagas,
			PassageirosPorTrecho: pass,
			Ativa:                v.Ativa,
		}
	}

	reservasCopia := make(map[string]*Reserva, len(g.reservas))
	for k, v := range g.reservas {
		trechosCop := make([]ItemTrechoReserva, len(v.Trechos))
		copy(trechosCop, v.Trechos)
		reservasCopia[k] = &Reserva{
			ID:              v.ID,
			PassageiroLogin: v.PassageiroLogin,
			Trechos:         trechosCop,
			PrecoTotal:      v.PrecoTotal,
			Data:            v.Data,
			Status:          v.Status,
			CriadoEm:        v.CriadoEm,
		}
	}

	holdsCopia := make(map[string]*Hold, len(g.holds))
	for k, v := range g.holds {
		trechosCop := make([]ItemTrechoReserva, len(v.Trechos))
		copy(trechosCop, v.Trechos)
		holdsCopia[k] = &Hold{
			ID:               v.ID,
			PassageiroLogin:  v.PassageiroLogin,
			Trechos:          trechosCop,
			PrecoTotal:       v.PrecoTotal,
			Data:             v.Data,
			TimestampLamport: v.TimestampLamport,
			Status:           v.Status,
			CriadoEm:         v.CriadoEm,
			ExpiraEm:         v.ExpiraEm,
		}
	}

	snapshot := &SnapshotEstado{
		Versao:       VersaoSnapshot,
		SalvoEm:      time.Now().UTC(),
		Usuarios:     usuariosCopia,
		Caronas:      caronasCopia,
		Reservas:     reservasCopia,
		Holds:        holdsCopia,
		SeqCaronaId:  g.seqCaronaId,
		SeqReservaId: g.seqReservaId,
		SeqHoldId:    g.seqHoldId,
	}

	g.mu.RUnlock()

	dados, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("falha ao serializar snapshot de estado: %w", err)
	}

	// Garante a existência do diretório pai
	dir := filepath.Dir(caminhoArquivo)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("falha ao criar diretorio para persistencia (%s): %w", dir, err)
		}
	}

	caminhoTmp := caminhoArquivo + ".tmp"
	arquivoTmp, err := os.OpenFile(caminhoTmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("falha ao abrir arquivo temporario (%s): %w", caminhoTmp, err)
	}

	if _, err := arquivoTmp.Write(dados); err != nil {
		arquivoTmp.Close()
		_ = os.Remove(caminhoTmp)
		return fmt.Errorf("falha ao gravar snapshot: %w", err)
	}

	if err := arquivoTmp.Sync(); err != nil {
		arquivoTmp.Close()
		_ = os.Remove(caminhoTmp)
		return fmt.Errorf("falha no fsync do arquivo snapshot: %w", err)
	}

	if err := arquivoTmp.Close(); err != nil {
		_ = os.Remove(caminhoTmp)
		return fmt.Errorf("falha ao fechar arquivo temporario: %w", err)
	}

	// Substituição atômica via os.Rename
	if err := os.Rename(caminhoTmp, caminhoArquivo); err != nil {
		_ = os.Remove(caminhoTmp)
		return fmt.Errorf("falha na substituicao atomica do snapshot: %w", err)
	}

	return nil
}

// CarregarJSON restaura o estado completo a partir de um snapshot JSON previamente salvo.
func (g *GerenciadorEstado) CarregarJSON(caminhoArquivo string) error {
	caminhoArquivo = strings.TrimSpace(caminhoArquivo)
	if caminhoArquivo == "" {
		return errors.New("caminho de arquivo para leitura JSON nao pode ser vazio")
	}

	dados, err := os.ReadFile(caminhoArquivo)
	if err != nil {
		return fmt.Errorf("falha ao ler arquivo de snapshot (%s): %w", caminhoArquivo, err)
	}

	var snapshot SnapshotEstado
	if err := json.Unmarshal(dados, &snapshot); err != nil {
		return fmt.Errorf("snapshot corrompido ou formato JSON invalido: %w", err)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// Inicializa mapas caso o snapshot venha nulo
	if snapshot.Usuarios == nil {
		snapshot.Usuarios = make(map[string]*Usuario)
	}
	if snapshot.Caronas == nil {
		snapshot.Caronas = make(map[string]*Carona)
	}
	if snapshot.Reservas == nil {
		snapshot.Reservas = make(map[string]*Reserva)
	}
	if snapshot.Holds == nil {
		snapshot.Holds = make(map[string]*Hold)
	}

	g.usuarios = snapshot.Usuarios
	g.caronas = snapshot.Caronas
	g.reservas = snapshot.Reservas
	g.holds = snapshot.Holds
	g.seqCaronaId = snapshot.SeqCaronaId
	g.seqReservaId = snapshot.SeqReservaId
	g.seqHoldId = snapshot.SeqHoldId

	// Limpa quaisquer holds cujo TTL tenha expirado durante o período em que o sistema esteve desligado
	g.expirarHoldsOrfaosTravado()

	return nil
}

// NovoGerenciadorEstadoComPersistencia cria uma instância do gerenciador carregando dados prévios se o arquivo existir.
func NovoGerenciadorEstadoComPersistencia(caminhoArquivo string) (*GerenciadorEstado, error) {
	g := NovoGerenciadorEstado()
	g.caminhoPersistencia = caminhoArquivo

	if caminhoArquivo != "" {
		if _, err := os.Stat(caminhoArquivo); err == nil {
			if err := g.CarregarJSON(caminhoArquivo); err != nil {
				return nil, fmt.Errorf("erro ao carregar persistencia inicial: %w", err)
			}
		}
	}

	return g, nil
}
