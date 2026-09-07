package estado

import (
	"errors"
	"strings"
)

// CidadesRegionais limita o catálogo inicial para o Portal do Sertão, Recôncavo e RMS
var CidadesRegionais = []string{
	"Salvador",
	"Simões Filho",
	"Candeias",
	"Santo Amaro",
	"Amélia Rodrigues",
	"Conceição do Jacuípe",
	"Feira de Santana",
	"Santa Bárbara",
	"Serrinha",
	"Alagoinhas",
	"Cachoeira",
	"Santo Antônio de Jesus",
}

// ObterCidadesRegionais retorna a lista completa de cidades atendidas
func ObterCidadesRegionais() []string {
	cidades := make([]string, len(CidadesRegionais))
	copy(cidades, CidadesRegionais)
	return cidades
}

// NormalizarCidade ajusta a grafia oficial da cidade (case-insensitive)
func NormalizarCidade(cidade string) (string, bool) {
	cidadeNorm := strings.ToLower(strings.TrimSpace(cidade))
	for _, c := range CidadesRegionais {
		if strings.ToLower(c) == cidadeNorm {
			return c, true
		}
	}
	return cidade, false
}

// ValidarRota garante que a rota possui pelo menos 2 paradas válidas e sem duplicações consecutivas
func ValidarRota(rota []string) ([]string, error) {
	if len(rota) < 2 {
		return nil, errors.New("a rota deve conter no mínimo 2 cidades (origem e destino)")
	}

	rotaNormalizada := make([]string, 0, len(rota))
	for i, c := range rota {
		norm, ok := NormalizarCidade(c)
		if !ok {
			return nil, errors.New("cidade '" + c + "' não pertence ao catálogo regional atendido")
		}
		if i > 0 && norm == rotaNormalizada[len(rotaNormalizada)-1] {
			return nil, errors.New("a rota contém paradas consecutivas duplicadas: '" + norm + "'")
		}
		rotaNormalizada = append(rotaNormalizada, norm)
	}

	return rotaNormalizada, nil
}