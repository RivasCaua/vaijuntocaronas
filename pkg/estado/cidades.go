package estado

import (
	"errors"
	"strings"
)

// CidadesRegionais representa o catálogo de cidades suportadas na região de Feira de Santana e Salvador
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

// ObterCidadesRegionais retorna uma cópia da lista de cidades atendidas
func ObterCidadesRegionais() []string {
	cidades := make([]string, len(CidadesRegionais))
	copy(cidades, CidadesRegionais)
	return cidades
}

// NormalizarCidade valida e ajusta a grafia da cidade de acordo com o catálogo regional
func NormalizarCidade(cidade string) (string, bool) {
	cidadeNorm := strings.ToLower(strings.TrimSpace(cidade))
	for _, c := range CidadesRegionais {
		if strings.ToLower(c) == cidadeNorm {
			return c, true
		}
	}
	return cidade, false
}

// ValidarRota verifica se a sequência de paradas possui pelo menos 2 cidades válidas e sem duplicidade consecutiva
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
