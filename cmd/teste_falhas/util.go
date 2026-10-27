package main

import (
	"bufio"
	"net"

	"vaijunto/pkg/protocolo"
)

// scannerT e um alias local para o tipo de scanner retornado por protocolo.CriarScanner,
// usado apenas para deixar as assinaturas de funcoes auxiliares mais legiveis neste pacote.
type scannerT = bufio.Scanner

// enviarRecv envia uma requisicao e aguarda a resposta correspondente na mesma conexao TCP,
// retornando erro tanto em falhas de rede quanto quando o servidor responde com StatusErro.
func enviarRecv(conn net.Conn, scanner *scannerT, req protocolo.Requisicao) (protocolo.Resposta, error) {
	var resp protocolo.Resposta

	if err := protocolo.EnviarMensagem(conn, req); err != nil {
		return resp, err
	}
	if err := protocolo.LerMensagem(scanner, &resp); err != nil {
		return resp, err
	}
	if resp.Status != protocolo.StatusOK {
		return resp, errRespostaServidor{acao: req.Acao, motivo: resp.Erro}
	}
	return resp, nil
}

// errRespostaServidor representa uma resposta de dominio recusada (StatusErro) pelo servidor.
type errRespostaServidor struct {
	acao   string
	motivo string
}

func (e errRespostaServidor) Error() string {
	return e.acao + " recusado pelo servidor: " + e.motivo
}
