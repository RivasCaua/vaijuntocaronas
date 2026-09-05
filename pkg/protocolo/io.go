package protocolo

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
)

// EnviarMensagem converte uma estrutura qualquer em JSON e escreve em socket TCP
// Finalizando com uma quebra de linha (\n) para indicar o fim da mensagem.
func EnviarMensagem(conn net.Conn, msg interface{}) error {
	if conn == nil {
		return errors.New("Conexão de rede nula")
	}

	// Converte a Struct Go para bytes no formato JSON
	dadosJSON, err := json.Marshal(msg)
	if err != nil{
		return fmt.Errorf("Falha ao serializar mensagem para JSON: %w", err)
	}

	// Adiciona o delimitador \n no final
	dadosJSON = append(dadosJSON, '\n')

	// Escreve no socket TCP
	_, err = conn.Write(dadosJSON)
	if err != nil {
		return fmt.Errorf("Falha ao enviar dados pelo socker TCP: %w", err)
	}

	return nil
}

// LerMensagem aguarda a próxima linha vinda do socket e faz o parse do JSON
// para dentro da struct apontada por dest (usando ponteiro &)
func LerMensagem(scanner *bufio.Scanner, dest interface{}) error {
	if scanner == nil {
		return errors.New("Scanner nulo")
	}

	// scanner.Scan() bloqueia até que uma linha inteira chegue 
	if !scanner.Scan() {
		// Se o Scan() der false, ou a conexão der um erro
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("Erro na leitura do Socket: %w", err)
		}
		return errors.New("Conexão falha, encerrado (EOF)")
	}

	// Obtém os bytes da linha lida
	bytesLinha := scanner.Bytes()
	if len(bytesLinha) == 0 {
		return errors.New("Mensagem vazia)")
	}

	// Converte os bytes JSON de volta para a struct Go
	err := json.Unmarshal(bytesLinha, dest)
	if err != nil {
		return fmt.Errorf("JSON mal formatado: %w", err)
	}

	return nil
}

// CriarScanner cria um leitor de linhas bufferizando sobre a conexão TCP
func CriarScanner(conn net.Conn) *bufio.Scanner {
	scanner := bufio.NewScanner(conn)
	// Configura buffer inicial de 64kb e capacidade máxima de 1MB por semana
	buf := make([]byte, 64*1024) 
	scanner.Buffer(buf, 1024*1024)
	return scanner
}