# VaiJunto - Central de Caronas Compartilhadas

> **TEC502 - Concorrência e Conectividade (Problema 1)**  
> Sistema de intermediação de caronas intermunicipais com controle estrito de concorrência, comunicação TCP pura e prevenção absoluta de overbooking.

---

## Visão Geral do Projeto

O **VaiJunto** é um sistema centralizado desenvolvido em **Go (Golang)** focado na gestão de caronas intermunicipais entre Salvador, Feira de Santana e região (Portal do Sertão, Recôncavo e RMS).

O sistema lida com o desafio de múltiplos usuários (motoristas e passageiros) realizando acessos simultâneos — cadastrando usuários, publicando caronas, consultando itinerários e efetuando reservas concorrentes.

---

## Visão Detalhada da Arquitetura do Sistema

A arquitetura do **VaiJunto** foi projetada visando alto desempenho, desacoplamento de camadas, conectividade de baixo nível e controle absoluto de concorrência em memória.

### Diagrama da Arquitetura de Componentes

```text
 ┌──────────────────────┐         ┌──────────────────────┐
 │  Cliente Motorista   │         │  Cliente Passageiro  │
 │   (cmd/motorista)    │         │   (cmd/passageiro)   │
 └──────────┬───────────┘         └──────────┬───────────┘
            │                                │
            │ Conexão TCP                    │ Conexão TCP
            │ (JSON Stream + '\n')           │ (JSON Stream + '\n')
            ▼                                ▼
 ┌───────────────────────────────────────────────────────┐
 │               Servidor Central (cmd/servidor)          │
 │                                                       │
 │  ┌─────────────────────────────────────────────────┐  │
 │  │      Listener TCP (net.Listen - Porta 8080)     │  │
 │  └────────────────────────┬────────────────────────┘  │
 │                           │                           │
 │                           ▼ Dispara per-connection    │
 │  ┌─────────────────────────────────────────────────┐  │
 │  │        Goroutine Cliente Worker (goroutine)     │  │
 │  └────────────────────────┬────────────────────────┘  │
 │                           │                           │
 └───────────────────────────┼───────────────────────────┘
                             │
                             ▼ Invoca Métodos do Domínio
 ┌───────────────────────────────────────────────────────┐
 │         Gerenciador de Estado (pkg/estado)            │
 │                                                       │
 │  ┌─────────────────────────────────────────────────┐  │
 │  │     Exclusão Mútua: sync.RWMutex (Lock/RLock)   │  │
 │  └────────────────────────┬────────────────────────┘  │
 │                           │                           │
 │        ┌──────────────────┼──────────────────┐        │
 │        ▼                  ▼                  ▼        │
 │  ┌───────────┐      ┌───────────┐      ┌───────────┐  │
 │  │  Usuários │      │  Caronas  │      │  Reservas │  │
 │  │  (map)    │      │  (map)    │      │  (map)    │  │
 │  └───────────┘      └───────────┘      └───────────┘  │
 └───────────────────────────────────────────────────────┘
```

---

## Decisões de Arquitetura de Software & Engenharia

### 1. Camada de Rede: Sockets TCP Nativo vs HTTP/REST
* **Escolha:** Utilização de sockets **TCP puros** (`net.Conn`) em substituição a HTTP/REST.
* **Justificativa Técnica:** O protocolo HTTP impõe alto *overhead* (cabeçalhos verbosos, handshakes repetidos e overhead de parse). O TCP direto garante menor latência, respostas imediatas e consumo mínimo de memória no servidor.
* **Resolução do Problema de TCP Framing:** O TCP é um protocolo orientado a stream de bytes e não a mensagens separadas. Para evitar fragmentação ou junção de pacotes (*framing issue*), implementamos em `pkg/protocolo/io.go` uma delimitação por **Line-Feed (`\n`)**.
* **Stream Buffer Optimizations:** A leitura é realizada utilizando `bufio.Scanner` com buffer configurável de 64KB até 1MB por pacote, evitando estouro de memória em buscas extensas.

### 2. Camada de Concorrência: Modelo Worker Goroutines + `sync.RWMutex`
* **Thread-per-Connection:** Para cada cliente aceito no socket (`listener.Accept()`), o servidor dispara uma `goroutine` independente. Isso permite escalabilidade para milhares de conexões simultâneas com baixo uso de RAM (uma goroutine consome ~2KB inicial).
* **Padrão Leitor-Escritor (`sync.RWMutex`):**
  * **Trava de Leitura (`RLock`):** Utilizada em operações read-only como `BuscarItinerarios` e `ValidarToken`. Permite que múltiplos passageiros consultem o inventário simultaneamente sem causar gargalos (*zero-contention reads*).
  * **Trava de Escrita (`Lock`):** Utilizada em operações de mutação de estado como `ReservarItinerario`, `PublicarCarona`, `CadastrarUsuario` e `CancelarReserva`.

### 3. Algoritmo de Reserva Atômica (**Check-Then-Act em 2 Fases**)
Para erradicar a ocorrência de **Overbooking** sob concorrência massiva, a reserva de viagens é tratada de forma transacional e atômica em memória:
* **Fase 1 (Check):** Sob `Lock()`, o algoritmo itera sobre todos os sub-trechos da rota solicitada. Se qualquer segmento possuir `vagas <= 0` ou carona inativa, a transação é abortada e nada é alterado.
* **Fase 2 (Act):** Somente se **100% dos trechos forem validados com sucesso**, o sistema decrementa o inventário de vagas (`VagasPorTrecho--`) e gera o registro da reserva de forma indivisível.

### 4. Camada de Domínio: Desmembramento de Rotas e Grafo Regional
* **Modelo de Sub-Trechos Dinâmicos:** Em vez de alocar assentos na carona global, a estrutura de dados [Carona](file:///home/rivasuzeda/Faculdade/MI%20-%20Concorr%C3%AAncia%20e%20conectividade/pkg/estado/carona.go) mapeia a ocupação através de `VagasPorTrecho map[string]int` e `PassageirosPorTrecho map[string][]string`.
* **Benefício de Negócio:** Maximiza a taxa de ocupação do veículo. O mesmo assento físico pode ser reutilizado por passageiros diferentes em segmentos distintos da mesma rota (ex: Passageiro A no trecho *Salvador->Amélia* e Passageiro B no trecho *Amélia->Feira*).
* **Catálogo de Cidades Regionais:** Restringe a validação e normalização de rotas às 12 cidades oficiais do Portal do Sertão, Recôncavo e RMS, prevenindo dados inválidos no sistema.

---

## Pacotes da Solução (`pkg/` e `cmd/`)

A aplicação segue o padrão oficial do ecossistema Go (*Standard Go Project Layout*), dividida nos seguintes pacotes:

### 1. Pacotes da Aplicação Executável (`cmd/`)
* `cmd/servidor`: Ponto de entrada do Servidor Central TCP (:8080). Gerencia o listener de rede, encerramento gracioso (*graceful shutdown*) e despacho de goroutines por cliente.
* `cmd/motorista`: Cliente CLI interativo para motoristas (cadastrar, autenticar, publicar carona passo a passo estilo Uber, listar e cancelar caronas).
* `cmd/passageiro`: Cliente CLI interativo para passageiros (cadastrar, autenticar, buscar itinerários regionais, realizar reserva atômica, consultar histórico e cancelar reserva).
* `cmd/teste_carga`: Robô simulador de alta concorrência e auditor de condições de corrida (*race conditions*).

### 2. Pacotes de Biblioteca e Domínio (`pkg/`)
* `pkg/estado`: Gerenciador de estado em memória thread-safe (`GerenciadorEstado`). Responsável pela exclusão mútua (`sync.RWMutex`), mapa de assentos por sub-trechos, tokens de sessão e algoritmo de reserva atômica (**Check-Then-Act**).
* `pkg/protocolo`: Definição de mensagens JSON da camada de aplicação (`Requisicao`, `Resposta`, DTOs), constantes de ações e utilitários de I/O em sockets TCP (`EnviarMensagem`, `LerMensagem` com `bufio.Scanner` delimitado por `\n`).

---

## Estrutura do Projeto

```text
.
├── Dockerfile                  # Containerização multi-stage do servidor e ambiente
├── docker-compose.yml          # Orquestração dos containers Docker
├── go.mod                      # Módulo Go do projeto
├── cmd/
│   ├── servidor/               # Servidor Central TCP (:8080)
│   ├── motorista/              # Cliente CLI para Motoristas
│   ├── passageiro/             # Cliente CLI para Passageiros
│   └── teste_carga/            # Simulator/Auditor de Concorrência Massiva
└── pkg/
    ├── estado/                 # Regras de negócio e gerenciador de estado thread-safe
    │   ├── carona.go           # Structs de Carona e Reserva
    │   ├── cidades.go          # Validação e catálogo regional de 12 cidades
    │   ├── gerenciador.go      # Gerenciador de estado com Mutex (Lock/RLock)
    │   └── usuario.go          # Structs de Usuário e Sessão
    └── protocolo/              # DTOs, constantes e comunicação TCP/JSON
        ├── io.go               # Escrita/Leitura de sockets TCP delimitados por \n
        └── mensagens.go        # Structs Requisicao e Resposta JSON
```

---

## Como Usar (Guia dos Clientes)

### Para Motoristas (`cmd/motorista`):
1. **Cadastrar/Login:** Cadastre-se escolhendo o papel de motorista e efetue o login para receber um token de sessão.
2. **Publicar Carona (Estilo Uber):**
   * Selecione a cidade de **Origem** (ex: *Salvador*).
   * Selecione o **Destino Final** (ex: *Feira de Santana*).
   * Adicione paradas intermediárias (ex: *Simões Filho*, *Amélia Rodrigues*).
   * Informe a data (AAAA-MM-DD), horários, assentos livres e preço por trecho.
3. **Listar e Cancelar:** Consulte suas caronas ativas e o status de ocupação de cada sub-trecho ou cancele uma carona se necessário.

### Para Passageiros (`cmd/passageiro`):
1. **Cadastrar/Login:** Cadastre-se escolhendo o papel de passageiro e efetue o login.
2. **Buscar Itinerários:** Informe a cidade de partida, destino e data. O sistema trará todas as opções de itinerários diretos ou combinados ordenados por menor preço e horário.
3. **Reservar:** Selecione o itinerário desejado para efetuar a reserva atômica.
4. **Listar e Cancelar Reservas:** Consulte seu histórico de reservas ou cancele uma reserva devolvendo as vagas ao inventário.

---

## Como Executar

### Pré-requisitos
* **Go 1.20+** instalado OU **Docker / Docker Compose**.

### Opção A: Executando Localmente (Go)

1. **Inicie o Servidor Central:**
   ```bash
   go run ./cmd/servidor -porta 8080
   ```

2. **Em um novo terminal, inicie o Cliente Motorista:**
   ```bash
   go run ./cmd/motorista -servidor localhost:8080
   ```

3. **Em outro terminal, inicie o Cliente Passageiro:**
   ```bash
   go run ./cmd/passageiro -servidor localhost:8080
   ```

4. **Executar o Teste de Carga e Concorrência Massiva:**
   ```bash
   go run ./cmd/teste_carga -servidor localhost:8080 -clientes 30 -vagas 5
   ```

---

### Opção B: Executando via Docker Compose

1. **Subir apenas o Servidor:**
   ```bash
   docker-compose up --build servidor
   ```

2. **Rodar o Teste de Carga via Docker:**
   ```bash
   docker-compose --profile teste up --build
   ```

---

## Dockerfile e Configuração de Ambiente

O projeto utiliza **Multi-Stage Build** no `Dockerfile`:
1. **Estágio 1 (Builder):** Utiliza a imagem oficial `golang:1.22-alpine`, configura o diretório `/app`, copia os módulos e compila estaticamente todos os executáveis Go sem dependências externas (`CGO_ENABLED=0 GOOS=linux`).
2. **Estágio 2 (Runtime):** Utiliza a imagem minimalista `alpine:latest`, expõe a porta TCP `8080` e executa o binário estático do servidor `/app/bin/servidor`.

---

## Teste de Carga e Auditoria de Concorrência

O módulo `cmd/teste_carga` simula o cenário crítico de **disputa simultânea por vagas**:
* Publica uma carona de teste com número limitado de assentos (ex: 5 vagas).
* Dispara `N` goroutines clientes (ex: 30 clientes) que tentam realizar o fluxo completo de cadastro, login, busca e reserva **exatamente no mesmo instante**.
* Mede a latência e audita o resultado, garantindo que **exatamente 5 vagas são vendidas** e as outras **25 requisições são recusadas graciosamente**, confirmando a ausência de condições de corrida (*race conditions*).

---

## Guia de Apresentação (Roteiro de Fala para Avaliação)

1. **Abertura & Contexto:** Apresente o **VaiJunto** como um servidor centralizado em Go que atende conexões TCP puras usando payloads JSON delimitados por `\n`.
2. **Decisão de Negócio (Modelo de Sub-Trechos):** Mostre que rotas são fragmentadas (ex: `Salvador -> Amélia Rodrigues -> Feira de Santana`), alocando vagas por segmento (`VagasPorTrecho`).
3. **Arquitetura de Concorrência (`Goroutines` + `sync.RWMutex`):** Explique o modelo *thread-per-connection* com goroutines e a divisão entre **Leitura Concorrente (`RLock`)** e **Escrita Exclusiva (`Lock`)**.
4. **Algoritmo de Reserva Atômica (**Check-Then-Act**):** Explique que sob a trava de escrita, o sistema verifica a disponibilidade em todos os trechos (Fase 1: Check) antes de debitar as vagas (Fase 2: Act), garantindo **0% Overbooking**.
5. **Demonstração Prática:** Execute o script de teste de carga em tempo real:
   ```bash
   go run ./cmd/teste_carga -servidor localhost:8080 -clientes 30 -vagas 5
   ```
