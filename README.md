# VaiJunto - Consórcio de Caronas Compartilhadas

> **TEC502 - Concorrência e Conectividade (Problema 1)**
> Consórcio de empresas de transporte intermunicipal independentes, cada uma operando seu próprio
> servidor com controle estrito de concorrência, comunicação TCP pura com clientes, replicação via
> Write-Ahead Log e coordenação distribuída (Scatter-Gather, Two-Phase Commit, heartbeat) via REST
> entre empresas.

---

## Visão Geral do Projeto

O **VaiJunto** nasceu como um servidor centralizado em **Go (Golang)** para gestão de caronas
intermunicipais entre Salvador, Feira de Santana e região (Portal do Sertão, Recôncavo e RMS), e
evoluiu para um **consórcio multi-empresa**: cada empresa de transporte roda sua própria instância
independente do mesmo binário (`cmd/servidor`), com seu próprio estado em memória e seu próprio
Write-Ahead Log em disco, cooperando com as demais via uma API REST inter-servidores
(`pkg/consorcio`) para federar buscas de itinerários e coordenar reservas que atravessam mais de uma
empresa, sem jamais permitir overbooking - nem dentro de uma empresa, nem entre elas.

O sistema lida com múltiplos usuários (motoristas e passageiros) realizando acessos simultâneos -
cadastrando-se, publicando caronas, consultando itinerários e efetuando reservas concorrentes - e
com a possibilidade real de uma dessas empresas cair e se recuperar sem perder dados nem corromper o
estado das demais.

Para a especificação completa da API REST do consórcio (endpoints, DTOs, diagramas de sequência dos
fluxos distribuídos) veja **[`docs/api_consorcio.md`](docs/api_consorcio.md)**.

---

## Visão Detalhada da Arquitetura do Sistema

Cada empresa do consórcio expõe **dois listeners simultâneos**: um TCP puro para seus próprios
motoristas/passageiros, e um REST para conversar com as empresas parceiras.

### Diagrama da Arquitetura do Consórcio

```text
 ┌──────────────────────┐         ┌──────────────────────┐
 │  Cliente Motorista   │         │  Cliente Passageiro  │
 │   (cmd/motorista)    │         │   (cmd/passageiro)   │
 └──────────┬───────────┘         └──────────┬───────────┘
            │ TCP + JSON ('\n')              │ TCP + JSON ('\n')
            ▼                                ▼
 ┌───────────────────────────────────────────────────────┐
 │        nodo-salvador (empresa RMS) - cmd/servidor      │
 │  ┌─────────────┐  ┌─────────────┐  ┌────────────────┐ │
 │  │ Listener TCP│  │Listener REST│  │ CoordenadorDist.│ │
 │  │   :8080     │  │   :9090     │  │ (2PC/Scatter/HB)│ │
 │  └──────┬──────┘  └──────┬──────┘  └────────┬───────┘ │
 │         └────────────────┴──────────────────┘         │
 │                          ▼                             │
 │         GerenciadorEstado (sync.RWMutex)                │
 │         usuários · caronas · reservas · holds           │
 │                          │                              │
 │                          ▼                              │
 │              WAL (append-only + fsync + CRC32)          │
 └───────────────────────────┬───────────────────────────┘
                             │ REST: heartbeat, scatter-gather,
                             │ 2PC (prepare/commit/abort), sync WAL
             ┌───────────────┼────────────────┐
             ▼                                ▼
 ┌─────────────────────┐          ┌─────────────────────┐
 │ nodo-feira (Portal   │◄────────►│ nodo-reconcavo       │
 │ do Sertão)           │  REST    │ (Recôncavo)          │
 │ mesma estrutura ↑    │          │ mesma estrutura ↑    │
 └─────────────────────┘          └─────────────────────┘
```

Dentro de cada nó, o modelo de concorrência do servidor original permanece intacto:

```text
 ┌───────────────────────────────────────────────────────┐
 │         Gerenciador de Estado (pkg/estado)            │
 │  ┌─────────────────────────────────────────────────┐  │
 │  │     Exclusão Mútua: sync.RWMutex (Lock/RLock)   │  │
 │  └────────────────────────┬────────────────────────┘  │
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

### 1. Camada de Rede com Clientes: Sockets TCP Nativo vs HTTP/REST
* **Escolha:** Sockets **TCP puros** (`net.Conn`) para motoristas/passageiros, reservando HTTP/REST
  apenas para a comunicação **entre empresas** do consórcio (onde o overhead de um protocolo
  padronizado e auto-descritivo compensa, por ser tráfego inter-serviço, não de alta frequência por
  cliente final).
* **Framing TCP:** `pkg/protocolo/io.go` delimita mensagens por **Line-Feed (`\n`)**, lidas com
  `bufio.Scanner` (buffer de 64KB a 1MB).

### 2. Camada de Concorrência Local: Goroutines + `sync.RWMutex`
* **Thread-per-Connection:** cada cliente aceito dispara uma `goroutine` independente.
* **Leitor-Escritor:** `RLock` em `BuscarItinerarios`/`ValidarToken`; `Lock` em `ReservarItinerario`,
  `PublicarCarona`, `CadastrarUsuario`, `CancelarReserva` e nas operações de `Hold` (Fase 1/2 do 2PC).

### 3. Algoritmo de Reserva Atômica: Two-Phase Commit com Leases (Holds)
Toda reserva - local ou entre empresas - passa pelo mesmo protocolo de duas fases
(`CoordenadorDistribuido.ExecutarReserva2PC`):
* **Fase 1 (Prepare):** para cada sub-trecho, sob `Lock()`, verifica disponibilidade e, se livre,
  debita provisoriamente a vaga criando um `Hold` com TTL (lease). Se dois passageiros disputam a
  última vaga ao mesmo tempo, o desempate é feito pelo **relógio de Lamport** (menor timestamp lógico
  vence; empate quebrado pelo ID do nó).
* **Fase 2 (Commit/Abort):** só confirmada se **todos** os participantes (locais e remotos)
  votarem SIM; qualquer recusa ou falha de rede aborta a transação inteira e **compensa**
  automaticamente devolvendo as vagas já debitadas nos participantes que haviam votado SIM - sem
  vazamento de hold, mesmo com um nó caindo no meio da transação (validado em `cmd/teste_falhas`).
* Participantes inteiramente locais resolvem as duas fases em memória, sem round-trip de rede; apenas
  trechos de uma empresa parceira acionam o REST (`/api/v1/2pc/{prepare,commit,abort}`).

### 4. Durabilidade: Write-Ahead Log com `fsync` e CRC32
* Toda mutação de domínio confirmada ao cliente só é confirmada **depois** de persistida com
  `fsync` no WAL local (`pkg/consorcio/wal.go`) - nenhuma resposta de sucesso sai sem durabilidade
  garantida contra queda do processo.
* Cada linha do log carrega um checksum CRC32; corrupção de disco é detectada explicitamente no boot.
* **Recuperação pós-queda:** ao reiniciar, o servidor relê o WAL do início, reaplica cada operação
  (idempotente) na ordem exata do log antes de abrir qualquer porta a clientes, e retoma o relógio de
  Lamport do maior timestamp já observado - nunca "volta no tempo" lógico após um crash.
* Fases transitórias do 2PC (`2PC_PREPARE`, `2PC_ABORT`) são deliberadamente descartadas no replay:
  um prepare sem commit correspondente vira aborto implícito (política de segurança contra reservas
  "fantasma").

### 5. Coordenação Distribuída entre Empresas (`pkg/consorcio`)
* **Relógio de Lamport:** cada nó mantém um relógio lógico (`Tick`/`Atualizar`) usado para ordenar
  eventos e desempatar concorrência entre empresas distintas.
* **Busca combinada entre empresas:** o inventário bruto de sub-trechos do estado local e de todos os
  parceiros é coletado em paralelo (Scatter) e montado em um grafo de cidades, sobre o qual uma busca
  em profundidade encontra itinerários que **combinam caronas de empresas diferentes** (ex.: Salvador
  → Feira numa empresa, Feira → Vitória da Conquista noutra, Vitória da Conquista → Montes Claros numa
  terceira) - tolerando falha parcial de qualquer nó indisponível.
* **Heartbeat automático:** cada nó envia heartbeats periódicos a seus parceiros (`-heartbeat-
  intervalo` em `cmd/servidor`, padrão 2s) e marca o status de cada um como `ONLINE`, `SUSPEITO`
  (1 falha) ou `OFFLINE` (3 falhas), refletido em `/api/v1/cluster/info`.
* Ver **[`docs/api_consorcio.md`](docs/api_consorcio.md)** para a especificação completa dos
  endpoints REST e diagramas de sequência de cada fluxo.

### 6. Camada de Domínio: Desmembramento de Rotas e Grafo Regional
* **Sub-Trechos Dinâmicos:** a estrutura [Carona](pkg/estado/carona.go) mapeia ocupação por
  `VagasPorTrecho map[string]int`, permitindo que o mesmo assento físico seja reaproveitado por
  passageiros diferentes em segmentos distintos da mesma rota.
* **Catálogo de Cidades Regionais:** 12 cidades oficiais do Portal do Sertão, Recôncavo e RMS.

---

## Pacotes da Solução (`pkg/` e `cmd/`)

### 1. Pacotes da Aplicação Executável (`cmd/`)
* `cmd/servidor`: Ponto de entrada de uma instância/empresa do consórcio. Abre o listener TCP de
  clientes, o listener REST inter-empresas, a rotina automática de heartbeat e executa a recuperação
  via WAL no boot.
* `cmd/motorista`: Cliente CLI interativo para motoristas.
* `cmd/passageiro`: Cliente CLI interativo para passageiros.
* `cmd/teste_carga`: Robô de auditoria de condição de corrida - dispara N clientes contra UMA única
  carona com poucas vagas e confirma zero overbooking.
* `cmd/simulador`: Simulador autônomo (headless) de carga sustentada - dezenas de passageiros
  virtuais buscando/reservando/cancelando continuamente, com relatório de throughput e latência
  (p50/p95/p99) por operação.
* `cmd/teste_falhas`: Robô de caos - sobe 3 processos reais do consórcio, mata um abruptamente
  (`SIGKILL`) em pleno funcionamento e audita detecção de falha, isolamento dos sobreviventes,
  compensação do 2PC e recuperação via WAL.

### 2. Pacotes de Biblioteca e Domínio (`pkg/`)
* `pkg/estado`: Gerenciador de estado em memória thread-safe. Usuários, caronas, reservas, holds
  (leases do 2PC), algoritmo de reserva atômica, persistência/recuperação de domínio.
* `pkg/protocolo`: Mensagens JSON da camada de aplicação TCP (`Requisicao`, `Resposta`, DTOs),
  relógio de Lamport e utilitários de I/O em sockets.
* `pkg/consorcio`: Camada de coordenação distribuída entre empresas - `CoordenadorDistribuido`
  (Scatter-Gather, 2PC, heartbeat), `ServidorREST` (API HTTP inter-nós), `ClienteREST`, `WAL` e os
  DTOs de rede do consórcio. Ver `docs/api_consorcio.md` para a especificação completa.

---

## Estrutura do Projeto

```text
.
├── Dockerfile                  # Containerização multi-stage de todos os binarios
├── docker-compose.yml          # Topologia de 3 empresas do consorcio + clientes + testes
├── go.mod                      # Módulo Go do projeto
├── docs/
│   └── api_consorcio.md        # Especificação da API REST, DTOs e diagramas de sequência
├── cmd/
│   ├── servidor/                # Servidor de uma empresa (TCP + REST + WAL + coordenador)
│   ├── motorista/                # Cliente CLI para Motoristas
│   ├── passageiro/               # Cliente CLI para Passageiros
│   ├── teste_carga/              # Auditor de condição de corrida (overbooking)
│   ├── simulador/                 # Simulador autônomo de carga sustentada
│   └── teste_falhas/              # Robô de caos / queda de nós
└── pkg/
    ├── estado/                 # Regras de negócio e gerenciador de estado thread-safe
    │   ├── carona.go           # Structs de Carona e Reserva
    │   ├── cidades.go          # Validação e catálogo regional de 12 cidades
    │   ├── gerenciador.go      # Gerenciador de estado com Mutex (Lock/RLock)
    │   ├── hold.go             # Leases/Holds do Two-Phase Commit
    │   ├── persistencia.go     # Snapshot JSON de estado (uso auxiliar/depuração)
    │   ├── recuperacao.go      # Replay idempotente de registros do WAL
    │   └── usuario.go          # Structs de Usuário e Sessão
    ├── protocolo/              # DTOs, constantes e comunicação TCP/JSON
    │   ├── io.go               # Escrita/Leitura de sockets TCP delimitados por \n
    │   ├── lamport.go          # Timestamp e comparação de relógio lógico
    │   └── mensagens.go        # Structs Requisicao e Resposta JSON
    └── consorcio/              # Coordenação distribuída entre empresas
        ├── modelo.go           # DTOs de rede do consórcio (NodoDTO, HeartbeatDTO, 2PC, ...)
        ├── lamport.go          # Relógio lógico por nó (RelogioLamport)
        ├── wal.go              # Write-Ahead Log (append-only, fsync, CRC32)
        ├── cliente_rest.go     # Cliente HTTP resiliente para chamadas inter-nós
        ├── servidor_rest.go    # Servidor HTTP inter-nós (handlers de todos os endpoints)
        └── coordenador.go      # Scatter-Gather, Two-Phase Commit, heartbeat automático
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

> O passageiro conecta-se a uma única empresa, mas a busca atravessa o consórcio inteiro: itinerários
> podem combinar trechos de caronas publicadas em empresas diferentes (`CoordenadorDistribuido.
> BuscarItinerariosCombinados`), reservados atomicamente via 2PC - o cenário central do consórcio.
> Listar/cancelar uma reserva que envolve mais de uma empresa é a única limitação conhecida nesse
> fluxo; ver seção 10 de `docs/api_consorcio.md`.

---

## Como Executar

### Pré-requisitos
* **Go 1.22+** instalado OU **Docker / Docker Compose**.

### Opção A: Executando Localmente (Go) - uma única empresa

```bash
go run ./cmd/servidor -empresa-id=nodo-salvador -porta 8080 -porta-rest 9090
go run ./cmd/motorista -servidor localhost:8080
go run ./cmd/passageiro -servidor localhost:8080
```

### Opção B: Executando Localmente (Go) - consórcio completo de 3 empresas

Em três terminais separados (cada empresa conhece as outras duas via `-membros`):

```bash
go run ./cmd/servidor -empresa-id=nodo-salvador -regiao=RMS \
  -porta=8081 -porta-rest=9091 -wal=dados/nodo-salvador.wal \
  -membros=nodo-feira@localhost:9092,nodo-reconcavo@localhost:9093

go run ./cmd/servidor -empresa-id=nodo-feira -regiao="Portal do Sertão" \
  -porta=8082 -porta-rest=9092 -wal=dados/nodo-feira.wal \
  -membros=nodo-salvador@localhost:9091,nodo-reconcavo@localhost:9093

go run ./cmd/servidor -empresa-id=nodo-reconcavo -regiao="Recôncavo" \
  -porta=8083 -porta-rest=9093 -wal=dados/nodo-reconcavo.wal \
  -membros=nodo-salvador@localhost:9091,nodo-feira@localhost:9092
```

Confirme que o consórcio está de pé consultando a topologia vista por qualquer um dos nós:

```bash
curl http://localhost:9091/api/v1/cluster/info
```

### Opção C: Validações automatizadas

```bash
# Auditoria de overbooking (uma carona, muitos clientes disputando poucas vagas)
go run ./cmd/teste_carga -servidor localhost:8081 -clientes 30 -vagas 5

# Simulador de carga sustentada (vários passageiros autônomos por um período)
go run ./cmd/simulador -servidores=localhost:8081,localhost:8082,localhost:8083 \
  -passageiros=80 -motoristas=10 -duracao=30s

# Robô de caos (sobe 3 nós sozinho, mata um com SIGKILL, audita recuperação)
go run ./cmd/teste_falhas -no-alvo=nodo-feira -tempo-queda=5s
```

Os resultados reais dessas três ferramentas, incluindo a auditoria de compensação do 2PC sob falha
de um nó e a verificação de integridade pós-recuperação via WAL, estão documentados com números em
**[`docs/api_consorcio.md`](docs/api_consorcio.md#7-ferramentas-de-validação-bloco-5)**.

---

### Opção D: Executando via Docker Compose (consórcio completo containerizado)

```bash
# Sobe as 3 empresas do consórcio
docker-compose up --build nodo-salvador nodo-feira nodo-reconcavo

# Clientes interativos
docker-compose run --rm motorista
docker-compose run --rm passageiro

# Teste de carga
docker-compose --profile teste up --build
```

---

## Dockerfile e Configuração de Ambiente

O projeto utiliza **Multi-Stage Build** no `Dockerfile`:
1. **Estágio 1 (Builder):** imagem oficial `golang:1.22-alpine`, compila estaticamente todos os
   executáveis Go (`servidor`, `motorista`, `passageiro`, `teste_carga`, `simulador`,
   `teste_falhas`) sem dependências externas (`CGO_ENABLED=0 GOOS=linux`).
2. **Estágio 2 (Runtime):** imagem minimalista `alpine:latest`, expõe as portas TCP e REST do
   servidor e executa o binário solicitado via `docker-compose.yml`.

---

## Testes Automatizados

Além dos três robôs de validação de ponta a ponta descritos acima, o núcleo de domínio e a camada
de consórcio têm suíte de testes unitários e de integração local:

```bash
go test ./...          # suíte completa
go test -race ./...    # com detector de condições de corrida
```

Cobertura inclui: cadastro/autenticação, reserva atômica e cancelamento, expiração de holds,
persistência e recuperação via WAL, relógio de Lamport, Scatter-Gather com tolerância a falha
parcial, Two-Phase Commit (commit multi-nó, abort com compensação) e a rotina automática de
heartbeat detectando a queda de um membro em tempo real.

---

## Guia de Apresentação (Roteiro de Fala para Avaliação)

1. **Abertura & Contexto:** Apresente o **VaiJunto** como um consórcio de empresas de transporte,
   cada uma rodando seu próprio servidor Go, atendendo clientes via TCP puro e conversando entre si
   via REST.
2. **Decisão de Negócio (Modelo de Sub-Trechos):** Mostre que rotas são fragmentadas (ex:
   `Salvador -> Amélia Rodrigues -> Feira de Santana`), alocando vagas por segmento.
3. **Arquitetura de Concorrência Local (`Goroutines` + `sync.RWMutex`):** Thread-per-connection e a
   divisão entre leitura concorrente e escrita exclusiva.
4. **Two-Phase Commit com Compensação:** Explique as duas fases (Prepare com Hold/Lease, Commit ou
   Abort com devolução automática de vagas), e como o desempate de concorrência usa o relógio de
   Lamport em vez de tempo físico.
5. **Durabilidade e Recuperação:** Mostre o WAL com `fsync` e CRC32, e como um nó reconstrói seu
   estado inteiro ao reiniciar após uma queda.
6. **Demonstração ao vivo - carga:**
   ```bash
   go run ./cmd/teste_carga -servidor localhost:8081 -clientes 30 -vagas 5
   ```
7. **Demonstração ao vivo - caos:**
   ```bash
   go run ./cmd/teste_falhas -no-alvo=nodo-feira -tempo-queda=5s
   ```
   Mostre o veredito final: zero perda de dados, zero overbooking, isolamento de falha confirmado.
