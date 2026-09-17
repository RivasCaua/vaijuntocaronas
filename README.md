# VaiJunto - Central de Caronas Compartilhadas

> **TEC502 - Concorrência e Conectividade (Problema 1)**  
> Sistema de intermediação de caronas intermunicipais com controle estrito de concorrência, comunicação TCP pura e prevenção absoluta de overbooking.

---

## 📌 Visão Geral do Projeto

O **VaiJunto** é um sistema centralizado desenvolvido em **Go (Golang)** focado na gestão de caronas intermunicipais entre Salvador, Feira de Santana e região (Portal do Sertão, Recôncavo e RMS).

O sistema lida com o desafio de múltiplos usuários (motoristas e passageiros) realizando acessos simultâneos — cadastrando usuários, publicando caronas, consultando itinerários e efetuando reservas concorrentes.

---

## 🛠️ Principais Decisões de Arquitetura e Negócio

### 1. Desmembramento de Rotas em Sub-Trechos
Em vez de tratar a viagem como um bloco fixo, a rota é dividida em segmentos individuais (ex: `Salvador -> Amélia Rodrigues -> Feira de Santana`).
* **Vagas Dinâmicas:** As vagas são alocadas por trecho específico (`VagasPorTrecho`), permitindo otimizar a lotação do veículo sem conflito de assentos.

### 2. Protocolo Application-Layer sobre TCP Nativo
A comunicação entre os clientes (CLI) e o servidor é feita via **socket TCP puro** (`net.Conn`), utilizando payloads em **JSON delimitados por quebra de linha (`\n`)**.
* Menor *overhead* que HTTP/REST.
* Leitura em stream eficiente usando `bufio.Scanner`.

### 3. Modelo de Concorrência Confiável (`Goroutines` + `sync.RWMutex`)
* **Thread-per-Connection:** Cada cliente conectado é atendido por uma goroutine independente.
* **Leitura Concorrente (`RLock`):** Múltiplos passageiros podem consultar itinerários e cidades simultaneamente.
* **Escrita Exclusiva (`Lock`):** Operações de estado (cadastro, publicação, reserva e cancelamento) usam trava de escrita.

### 4. Algoritmo de Reserva Atômica (**Check-Then-Act**)
Garantia de **0% Overbooking**:
1. **Fase 1 (Check):** Sob `Lock()`, o servidor verifica se **todos** os sub-trechos da solicitação possuem vaga livre (`vagas > 0`).
2. **Fase 2 (Act):** Somente se todos os trechos forem válidos, o inventário é debitado atômica e integralmente.

---

## 📁 Estrutura do Projeto

```text
.
├── Dockerfile                  # Containerização do servidor e teste de carga
├── docker-compose.yml          # Orquestração dos containers Docker
├── go.mod                      # Módulo Go do projeto
├── cmd/
│   ├── servidor/               # Servidor Central TCP (:8080)
│   ├── motorista/              # Cliente CLI para Motoristas
│   ├── passageiro/             # Cliente CLI para Passageiros
│   └── teste_carga/            # Simulator/Auditor de Concorrência Massiva
└── pkg/
    ├── estado/                 # Regras de negócio e gerenciador de estado thread-safe
    └── protocolo/              # DTOs, constantes e comunicação TCP/JSON
```

---

## 🚀 Como Executar

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

## ⚡ Teste de Carga e Auditoria de Concorrência

O módulo `cmd/teste_carga` simula o cenário crítico de **disputa simultânea por vagas**:
* Publica uma carona de teste com número limitado de assentos (ex: 5 vagas).
* Dispara `N` goroutines clientes (ex: 30 clientes) que tentam realizar o fluxo completo de cadastro, login, busca e reserva **exatamente no mesmo instante**.
* Mede a latência e audita o resultado, garantindo que **exatamente 5 vagas são vendidas** e as outras **25 requisições são recusadas graciosamente**, confirmando a ausência de condições de corrida (*race conditions*).
