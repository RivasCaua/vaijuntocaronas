# Estágio 1: Compilação estática dos binários Go
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copia módulo e código fonte do projeto
COPY go.mod ./
COPY cmd/ ./cmd/
COPY pkg/ ./pkg/

# Compilação dos executáveis estáticos para Linux
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/bin/servidor ./cmd/servidor
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/bin/motorista ./cmd/motorista
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/bin/passageiro ./cmd/passageiro
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/bin/teste_carga ./cmd/teste_carga

# Estágio 2: Imagem final minimalista de execução
FROM alpine:latest

WORKDIR /app

# Copia os binários compilados
COPY --from=builder /app/bin/ /app/bin/

# Expõe a porta TCP padrão 8080
EXPOSE 8080

# Comando padrão ao executar o contêiner do servidor central
CMD ["/app/bin/servidor", "-porta", "8080"]
