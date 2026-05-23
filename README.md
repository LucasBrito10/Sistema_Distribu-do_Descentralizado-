# Infraestrutura Distribuída para Coordenação de Drones Autônomos

> **TEC502 — Sistemas Distribuídos · UEFS**  
> Problema 2: Desbloqueio do Estreito de Ormuz

Sistema distribuído descentralizado para coordenação de drones de monitoramento marítimo no Estreito de Ormuz. A solução elimina qualquer ponto único de falha ao distribuir o estado do cluster via **Hazelcast**, permitindo que setores continuem operando de forma autônoma mesmo diante de falhas de brokers ou drones.

---

## Arquitetura da Solução

### Estilo Arquitetural

O sistema adota um estilo de **broker distribuído com cluster peer-to-peer entre os nós**. Não existe servidor central — cada máquina do laboratório executa seu próprio broker de setor, e todos os brokers formam um cluster Hazelcast que compartilha estado distribuído (fila de requisições, mapa de drones, clientes registrados).

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLUSTER HAZELCAST                        │
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐        │
│  │ Broker NORTE │◄──►│ Broker  SUL  │◄──►│ Broker LESTE │  ...  │
│  │   :8080      │   │   :8080      │   │   :8080      │        │
│  │   :5701      │   │   :5701      │   │   :5701      │        │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘        │
│         │                  │                  │                 │
│   Sensores/         Sensores/           Sensores/               │
│   Drones            Drones              Drones                  │
└─────────────────────────────────────────────────────────────────┘
```

### Componentes

| Componente | Módulo | Papel |
|---|---|---|
| **Broker de Setor** | `ormuz-broker` | Recebe conexões TCP de clientes locais, processa mensagens, publica no bus global Hazelcast |
| **Drone** | `ormuz-clients` / `Drone` | Registra-se no broker local, aguarda despacho, executa missão e libera-se ao concluir |
| **Sensor (CoastalRadar / NavalSensor)** | `ormuz-clients` | Gera eventos de detecção com criticidade e envia requisições de despacho de drone |
| **SmartBuoy** | `ormuz-clients` | Sensor de boia marítima, republica tráfego AIS e dados ambientais |
| **Fila Distribuída** | Hazelcast `IQueue` | Armazena requisições pendentes quando não há drones disponíveis |
| **Cluster Bus** | Hazelcast `ITopic` | Canal pub/sub global entre todos os brokers para entrega de mensagens cross-setor |
| **Monitor Central** | `ormuz-clients` / `AppMonitor` | Ferramenta administrativa para visualização em tempo real do estado do cluster |
| **Teste de Carga** | `ormuz-clients` / `AppLoadTest` | Gera requisições concorrentes para validação de consistência |

### Ausência de Ponto Único de Falha

- Cada broker mantém uma cópia distribuída do estado via Hazelcast (`IMap`, `MultiMap`, `IQueue`, `ITopic`).
- Se um broker falhar, o método `cleanUpOrphanedData` é chamado pelos membros sobreviventes (via `MembershipListener`), recolocando os recursos órfãos na fila.
- Clientes configuram `BROKER_IP` como lista ordenada: o broker local é sempre o primeiro (afinidade de setor); os demais são fallback automático de failover.

---

## Protocolo de Comunicação

### Tecnologia

Comunicação entre clientes (sensores, drones) e brokers via **sockets TCP raw**. Entre brokers, o estado é sincronizado via **Hazelcast** (porta 5701).

### APIs entre Componentes

As mensagens trafegam como objetos `Message` serializados. Os `CommandType` e `ServicesTypes` definem os contratos da API:

| Operação | CommandType / fluxo | Descrição |
|---|---|---|
| Registrar cliente | `ACTIVATE` | Drone ou sensor conecta ao broker e anuncia seu tipo de recurso e tópico |
| Solicitar drone | Publicação em `filaRequisicoes` | Sensor insere requisição com setor, criticidade e timestamp na fila distribuída |
| Confirmar despacho | Resposta ao sensor via `clusterWideBus` | Broker reserva drone no `IMap<sharedDrones>` e notifica |
| Missão concluída / liberar drone | `DEACTIVATE` / retorno de status | Drone sinaliza conclusão; broker atualiza mapa e re-consulta a fila |
| Falha de drone | Timeout + watchdog (30 s) | Broker detecta desconexão, remove drone do mapa e recoloca requisição na fila |
| Monitorar cluster | `REQUEST_STATUS` | Monitor central assina tópicos e exibe estado em tempo real |

### Tratamento de Falhas de Comunicação

- **Timeout de tarefa**: o `ScheduledExecutorService` (watchdog) monitora cada tarefa com janela de 30 segundos (`TASK_TIMEOUT_MS`).
- **Retransmissão**: clientes reconectam automaticamente ao próximo broker da lista `BROKER_IP` em caso de falha na conexão.
- **ACK implícito**: a entrega de mensagem local é confirmada pelo `deliverLocal`, que itera sobre os handlers registrados no broker receptor.

---

## Concorrência Distribuída

### Exclusão Mútua de Drones

O acesso ao mapa de drones (`IMap<String, DroneInterface> sharedDrones`) é feito com **locks distribuídos do Hazelcast**. A estrutura `IMap` garante atomicidade nas operações de reserva: apenas um broker consegue alterar o estado de um drone de `DISPONIVEL` para `DESPACHADO` por vez, mesmo sob requisições concorrentes de setores distintos.

### Não-Duplicidade de Cobertura

A fila distribuída (`IQueue<Message> filaRequisicoes`) é única para todo o cluster. Cada requisição possui identificador único e é retirada da fila (`poll`) de forma atômica, garantindo que dois brokers nunca processem a mesma requisição simultaneamente.

### Priorização de Requisições

A fila respeita a propriedade de **ordering** (FIFO distribuído do Hazelcast), preservando a ordem de chegada das requisições. Ocorrências de maior criticidade podem ser inseridas com prioridade via comparação de campos na mensagem.

---

## Confiabilidade

### Fila Distribuída e Replanejamento

- Quando não há drones disponíveis, a requisição é enfileirada em `filaRequisicoes` (Hazelcast `IQueue`, replicada no cluster).
- O método `checkDistributedQueue` é chamado sempre que um drone é liberado (retorno de missão ou reconexão), disparando automaticamente a próxima alocação.
- Requisições podem ser encaminhadas entre setores via `clusterWideBus` quando o setor de origem não possui drones disponíveis.

### Tolerância à Falha de Drone

1. O watchdog detecta inatividade do drone após `TASK_TIMEOUT_MS` (30 s).
2. O `MembershipListener` detecta queda de broker e chama `cleanUpOrphanedData`.
3. Em ambos os casos, a requisição é recolocada na fila distribuída e um novo drone é designado automaticamente.

### Testes de Consistência sob Carga

O componente `AppLoadTest` gera múltiplas requisições concorrentes simulando condições críticas (falhas simultâneas, alta carga de sensores). Os resultados são observáveis via `AppMonitor`.

---

## Execução com Docker

### Pré-requisitos

- Docker e Docker Compose instalados em todas as máquinas do cluster.
- Conectividade TCP nas portas `8080` (clientes) e `5701` (Hazelcast inter-broker).

### Ambiente de Desenvolvimento (máquina única — simula 4 setores)

```bash
# Sobe os 4 brokers + sensores + drones localmente
docker compose -f docker-compose.dev.yaml up -d

# Para incluir o monitor interativo
docker compose -f docker-compose.dev.yaml --profile admin up -d
docker attach monitor-central
```

### Ambiente de Produção (múltiplas máquinas — LARSID/LADICA)

Execute em **cada máquina** do cluster, ajustando as variáveis de ambiente:

```bash
# Exemplo para a máquina do SETOR_NORTE
export SETOR_NOME=SETOR_NORTE
export CLUSTER_IPS=192.168.1.10,192.168.1.11,192.168.1.12,192.168.1.13
export PUBLIC_ADDRESS=192.168.1.10   # IP físico DESTA máquina

docker compose up -d
```

| Variável | Descrição |
|---|---|
| `SETOR_NOME` | Identificador único do setor desta máquina (`SETOR_NORTE`, `SETOR_SUL`, `SETOR_LESTE`, `SETOR_OESTE`) |
| `CLUSTER_IPS` | IPs físicos de **todas** as máquinas do cluster (incluindo a atual) |
| `PUBLIC_ADDRESS` | IP físico desta máquina — necessário para o Hazelcast se anunciar corretamente entre máquinas distintas |

### Ferramentas Administrativas (opcional)

```bash
# Ativar monitor e teste de carga
docker compose --profile admin up -d

# Acompanhar monitor em tempo real
docker attach monitor-central
```

### Verificar status do cluster

```bash
docker ps
docker logs broker-SETOR_NORTE
```

---

## 🗂️ Estrutura do Repositório

```
.
├── docker-compose.yaml          # Deploy multi-máquina (produção)
├── docker-compose.dev.yaml      # Deploy local (desenvolvimento/testes)
├── ormuz-broker/                # Módulo do broker de setor
│   └── src/main/java/com/ormuz/broker/
│       ├── app/AppBroker.java           # Ponto de entrada do broker
│       ├── core/Broker.java             # Lógica central: Hazelcast, fila, despacho
│       ├── log/BrokerLogger.java        # Logger com tipos (SISTEMA, BUS, CLUSTER)
│       └── network/MiddlewareHandlerServer.java  # Handler TCP por cliente
├── ormuz-clients/               # Módulo de drones, sensores e ferramentas
│   └── src/main/java/com/ormuz/client/
│       ├── app/
│       │   ├── AppFrotaDrones.java      # Inicializa frota de drones
│       │   ├── AppSensores.java         # Inicializa sensores do setor
│       │   ├── AppMonitor.java          # Monitor central interativo
│       │   └── AppLoadTest.java         # Teste de carga
│       ├── core/
│       │   ├── Client.java              # Base de todo cliente
│       │   └── MiddlewareClient.java    # Conexão TCP com failover
│       ├── drone/Drone.java             # Lógica do drone (missão, liberação)
│       └── sensor/
│           ├── CoastalRadar.java        # Radar costeiro
│           └── NavalSensor.java         # Sensor naval subaquático
└── ormuz-shared/                # Tipos e enums compartilhados
    └── src/main/java/com/ormuz/shared/
        ├── enums/
        │   ├── CommandType.java         # Comandos do protocolo
        │   ├── ServicesTypes.java       # Serviços por tipo de recurso
        │   ├── ResourcesTypes.java      # Tipos de recurso (drone, radar, boia…)
        │   └── TopicType.java           # Tópicos pub/sub
        ├── interfaces/DroneInterface.java
        └── types/                       # DTOs: Message, DroneData, ClientData
```

---

## 🧪 Teste de Falha Documentado

### Cenário: queda de um broker

```bash
# 1. Verificar estado inicial
docker logs broker-SETOR_NORTE | grep "Cluster size"

# 2. Derrubar o broker do setor leste
docker stop broker-SETOR_LESTE

# 3. Observar reconfiguração no cluster
docker logs broker-SETOR_NORTE | grep -E "memberRemoved|cleanUp|fila"

# 4. Confirmar que sensores/drones do setor leste reconectaram ao fallback
docker logs sensores-SETOR_LESTE | grep "Reconectando"

# 5. Restaurar o broker
docker start broker-SETOR_LESTE
docker logs broker-SETOR_NORTE | grep "memberAdded"
```

### Cenário: perda de drone durante missão

```bash
# Identificar container do drone
docker ps | grep drones

# Simular falha
docker pause drones-SETOR_SUL

# Após ~30s (TASK_TIMEOUT_MS), verificar replanejamento
docker logs broker-SETOR_SUL | grep -E "timeout|fila|replanejamento"

# Restaurar
docker unpause drones-SETOR_SUL
```

---

## 🔧 Tecnologias Utilizadas

| Tecnologia | Uso |
|---|---|
| **Java 17+** | Linguagem principal |
| **Hazelcast** | Cluster distribuído, fila, mapa e bus global |
| **Sockets TCP** | Comunicação broker ↔ clientes |
| **Docker / Docker Compose** | Contêinerização e orquestração |
| **Maven** | Build e gestão de dependências |

---

## 📋 Informações Acadêmicas

- **Disciplina**: TEC502 — Sistemas Distribuídos  
- **Instituição**: Universidade Estadual de Feira de Santana (UEFS)  
- **Departamento**: Tecnologia  
- **Laboratórios de apresentação**: LARSID / LADICA  
