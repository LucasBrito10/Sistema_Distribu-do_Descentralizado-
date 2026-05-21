package com.ormuz.broker.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.stream.Collectors;

import com.ormuz.broker.core.Broker;
import com.ormuz.broker.core.BrokerBuilder;
import com.ormuz.broker.core.BrokerInterface;
import com.ormuz.broker.log.BrokerLogger;
import com.ormuz.broker.log.BrokerLogger.LogType;
import com.ormuz.shared.enums.CommandType;
import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.types.Message;

public class AppBroker {

    public static void main(String[] args) {
        int portaLocal = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        
        // CORREÇÃO: Lê a string e gera um array separando pela vírgula
        String[] clusterIps = args.length > 1 ? args[1].split(",") : new String[]{"127.0.0.1"};

        String logTypesEnv = System.getenv("BROKER_LOG_TYPES");
        if (logTypesEnv != null && !logTypesEnv.isBlank()) {
            BrokerLogger.desabilitarTodos();
            for (String token : logTypesEnv.split(",")) {
                try {
                    BrokerLogger.habilitar(LogType.valueOf(token.trim().toUpperCase()));
                } catch (IllegalArgumentException e) {
                    System.err.println("[SISTEMA] LogType desconhecido ignorado: '" + token.trim() + "'");
                }
            }
        }

        BrokerLogger.log(LogType.SISTEMA, "[SISTEMA] Iniciando Servidor Broker...");
        BrokerInterface broker = new BrokerBuilder()
                .withPort(portaLocal)
                .withClusterIps(clusterIps) // Repassa o array de IPs/Hosts para o Hazelcast
                .build();
        BrokerLogger.log(LogType.SISTEMA, "[SISTEMA] Broker rodando na porta " + portaLocal);

        new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            try {
                Thread.sleep(7000);
                while (true) {
                    exibirMenu();
                    String op = scanner.nextLine().trim();

                    switch (op) {
                        case "1" -> provisionarRecurso(scanner, broker);
                        case "2" -> reassociarRecursoSetor(scanner, broker);
                        case "3" -> configurarDataTypesSensor(scanner, broker);
                        case "4" -> configurarLogsBroker(scanner);
                        case "5" -> System.exit(0);
                        default  -> System.out.println("[PAINEL] Opção inválida.");
                    }
                }
            } catch (NoSuchElementException e) {
                System.out.println("[SISTEMA] Console indisponível (Modo Background ativo).");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        broker.runServer(true);
    }

    private static void exibirMenu() {
        System.out.println("\n══════════════════════════════════════════════════");
        System.out.println("       PAINEL DE CONTROLE DO BROKER ORMUZ         ");
        System.out.println("══════════════════════════════════════════════════");
        System.out.println("1. Provisionar Novo Recurso no Cluster");
        System.out.println("2. Reassociar Recurso a Outro Setor");
        System.out.println("3. Configurar Tipos de Dado dos Sensores");
        System.out.println("4. Configurar Tipos de Log do Broker");
        System.out.println("5. Sair");
        System.out.print("Escolha uma opcao: ");
    }

    private static void provisionarRecurso(Scanner scanner, BrokerInterface broker) {
        System.out.println("\n--- Selecione o Tipo de Dispositivo ---");
        System.out.println("1. Radar Costeiro (Coastal Radar)");
        System.out.println("2. Sensor Naval (Naval Sensor)");
        System.out.println("3. Unidade Drone");
        System.out.print("Opcao: ");
        String tipo = scanner.nextLine().trim();

        System.out.print("Setor Alvo (ex: SETOR_NORTE): ");
        String setor = scanner.nextLine().trim().toUpperCase();

        System.out.print("Broker Alvo para Conexao Fisica (ex: broker-norte): ");
        String brokerAlvo = scanner.nextLine().trim();

        Message cmd = new Message();
        cmd.setCommandType(CommandType.ACTIVATE);
        cmd.setSectorId(setor);
        cmd.setNodeId(brokerAlvo);

        switch (tipo) {
            case "1" -> { cmd.setTargetNodeId("FACTORY-SENSORES-" + setor); cmd.setServiceType(ServicesTypes.TRAFFIC_MONITORING); }
            case "2" -> { cmd.setTargetNodeId("FACTORY-SENSORES-" + setor); cmd.setServiceType(ServicesTypes.HYDROGRAPHIC_PROFILING); }
            case "3" -> { cmd.setTargetNodeId("FACTORY-DRONES-" + setor);   cmd.setServiceType(ServicesTypes.VISUAL_RECONNAISSANCE); }
            default  -> { System.out.println("Tipo inválido."); return; }
        }

        publicarComando(broker, cmd, "Provisonamento de recurso enviado.");
    }

    private static void reassociarRecursoSetor(Scanner scanner, BrokerInterface broker) {
        System.out.println("\n--- Reassociar Recurso a Outro Setor ---");
        System.out.println("Informe o ID do recurso a reassociar.");
        System.out.println("  Exemplos: DRN-DINAMICO-1-SETOR_NORTE, FACTORY-SENSORES-SETOR_SUL");
        System.out.print("ID do Recurso: ");
        String recursoId = scanner.nextLine().trim();

        System.out.print("Novo Setor Alvo (ex: SETOR_SUL): ");
        String novoSetor = scanner.nextLine().trim().toUpperCase();

        System.out.print("Broker do Novo Setor (ex: broker-sul): ");
        String novoBroker = scanner.nextLine().trim();

        Message cmd = new Message();
        cmd.setCommandType(CommandType.REASSIGN_SECTOR);
        cmd.setTargetNodeId(recursoId);
        cmd.setSectorId(novoSetor);
        cmd.setNodeId(novoBroker);

        publicarComando(broker, cmd,
                "Reassociação de '" + recursoId + "' para setor '" + novoSetor
                + "' via broker '" + novoBroker + "' enviada.");
    }

    private static void configurarDataTypesSensor(Scanner scanner, BrokerInterface broker) {
        System.out.println("\n--- Configurar Tipos de Dado dos Sensores ---");
        System.out.print("Setor Alvo (ex: SETOR_NORTE): ");
        String setor = scanner.nextLine().trim().toUpperCase();

        System.out.println("\nTipos de dado disponíveis:");
        ServicesTypes[] todos = ServicesTypes.values();
        for (int i = 0; i < todos.length; i++) {
            System.out.printf("  %2d. %s  (%s)%n", i + 1, todos[i].name(), todos[i].getRelatedResource());
        }
        System.out.println("  Digite os números separados por vírgula para HABILITAR.");
        System.out.println("  Deixe em branco para habilitar TODOS.");
        System.out.print("Seleção: ");
        String selecao = scanner.nextLine().trim();

        List<ServicesTypes> selecionados;
        if (selecao.isBlank()) {
            selecionados = Arrays.asList(todos);
            System.out.println("[PAINEL] Todos os tipos de dado serão habilitados.");
        } else {
            selecionados = new ArrayList<>();
            for (String parte : selecao.split(",")) {
                try {
                    int idx = Integer.parseInt(parte.trim()) - 1;
                    if (idx >= 0 && idx < todos.length) {
                        selecionados.add(todos[idx]);
                    } else {
                        System.out.println("[PAINEL] Índice fora do intervalo ignorado: " + (idx + 1));
                    }
                } catch (NumberFormatException e) {
                    System.out.println("[PAINEL] Entrada inválida ignorada: '" + parte.trim() + "'");
                }
            }
        }

        if (selecionados.isEmpty()) {
            System.out.println("[PAINEL] Nenhum tipo válido selecionado. Operação cancelada.");
            return;
        }

        Message cmd = new Message();
        cmd.setCommandType(CommandType.SET_DATA_TYPES);
        cmd.setTargetNodeId("FACTORY-SENSORES-" + setor);
        cmd.setSectorId(setor);
        cmd.setDataTypes(selecionados);

        String listaStr = selecionados.stream()
                .map(ServicesTypes::name)
                .collect(Collectors.joining(", "));
        publicarComando(broker, cmd,
                "Configuração de data types para setor '" + setor
                + "': [" + listaStr + "]");
    }

    private static void configurarLogsBroker(Scanner scanner) {
        System.out.println("\n--- Configurar Logs do Broker ---");
        System.out.println("Tipos disponíveis:");
        LogType[] tipos = LogType.values();
        for (int i = 0; i < tipos.length; i++) {
            String estado = BrokerLogger.estaHabilitado(tipos[i]) ? "[ON]" : "[OFF]";
            System.out.printf("  %2d. %s %s%n", i + 1, tipos[i].name(), estado);
        }
        System.out.println("  Digite os números a HABILITAR (vírgula). Os demais serão desabilitados.");
        System.out.println("  Deixe em branco para habilitar TODOS.");
        System.out.print("Seleção: ");
        String selecao = scanner.nextLine().trim();

        if (selecao.isBlank()) {
            BrokerLogger.habilitarTodos();
            System.out.println("[PAINEL] Todos os tipos de log habilitados.");
            return;
        }

        BrokerLogger.desabilitarTodos();
        for (String parte : selecao.split(",")) {
            try {
                int idx = Integer.parseInt(parte.trim()) - 1;
                if (idx >= 0 && idx < tipos.length) {
                    BrokerLogger.habilitar(tipos[idx]);
                    System.out.println("[PAINEL] Log habilitado: " + tipos[idx].name());
                }
            } catch (NumberFormatException e) {
                System.out.println("[PAINEL] Entrada inválida ignorada: '" + parte.trim() + "'");
            }
        }
    }

    private static void publicarComando(BrokerInterface broker, Message cmd, String confirmacao) {
        if (broker instanceof Broker b) {
            b.getClusterWideBus().publish(cmd);
            System.out.println("[ORQUESTRADOR] " + confirmacao);
        }
    }
}