package com.ormuz.client.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.ormuz.client.core.Client;
import com.ormuz.shared.enums.CommandType;
import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.interfaces.DroneInterface;
import com.ormuz.shared.types.ClientData;
import com.ormuz.shared.types.Message;

public class AppMonitor extends Client {

    // -------------------------------------------------------------------------
    // Recepção de mensagens do broker (dados de sensores e drones)
    // -------------------------------------------------------------------------

    @Override
    protected void handleIncomingMessage(Message msg) {
        if (msg.getData() == -1) return;
        if (msg.getServiceType() == null) return;

        String interpretacao;
        switch (msg.getServiceType()) {
            case HYDROGRAPHIC_PROFILING ->
                interpretacao = "Profundidade: " + msg.getData() + " metros. Status: Estável.";
            case PRESSURE_SENSING ->
                interpretacao = "Pressão barométrica: " + msg.getData() + " kPa.";
            case INTRUSION_DETECTION ->
                interpretacao = msg.getData() == 1
                        ? "⚠️  ALERTA CRÍTICO: Embarcação hostil não identificada!"
                        : "Área limpa.";
            case TRAFFIC_MONITORING ->
                interpretacao = "Tráfego marítimo registrado. Unidades: " + msg.getData();
            case LONG_RANGE_SURVEILLANCE ->
                interpretacao = "Varredura de longo alcance. Alertas: " + msg.getData();
            case VISUAL_RECONNAISSANCE ->
                interpretacao = msg.getData() == 0
                        ? "Drone retornou. Nenhuma anomalia visual."
                        : "Imagens confirmam alvo.";
            default ->
                interpretacao = "Valor lido: " + msg.getData();
        }

        System.out.println("\n>>> [MONITOR] " + msg.getServiceType()
                + " | Setor: " + msg.getSectorId()
                + " | Origem: " + msg.getNodeId()
                + "\n    └─ " + interpretacao);
    }

    // -------------------------------------------------------------------------
    // Ponto de entrada
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws InterruptedException {
        String clusterIps = args.length > 0 ? args[0] : "127.0.0.1";
        int    porta      = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
        String setor      = args.length > 2 ? args[2] : "SETOR_NORTE";

        // Extrai o primeiro host como preferencial, mas registra todos no
        // Hazelcast Client para que o monitor consiga conectar ao cluster
        // mesmo que o primeiro broker esteja fora do ar em multi-máquina
        String primeiroHost = clusterIps.split(",")[0].trim();

        AppMonitor monitor = new AppMonitor();
        monitor.setId("MONITOR-CENTRAL");
        monitor.setSectorId(setor);

        // Assina todos os tópicos de dados para visibilidade completa
        for (ServicesTypes s : ServicesTypes.values()) {
            monitor.startSubscribe(clusterIps, porta, s);
        }

        // Conexão Hazelcast Client para consulta ao estado distribuído
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("ormuz-cluster");
        for (String h : clusterIps.split(",")) {
            clientConfig.getNetworkConfig().addAddress(h.trim() + ":5701");
        }
        HazelcastInstance hzClient = HazelcastClient.newHazelcastClient(clientConfig);

        IMap<String, DroneInterface> dronesMap  = hzClient.getMap("DroneMap");
        IMap<String, ClientData>    clientsMap  = hzClient.getMap("global-clients-map");

        System.out.println("\n=== TERMINAL DE MONITORAMENTO ORMUZ ===");
        System.out.println("Broker: " + clusterIps + " | Setor: " + setor);

        try (Scanner scanner = new Scanner(System.in)) {
            int opcao = 0;
            while (opcao != 7) {
                exibirMenu();
                try {
                    opcao = Integer.parseInt(scanner.nextLine().trim());
                } catch (NumberFormatException e) {
                    opcao = 0;
                }

                switch (opcao) {
                    case 1 -> listarFrotaDrones(dronesMap);
                    case 2 -> listarEquipamentos(clientsMap);
                    case 3 -> listarDataTypesPorSetor(clientsMap);
                    case 4 -> configurarDataTypesSensor(scanner, monitor, clusterIps, setor);
                    case 5 -> reassociarRecurso(scanner, monitor, clusterIps, setor);
                    case 6 -> { for (int i = 0; i < 50; i++) System.out.println(); }
                    case 7 -> {
                        System.out.println("Encerrando...");
                        hzClient.shutdown();
                        System.exit(0);
                    }
                    default -> System.out.println("Opção inválida.");
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Menu
    // -------------------------------------------------------------------------

    private static void exibirMenu() {
        System.out.println("\n══════════════════════════════════════════════");
        System.out.println("       TERMINAL DE MONITORAMENTO ORMUZ         ");
        System.out.println("══════════════════════════════════════════════");
        System.out.println("1. Verificar Status da Frota de Drones");
        System.out.println("2. Listar Equipamentos Conectados");
        System.out.println("3. Listar Tipos de Dado Ativos por Setor");
        System.out.println("4. Configurar Tipos de Dado de Sensores");
        System.out.println("5. Reassociar Recurso a Outro Setor");
        System.out.println("6. Limpar Console");
        System.out.println("7. Sair");
        System.out.print("Escolha uma opcao: ");
    }

    // -------------------------------------------------------------------------
    // Opção 1 — status da frota
    // -------------------------------------------------------------------------

    private static void listarFrotaDrones(IMap<String, DroneInterface> dronesMap) {
        System.out.println("\n--- STATUS DA FROTA DE DRONES ---");
        if (dronesMap.isEmpty()) {
            System.out.println("Nenhum drone registrado.");
            return;
        }
        for (Map.Entry<String, DroneInterface> entry : dronesMap.entrySet()) {
            DroneInterface d = entry.getValue();
            String status = d.isInUse() ? "🔴 EM MISSÃO" : "🟢 LIVRE";
            String missao = d.getPendingServiceType() != null
                    ? " | Serviço: " + d.getPendingServiceType().name()
                    : "";
            String setorPendente = d.getPendingTaskSectorId() != null
                    ? " | Setor da Missão: " + d.getPendingTaskSectorId()
                    : "";
            System.out.printf("  %-40s %s%s%s%n",
                    d.getDroneId(), status, missao, setorPendente);
        }
    }

    // -------------------------------------------------------------------------
    // Opção 2 — equipamentos conectados
    // -------------------------------------------------------------------------

    private static void listarEquipamentos(IMap<String, ClientData> clientsMap) {
        System.out.println("\n--- EQUIPAMENTOS ONLINE ---");
        if (clientsMap.isEmpty()) {
            System.out.println("Nenhum cliente registrado.");
            return;
        }
        // Agrupa por setor para facilitar visualização
        Map<String, List<ClientData>> porSetor = clientsMap.values().stream()
                .collect(Collectors.groupingBy(c -> c.getSectorId() != null ? c.getSectorId() : "SEM_SETOR"));

        porSetor.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    System.out.println("\n  [" + entry.getKey() + "]");
                    entry.getValue().forEach(c ->
                            System.out.printf("    • %-45s tipo: %-12s tópico: %s%n",
                                    c.getNodeId(), c.getConnectionType(),
                                    c.getTopic() != null ? c.getTopic().name() : "N/A"));
                });
    }

    // -------------------------------------------------------------------------
    // Opção 3 — listar tipos de dado por setor (via clientes registrados)
    // -------------------------------------------------------------------------

    private static void listarDataTypesPorSetor(IMap<String, ClientData> clientsMap) {
        System.out.println("\n--- TIPOS DE DADO ATIVOS POR SETOR ---");
        System.out.println("(Baseado nos tópicos que cada sensor assina/publica)");
        if (clientsMap.isEmpty()) {
            System.out.println("Nenhum cliente registrado.");
            return;
        }
        Map<String, List<ClientData>> porSetor = clientsMap.values().stream()
                .filter(c -> c.getSectorId() != null)
                .collect(Collectors.groupingBy(ClientData::getSectorId));

        porSetor.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    System.out.println("\n  [" + entry.getKey() + "]");
                    Map<String, List<ClientData>> porTipo = entry.getValue().stream()
                            .collect(Collectors.groupingBy(c ->
                                    c.getTopic() != null ? c.getTopic().name() : "N/A"));
                    porTipo.forEach((topico, clientes) -> {
                        String ids = clientes.stream()
                                .map(ClientData::getNodeId)
                                .collect(Collectors.joining(", "));
                        System.out.printf("    • %-35s → %s%n", topico, ids);
                    });
                });
    }

    // -------------------------------------------------------------------------
    // Opção 4 — configurar tipos de dado dos sensores de um setor
    // -------------------------------------------------------------------------

    private static void configurarDataTypesSensor(Scanner scanner, AppMonitor monitor,
                                                   String ipBroker, String setorMonitor) {
        System.out.println("\n--- Configurar Tipos de Dado dos Sensores ---");
        System.out.print("Setor Alvo (ex: SETOR_NORTE): ");
        String setor = scanner.nextLine().trim().toUpperCase();

        System.out.println("\nTipos de dado disponíveis:");
        ServicesTypes[] todos = ServicesTypes.values();
        for (int i = 0; i < todos.length; i++) {
            System.out.printf("  %2d. %-35s recurso: %s%n",
                    i + 1, todos[i].name(), todos[i].getRelatedResource());
        }
        System.out.println("  Digite os números a HABILITAR (vírgula). Deixe em branco = TODOS.");
        System.out.print("Seleção: ");
        String selecao = scanner.nextLine().trim();

        List<ServicesTypes> selecionados;
        if (selecao.isBlank()) {
            selecionados = Arrays.asList(todos);
        } else {
            selecionados = new ArrayList<>();
            for (String parte : selecao.split(",")) {
                try {
                    int idx = Integer.parseInt(parte.trim()) - 1;
                    if (idx >= 0 && idx < todos.length) selecionados.add(todos[idx]);
                } catch (NumberFormatException ignored) {}
            }
        }

        if (selecionados.isEmpty()) {
            System.out.println("[MONITOR] Nenhum tipo válido selecionado.");
            return;
        }

        Message cmd = new Message();
        cmd.setCommandType(CommandType.SET_DATA_TYPES);
        cmd.setTargetNodeId("FACTORY-SENSORES-" + setor);
        cmd.setSectorId(setor);
        cmd.setDataTypes(selecionados);

        monitor.sendCommandMessage(cmd);
        System.out.println("[MONITOR] Comando SET_DATA_TYPES enviado para FACTORY-SENSORES-" + setor);
        System.out.println("[MONITOR] Tipos habilitados: "
                + selecionados.stream().map(ServicesTypes::name).collect(Collectors.joining(", ")));
    }

    // -------------------------------------------------------------------------
    // Opção 5 — reassociar recurso a outro setor
    // -------------------------------------------------------------------------

    private static void reassociarRecurso(Scanner scanner, AppMonitor monitor,
                                          String ipBroker, String setorMonitor) {
        System.out.println("\n--- Reassociar Recurso a Outro Setor ---");
        System.out.println("Esta operação envia um comando REASSIGN_SECTOR via barramento.");
        System.out.println("Exemplos de ID: DRN-DINAMICO-1-SETOR_NORTE, FACTORY-SENSORES-SETOR_SUL");
        System.out.print("ID do Recurso: ");
        String recursoId = scanner.nextLine().trim();

        System.out.print("Novo Setor (ex: SETOR_SUL): ");
        String novoSetor = scanner.nextLine().trim().toUpperCase();

        System.out.print("Broker do Novo Setor (ex: broker-sul): ");
        String novoBroker = scanner.nextLine().trim();

        System.out.println("\n[MONITOR] Comando REASSIGN_SECTOR:");
        System.out.println("  Recurso : " + recursoId);
        System.out.println("  Destino : " + novoSetor + " via " + novoBroker);
        System.out.println("[NOTA] Para efetuar a reassociação com acesso ao barramento Hazelcast,");
        System.out.println("       use o painel do AppBroker (opção 2) diretamente no container do broker.");
    }
}
