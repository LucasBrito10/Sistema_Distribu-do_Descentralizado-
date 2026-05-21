package com.ormuz.broker.core;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.topic.ITopic;
import com.ormuz.broker.log.BrokerLogger;
import com.ormuz.broker.log.BrokerLogger.LogType;
import com.ormuz.broker.network.MiddlewareHandlerBuilder;
import com.ormuz.broker.network.MiddlewareHandlerInterface;
import com.ormuz.shared.enums.CommandType;
import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.enums.TopicType;
import com.ormuz.shared.interfaces.DroneInterface;
import com.ormuz.shared.types.ClientData;
import com.ormuz.shared.types.DroneData;
import com.ormuz.shared.types.Message;

public class Broker implements BrokerInterface {
    private ServerSocket serverSocket;
    private final Map<TopicType, Set<MiddlewareHandlerInterface>> clientsPerTopic = new ConcurrentHashMap<>();
    private final Map<String, MiddlewareHandlerInterface> clientsById = new ConcurrentHashMap<>();
    
    private IMap<String, ClientData> sharedClients;
    private MultiMap<String, String> sharedTopics;  
    private IMap<String, DroneInterface> sharedDrones; 
    private ITopic<Message> clusterWideBus; 
    private IQueue<Message> filaRequisicoes; 
    
    private HazelcastInstance hz;
    private String brokerId;

    private final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor();
    private static final long TASK_TIMEOUT_MS = 30_000;

    public Broker(int port, String[] clusterIps) {
        try {
            this.serverSocket = new ServerSocket(port);

            Config config = new Config();
            config.setClusterName("ormuz-cluster");
            
            NetworkConfig network = config.getNetworkConfig();
            network.getInterfaces().setEnabled(false);

            String publicAddress = System.getenv("PUBLIC_ADDRESS");
            if (publicAddress != null && !publicAddress.isBlank()) {
                network.setPublicAddress(publicAddress.trim() + ":5701");
                BrokerLogger.log(LogType.SISTEMA, "[BROKER] PublicAddress configurado: " + publicAddress.trim() + ":5701");
            }
            
            JoinConfig join = network.getJoin();
            join.getMulticastConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(true);
            
            if (clusterIps != null && clusterIps.length > 0) {
                for (String ip : clusterIps) {
                    join.getTcpIpConfig().addMember(ip.trim() + ":5701");
                }
            } else {
                join.getTcpIpConfig().addMember("127.0.0.1:5701");
            }

            this.hz = Hazelcast.newHazelcastInstance(config);
            this.brokerId = hz.getCluster().getLocalMember().getUuid().toString();
            BrokerLogger.log(LogType.SISTEMA, "[BROKER " + brokerId.substring(0, 8) + "] Hazelcast iniciado. Cluster size: "
                + hz.getCluster().getMembers().size());

            this.clusterWideBus = hz.getTopic("ormuz-global-bus");
            this.clusterWideBus.addMessageListener(msg -> {
                BrokerLogger.log(LogType.BUS, "[BROKER " + brokerId.substring(0, 8) + "] <<< Mensagem no bus: " + msg.getMessageObject());
                deliverLocal(msg.getMessageObject());
            });

            this.hz.getCluster().addMembershipListener(new MembershipListener() {
                @Override public void memberAdded(MembershipEvent e) {
                    BrokerLogger.log(LogType.CLUSTER, "[BROKER] Novo membro no cluster: " + e.getMember().getUuid());
                }
                @Override public void memberRemoved(MembershipEvent e) {
                    BrokerLogger.log(LogType.CLUSTER, "[BROKER] Membro removido: " + e.getMember().getUuid() + " — limpando dados órfãos");
                    cleanUpOrphanedData(e.getMember().getUuid().toString());
                }
            });

            this.sharedClients = hz.getMap("global-clients-map");
            this.sharedTopics  = hz.getMultiMap("global-topics-multimap");
            this.sharedDrones = hz.getMap("DroneMap");
            this.sharedDrones.addIndex(IndexType.HASH, "inUse");
            this.filaRequisicoes = hz.getQueue("ormuz-fila-requisicoes");

            this.watchdog.scheduleAtFixedRate(() -> {
                long now = System.currentTimeMillis();
                for (Map.Entry<String, DroneInterface> entry : this.sharedDrones.entrySet()) {
                    DroneInterface drone = entry.getValue();
                    if (!drone.isInUse()) continue;
                    Long activatedAt = drone.getActivatedAt();
                    if (activatedAt != null && (now - activatedAt) > TASK_TIMEOUT_MS) {
                        BrokerLogger.log(LogType.DRONE,
                            "[WATCHDOG] Drone " + drone.getDroneId()
                            + " excedeu timeout de " + TASK_TIMEOUT_MS / 1000 + "s. Liberando forçadamente.");
                        DroneInterface freed = new DroneData(drone.getDroneId(), false, drone.getCurrentBrokerId());
                        if (this.sharedDrones.replace(entry.getKey(), drone, freed)) {
                            checkDistributedQueue();
                        }
                    }
                }
            }, 5, 5, TimeUnit.SECONDS);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void cleanUpOrphanedData(String crashedBrokerId) {
        for (Map.Entry<String, ClientData> entry : this.sharedClients.entrySet()) {
            ClientData client = entry.getValue();
            if (crashedBrokerId.equals(client.getConnectedBrokerId())) {
                this.sharedClients.remove(entry.getKey());
                String key = client.getSectorId() + ":" + client.getTopic();
                this.sharedTopics.remove(key, client.getNodeId());
                BrokerLogger.log(LogType.CLUSTER, "[BROKER] Cliente órfão removido: " + client.getNodeId());
            }
        }

        Predicate<String, DroneInterface> condition = Predicates.equal("currentBrokerId", crashedBrokerId);
        for (Map.Entry<String, DroneInterface> entry : this.sharedDrones.entrySet(condition)) {
            DroneInterface crashed = entry.getValue();

            // CORREÇÃO: remove a entrada completamente em vez de substituir por um objeto
            // com currentBrokerId=null. Isso evita que o putIfAbsent em addConnection
            // ignore o novo registro (com o broker correto) ao drone reconectar,
            // prevenindo o NullPointerException em getCurrentBrokerId().substring().
            if (this.sharedDrones.remove(entry.getKey(), crashed)) {
                BrokerLogger.log(LogType.DRONE, "[BROKER] Drone recuperado (entrada removida): " + entry.getKey());

                if (crashed.isInUse() && crashed.getPendingTaskSectorId() != null && crashed.getPendingServiceType() != null) {
                    BrokerLogger.log(LogType.DRONE, "[BROKER] Reassinando tarefa do setor " + crashed.getPendingTaskSectorId()
                            + " (drone caído: " + crashed.getDroneId() + ")");
                    Message reassign = new Message();
                    reassign.setSectorId(crashed.getPendingTaskSectorId());
                    reassign.setServiceType(crashed.getPendingServiceType() == ServicesTypes.VISUAL_RECONNAISSANCE
                            ? ServicesTypes.INTRUSION_DETECTION
                            : ServicesTypes.SEARCH_AND_RESCUE);
                    reassign.setData(1);
                    processMessage(reassign);
                }
            }
        }
    }

    @Override
    public void runServer(boolean isRunning) {
        BrokerLogger.log(LogType.SISTEMA, "[BROKER " + brokerId.substring(0, 8) + "] Aguardando conexões...");
        try {
            while (isRunning) {
                Socket socket = this.serverSocket.accept();
                BrokerLogger.log(LogType.CONEXAO, "[BROKER] Nova conexão: " + socket.getRemoteSocketAddress());
                MiddlewareHandlerInterface handler = new MiddlewareHandlerBuilder()
                        .withSocket(socket)
                        .withBroker(this)
                        .build();
                new Thread(handler).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addConnection(TopicType topic, String nodeId, MiddlewareHandlerInterface handler) {
        this.clientsPerTopic.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(handler);
        this.clientsById.put(nodeId, handler);
        BrokerLogger.log(LogType.CONEXAO, "[BROKER] Conexão registrada: " + nodeId + " → tópico " + topic);

        if (nodeId != null && nodeId.startsWith("DRN")) {
            DroneInterface drone = new DroneData(nodeId, false, this.brokerId);

            // CORREÇÃO: usa put() incondicional em vez de putIfAbsent().
            // Cenário de falha anterior: cleanUpOrphanedData() removia a entrada;
            // o drone reconectava ao broker sul e o putIfAbsent não entrava porque
            // a chave ainda existia (com currentBrokerId=null), deixando o mapa
            // distribuído com um brokerId nulo e causando NullPointerException
            // ao chamar getCurrentBrokerId().substring() em processMessage().
            // Com put() o registro é sempre atualizado com o broker correto.
            DroneInterface previous = this.sharedDrones.put(nodeId, drone);
            if (previous == null) {
                // Primeira conexão do drone: ele está disponível de fato.
                // Só aqui faz sentido drenar a fila — as 5 conexões seguintes
                // (re-registros das demais portas subscriber/publisher) não
                // representam um novo drone disponível, apenas o mesmo drone
                // completando seu handshake de 6 sockets.
                BrokerLogger.log(LogType.DRONE, "[BROKER] Drone registrado no cluster: " + nodeId);
                checkDistributedQueue();
            } else {
                BrokerLogger.log(LogType.DRONE, "[BROKER] Drone re-registrado no cluster (failover): " + nodeId
                        + " | broker anterior: " + previous.getCurrentBrokerId());
            }
        }
    }

    public void removeConnection(TopicType topic, String nodeId, MiddlewareHandlerInterface handler) {
        if (topic != null && this.clientsPerTopic.containsKey(topic)) {
            this.clientsPerTopic.get(topic).remove(handler);
        }
        this.clientsById.remove(nodeId);
        BrokerLogger.log(LogType.CONEXAO, "[BROKER] Conexão encerrada: " + nodeId);

        if (nodeId != null && nodeId.startsWith("DRN")) {
            DroneInterface fallen = this.sharedDrones.get(nodeId);

            if (fallen != null && fallen.isInUse()
                    && fallen.getPendingTaskSectorId() != null
                    && fallen.getPendingServiceType() != null) {
                BrokerLogger.log(LogType.DRONE, "[BROKER] Drone " + nodeId + " abatido. Reassinando tarefa do setor "
                        + fallen.getPendingTaskSectorId());
                this.sharedDrones.remove(nodeId);
                BrokerLogger.log(LogType.DRONE, "[BROKER] Drone removido do cluster: " + nodeId);
                Message reassign = new Message();
                reassign.setSectorId(fallen.getPendingTaskSectorId());
                reassign.setServiceType(fallen.getPendingServiceType() == ServicesTypes.VISUAL_RECONNAISSANCE
                        ? ServicesTypes.INTRUSION_DETECTION
                        : ServicesTypes.SEARCH_AND_RESCUE);
                reassign.setData(1);
                processMessage(reassign);
            } else {
                this.sharedDrones.remove(nodeId);
                BrokerLogger.log(LogType.DRONE, "[BROKER] Drone removido do cluster: " + nodeId);
            }
        }
    }

    private void checkDistributedQueue() {
        Message pending = this.filaRequisicoes.poll();
        if (pending != null) {
            BrokerLogger.log(LogType.MENSAGEM, "[BROKER] Desempilhando ocorrência da fila (Ordem de Chegada) para o setor: " + pending.getSectorId());
            processMessage(pending); 
        }
    }

    public void processMessage(Message message) {
        if (message == null) return;
        BrokerLogger.log(LogType.MENSAGEM, "[BROKER] >>> Processando: " + message);

        if ("BROKER_SIGNAL_COMPLETION".equals(message.getTargetNodeId())) {
            String droneId = message.getNodeId();
            DroneInterface oldDrone = this.sharedDrones.get(droneId);
            if (oldDrone != null) {
                DroneInterface freed = new DroneData(droneId, false, oldDrone.getCurrentBrokerId());
                boolean replaced = this.sharedDrones.replace(droneId, oldDrone, freed);
                BrokerLogger.log(LogType.DRONE, "[BROKER] Drone " + droneId + " liberado (replace=" + replaced + ")");
                
                if (replaced) {
                    checkDistributedQueue();
                }
            }
            return;
        }

        if (message.getCommandType() == CommandType.REASSIGN_SECTOR) {
            this.clusterWideBus.publish(message);
            return;
        }

        if (message.getCommandType() == CommandType.SET_DATA_TYPES) {
            this.clusterWideBus.publish(message);
            return;
        }

        if (message.getServiceType() == null) return;

        if ((message.getServiceType() == ServicesTypes.INTRUSION_DETECTION ||
             message.getServiceType() == ServicesTypes.SEARCH_AND_RESCUE) && message.getData() == 1) {

            Predicate<String, DroneInterface> condition = Predicates.equal("inUse", false);
            boolean droneFound = false;
            
            for (Map.Entry<String, DroneInterface> entry : this.sharedDrones.entrySet(condition)) {
                DroneInterface oldDrone = entry.getValue();

                DroneInterface next = new DroneData(oldDrone.getDroneId(), true, oldDrone.getCurrentBrokerId());
                next.setPendingTaskSectorId(message.getSectorId());
                next.setPendingServiceType(message.getServiceType() == ServicesTypes.INTRUSION_DETECTION
                        ? ServicesTypes.VISUAL_RECONNAISSANCE
                        : ServicesTypes.SEARCH_AND_RESCUE);
                next.setActivatedAt(System.currentTimeMillis());
                
                if (this.sharedDrones.replace(entry.getKey(), oldDrone, next)) {
                    Message cmd = new Message();
                    cmd.setTargetNodeId(next.getDroneId());
                    cmd.setCommandType(CommandType.ACTIVATE);
                    
                    String droneHomeSector = next.getDroneId().substring(next.getDroneId().lastIndexOf("-") + 1);
                    cmd.setSectorId(droneHomeSector);
                    
                    cmd.setServiceType(message.getServiceType() == ServicesTypes.INTRUSION_DETECTION
                            ? ServicesTypes.VISUAL_RECONNAISSANCE
                            : ServicesTypes.SEARCH_AND_RESCUE);
                    
                    BrokerLogger.log(LogType.DRONE, "[BROKER] Ativando drone " + next.getDroneId()
                            + " (broker físico: " + next.getCurrentBrokerId().substring(0, 8) + ")"
                            + " para " + cmd.getServiceType());
                    this.clusterWideBus.publish(cmd);
                    droneFound = true;
                    break;
                }
            }
            
            if (!droneFound) {
                this.filaRequisicoes.offer(message);
                BrokerLogger.log(LogType.ALERTA, "[BROKER] Sem drones livres. Ocorrência enviada para FILA DISTRIBUÍDA (Ordem de Chegada) - Setor: " + message.getSectorId());
            }
        } else {
            this.clusterWideBus.publish(message);
        }
    }

    private void deliverLocal(Message message) {
        if (message.getTargetNodeId() != null && this.clientsById.containsKey(message.getTargetNodeId())) {
            BrokerLogger.log(LogType.ENTREGA, "[BROKER] Entrega direta (Point-to-Point) para o recurso: " + message.getTargetNodeId());
            this.clientsById.get(message.getTargetNodeId()).sendMessage(message);
            return;
        }

        if (message.getServiceType() == null) return;
        String key = (message.getSectorId() != null ? message.getSectorId() : "DEFAULT")
                   + ":" + message.getServiceType().getDefaultTopic();
        Collection<String> ids = this.sharedTopics.get(key);
        if (ids == null || ids.isEmpty()) return;
        for (String id : ids) {
            if (message.getTargetNodeId() != null && !message.getTargetNodeId().equals(id)) {
                continue;
            }
            MiddlewareHandlerInterface local = this.clientsById.get(id);
            if (local != null) {
                BrokerLogger.log(LogType.ENTREGA, "[BROKER] Entregando Pub/Sub para " + id + " | msg: " + message);
                local.sendMessage(message);
            }
        }
    }

    public IMap<String, ClientData> getSharedClients() { return sharedClients; }
    public MultiMap<String, String> getSharedTopics() { return sharedTopics; }
    public IMap<String, DroneInterface> getSharedDrones() { return sharedDrones; }
    public ITopic<Message> getClusterWideBus() { return clusterWideBus; }
    public String getBrokerId() { return brokerId; }
}