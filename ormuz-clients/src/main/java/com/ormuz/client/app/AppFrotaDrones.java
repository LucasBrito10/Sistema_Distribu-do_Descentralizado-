package com.ormuz.client.app;

import java.util.concurrent.atomic.AtomicInteger;

import com.ormuz.client.core.Client;
import com.ormuz.client.drone.DroneBuilder;
import com.ormuz.shared.enums.CommandType;
import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.types.Message;

public class AppFrotaDrones extends Client {
    private static String clusterIpsGlobal;
    private static int portaBrokerGlobal;
    private static String setorGlobal;

    // AtomicInteger evita race condition: dois comandos ACTIVATE simultâneos
    // gerariam o mesmo ID de drone com indexUnico++ não sincronizado
    private static final AtomicInteger indexUnico = new AtomicInteger(1);

    @Override
    protected void handleIncomingMessage(Message msg) {
        if (msg.getCommandType() == CommandType.ACTIVATE) {
            String droneId = "DRN-DINAMICO-" + indexUnico.getAndIncrement() + "-" + setorGlobal;

            new DroneBuilder()
                    .withId(droneId)
                    .withSector(setorGlobal)
                    .withConnection(clusterIpsGlobal, portaBrokerGlobal)
                    .build();
            System.out.printf("[%s] [NODETYPE:DRONE] [SISTEMA] Unidade %s instanciada com suporte a failover distribuído.%n",
                    setorGlobal, droneId);

        } else if (msg.getCommandType() == CommandType.REASSIGN_SECTOR
                && getId().equals(msg.getTargetNodeId())) {

            String novoSetor  = msg.getSectorId();
            String novoBroker = msg.getNodeId();
            System.out.printf("[%s] [SISTEMA] Fábrica de drones reassociada para setor %s (broker: %s)%n",
                    setorGlobal, novoSetor, novoBroker);

            setorGlobal = novoSetor;
            // Preserva a lista completa do cluster: substitui apenas o primeiro
            // elemento (broker preferencial) mantendo os demais como fallback.
            // Isso garante que drones criados após a reassociação ainda tenham
            // acesso a toda a lista de failover e não fiquem com apenas 1 broker.
            String[] partes = clusterIpsGlobal.split(",");
            partes[0] = novoBroker;
            clusterIpsGlobal = String.join(",", partes);
            setSectorId(novoSetor);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        clusterIpsGlobal  = args.length > 0 ? args[0] : "127.0.0.1";
        portaBrokerGlobal = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
        setorGlobal       = args.length > 2 ? args[2] : "SETOR_NORTE";

        AppFrotaDrones fabrica = new AppFrotaDrones();
        fabrica.setId("FACTORY-DRONES-" + setorGlobal);
        fabrica.setSectorId(setorGlobal);
        fabrica.startSubscribe(clusterIpsGlobal, portaBrokerGlobal, ServicesTypes.VISUAL_RECONNAISSANCE);

        System.out.printf("[FABRICA] Container de provisionamento de Drones online para o %s.%n", setorGlobal);
        while (true) {
            try { Thread.sleep(60000); } catch (InterruptedException e) { break; }
        }
    }
}
