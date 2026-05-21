package com.ormuz.client.drone;

import com.ormuz.client.core.Client;
import com.ormuz.shared.enums.CommandType;
import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.interfaces.DroneInterface;
import com.ormuz.shared.types.Message;

public class Drone extends Client implements DroneInterface {
    private static final long serialVersionUID = 1L;
    private String droneId;
    private boolean inUse;
    private String currentBrokerId;
    private ServicesTypes pendingServiceType = null;
    private String pendingTaskSectorId = null;

    // Guarda de reentrada: impede dupla ativação caso o broker reentregue o ACTIVATE
    private volatile boolean missionActive = false;

    public Drone(String droneId, boolean inUse, String currentBrokerId) {
        this.droneId = droneId;
        this.inUse = inUse;
        this.currentBrokerId = currentBrokerId;
        this.setId(droneId);
    }

    @Override
    protected void handleIncomingMessage(Message msg) {
        if (msg.getCommandType() == CommandType.ACTIVATE) {
            // Idempotência: ignora ACTIVATE se já estiver em missão
            if (missionActive) return;
            missionActive = true;
            if (msg.getServiceType() == ServicesTypes.VISUAL_RECONNAISSANCE) {
                visualReconnaissanceService();
            } else if (msg.getServiceType() == ServicesTypes.SEARCH_AND_RESCUE) {
                searchAndRescueService();
            } else if (msg.getServiceType() == ServicesTypes.INFRASTRUCTURE_INSPECTION) {
                infrastructureInspectionService();
            }
        }
    }

    private void executeTask(ServicesTypes service, String taskName) {
        new Thread(() -> {
            try {
                System.out.println("\n[DRONE " + droneId + "] Iniciando: " + taskName);
                Thread.sleep(15000);
                System.out.println("[DRONE " + droneId + "] " + taskName + " concluído. Retornando base.\n");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("[DRONE " + droneId + "] Tarefa interrompida. Retornando base.\n");
            } finally {
                // Garante notificação ao broker em qualquer cenário (conclusão OU interrupção)
                missionActive = false;
                this.sendResponse(0, service, "BROKER_SIGNAL_COMPLETION");
            }
        }).start();
    }

    @Override public void visualReconnaissanceService() { executeTask(ServicesTypes.VISUAL_RECONNAISSANCE, "Reconhecimento Visual"); }
    @Override public void searchAndRescueService() { executeTask(ServicesTypes.SEARCH_AND_RESCUE, "Busca e Resgate"); }
    @Override public void infrastructureInspectionService() { executeTask(ServicesTypes.INFRASTRUCTURE_INSPECTION, "Inspeção de Infraestrutura"); }
    


    @Override
    public void setPendingTaskSectorId(String sectorId) {this.pendingTaskSectorId = sectorId;}

    
    @Override
    public String getPendingTaskSectorId() {return this.pendingTaskSectorId;}
    @Override public ServicesTypes getPendingServiceType() {return pendingServiceType;}

    @Override public void setPendingServiceType(ServicesTypes pendingServiceType) {this.pendingServiceType = pendingServiceType;}

    @Override public String getDroneId() { return droneId; }
    @Override public void setDroneId(String droneId) { this.droneId = droneId; }
    @Override public boolean isInUse() { return inUse; }
    @Override public void setInUse(boolean inUse) { this.inUse = inUse; }
    @Override public String getCurrentBrokerId() { return currentBrokerId; }
    @Override public void setCurrentBrokerId(String currentBrokerId) { this.currentBrokerId = currentBrokerId; }
    // activatedAt é gerenciado exclusivamente pelo broker via DroneData no Hazelcast.
    // O Drone cliente nunca lê nem grava esse campo — stubs necessários pela interface.
    @Override public Long getActivatedAt() { return null; }
    @Override public void setActivatedAt(Long activatedAt) {}
}