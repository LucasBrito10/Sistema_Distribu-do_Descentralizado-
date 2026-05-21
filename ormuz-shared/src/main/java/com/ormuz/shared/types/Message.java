package com.ormuz.shared.types;

import java.io.Serializable;
import com.ormuz.shared.enums.CommandType;
import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.enums.TopicType;

/**
 * BUG CORRIGIDO: topicType era inicializado inline com "= serviceType.getDefaultTopic()"
 * que sempre avalia para null pois serviceType é null no momento da instância.
 * Agora topicType é derivado lazily via getter.
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    private ServicesTypes serviceType;
    private String connectionType;
    private String nodeId;
    private int data = -1;
    private CommandType commandType;
    private String targetNodeId;
    private String sectorId;
    // Lista de ServicesTypes habilitados — usada pelo comando SET_DATA_TYPES
    private java.util.List<ServicesTypes> dataTypes;

    public Message() {}

    // BUG CORRIGIDO: deriva o topicType sempre a partir do serviceType atual
    public TopicType getTopicType() {
        return serviceType != null ? serviceType.getDefaultTopic() : null;
    }
    // Mantido para Jackson (deserialização) – valor ignorado, derivado de serviceType
    public void setTopicType(TopicType topicType) {}

    public ServicesTypes getServiceType() { return serviceType; }
    public void setServiceType(ServicesTypes s) { this.serviceType = s; }

    public String getConnectionType() { return connectionType; }
    public void setConnectionType(String c) { this.connectionType = c; }

    public String getNodeId() { return nodeId; }
    public void setNodeId(String n) { this.nodeId = n; }

    public int getData() { return data; }
    public void setData(int d) { this.data = d; }

    public CommandType getCommandType() { return commandType; }
    public void setCommandType(CommandType c) { this.commandType = c; }

    public String getTargetNodeId() { return targetNodeId; }
    public void setTargetNodeId(String t) { this.targetNodeId = t; }

    public String getSectorId() { return sectorId; }
    public void setSectorId(String s) { this.sectorId = s; }

    public java.util.List<ServicesTypes> getDataTypes() { return dataTypes; }
    public void setDataTypes(java.util.List<ServicesTypes> d) { this.dataTypes = d; }

    @Override
    public String toString() {
        return "Message{service=" + serviceType +
               ", node='" + nodeId + '\'' +
               ", sector='" + sectorId + '\'' +
               ", data=" + data +
               ", cmd=" + commandType +
               ", target='" + targetNodeId + '\'' +
               ", conn='" + connectionType + '\'' + '}';
    }
}
