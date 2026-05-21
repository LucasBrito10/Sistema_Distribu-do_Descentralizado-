package com.ormuz.broker.log;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.EnumSet;
import java.util.Set;

/**
 * Controle centralizado de logs para todos os brokers do sistema ORMUZ.
 * Permite habilitar ou desabilitar categorias específicas de log em tempo de
 * execução. Cada linha inclui timestamp e prefixo colorido por categoria.
 */
public class BrokerLogger {

    /** Categorias de log disponíveis no broker. */
    public enum LogType {
        /** Inicialização, porta e status geral do servidor. */
        SISTEMA,
        /** Eventos de conexão, desconexão e registro de clientes. */
        CONEXAO,
        /** Entrada e saída de membros no cluster Hazelcast. */
        CLUSTER,
        /** Recebimento e roteamento de mensagens (processMessage). */
        MENSAGEM,
        /** Publicações e recebimentos no canal global (cluster-wide bus). */
        BUS,
        /** Entrega local de mensagens a subscribers deste broker. */
        ENTREGA,
        /** Registro, ativação, liberação e recuperação de drones. */
        DRONE,
        /** Alertas críticos que sempre devem ser visíveis. */
        ALERTA,
        /** Reassociação de recursos entre setores. */
        REASSOCIAR
    }

    // Prefixos coloridos ANSI para cada categoria (funciona em terminais UNIX/Docker)
    private static final java.util.Map<LogType, String> PREFIXOS = new java.util.EnumMap<>(LogType.class);
    static {
        PREFIXOS.put(LogType.SISTEMA,    "\u001B[36m[SISTEMA  ]\u001B[0m"); // ciano
        PREFIXOS.put(LogType.CONEXAO,    "\u001B[34m[CONEXAO  ]\u001B[0m"); // azul
        PREFIXOS.put(LogType.CLUSTER,    "\u001B[35m[CLUSTER  ]\u001B[0m"); // magenta
        PREFIXOS.put(LogType.MENSAGEM,   "\u001B[37m[MENSAGEM ]\u001B[0m"); // branco
        PREFIXOS.put(LogType.BUS,        "\u001B[33m[BUS      ]\u001B[0m"); // amarelo
        PREFIXOS.put(LogType.ENTREGA,    "\u001B[32m[ENTREGA  ]\u001B[0m"); // verde
        PREFIXOS.put(LogType.DRONE,      "\u001B[96m[DRONE    ]\u001B[0m"); // ciano claro
        PREFIXOS.put(LogType.ALERTA,     "\u001B[91m[ALERTA   ]\u001B[0m"); // vermelho claro
        PREFIXOS.put(LogType.REASSOCIAR, "\u001B[93m[REASSOC  ]\u001B[0m"); // amarelo claro
    }

    private static final DateTimeFormatter HORA_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    // Por padrão, todos os tipos de log estão habilitados
    private static final Set<LogType> tiposHabilitados = EnumSet.allOf(LogType.class);

    /** Habilita uma categoria específica de log. */
    public static void habilitar(LogType tipo) { tiposHabilitados.add(tipo); }

    /** Desabilita uma categoria específica de log. */
    public static void desabilitar(LogType tipo) { tiposHabilitados.remove(tipo); }

    /** Habilita todas as categorias de log. */
    public static void habilitarTodos() { tiposHabilitados.addAll(EnumSet.allOf(LogType.class)); }

    /** Desabilita todas as categorias de log. */
    public static void desabilitarTodos() { tiposHabilitados.clear(); }

    /** Retorna true se a categoria informada está habilitada. */
    public static boolean estaHabilitado(LogType tipo) { return tiposHabilitados.contains(tipo); }

    /**
     * Imprime a mensagem em stdout com timestamp e prefixo colorido,
     * se a categoria estiver habilitada.
     */
    public static void log(LogType tipo, String mensagem) {
        if (tiposHabilitados.contains(tipo)) {
            String hora = LocalTime.now().format(HORA_FMT);
            String prefixo = PREFIXOS.getOrDefault(tipo, "[" + tipo.name() + "]");
            System.out.println(hora + " " + prefixo + " " + mensagem);
        }
    }

    /**
     * Imprime a mensagem em stderr com timestamp e prefixo colorido,
     * se a categoria estiver habilitada.
     */
    public static void err(LogType tipo, String mensagem) {
        if (tiposHabilitados.contains(tipo)) {
            String hora = LocalTime.now().format(HORA_FMT);
            String prefixo = PREFIXOS.getOrDefault(tipo, "[" + tipo.name() + "]");
            System.err.println(hora + " " + prefixo + " " + mensagem);
        }
    }
}
