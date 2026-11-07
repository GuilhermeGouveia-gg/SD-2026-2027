package raft04;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class Wire {
    public static final long PERIODO_HB_MS = Long.getLong("raft04.periodo", 2000L);
    public static final long FATOR_TIMEOUT = Long.getLong("raft04.fator", 3L);
    public static final long VERIFICACAO_MS = 200L;
    public static final long ELEICAO_MIN_MS = Long.getLong("raft04.eleicaoMin", 3L * PERIODO_HB_MS);
    public static final long ELEICAO_MAX_MS = Long.getLong("raft04.eleicaoMax", 6L * PERIODO_HB_MS);
    public static final double LIMIAR_ALARME = 28.0;
    public static final int GRUPO = 3;

    private Wire() {
    }

    public static String topicIn(int partition) {
        return "raft04-in-p" + partition;
    }

    public static String topicData(int partition) {
        return "raft04-p" + partition;
    }

    public static String topicElection(int partition) {
        return "raft04-eleicao-p" + partition;
    }

    public static int nodePort(int partition, int nodeIndex) {
        return 7000 + (partition * 100) + nodeIndex;
    }

    public static boolean isolated(int partition, int nodeIndex) {
        Path p = Paths.get("isolar-p" + partition + "-n" + nodeIndex);
        return Files.exists(p);
    }

    public static String encodeVoteRequest(long term, int candidate, long lastSeq, long lastTerm) {
        return "PEDIDO_VOTO;" + term + ";" + candidate + ";" + lastSeq + ";" + lastTerm;
    }

    public static String encodeVoteReply(long term, int voter, int candidate, boolean granted) {
        return "VOTO;" + term + ";" + voter + ";" + candidate + ";" + (granted ? "S" : "N");
    }

    public static String encodeBatch(long term, int leader, String address, int port,
                                    long hb, long lastSeq, long lastTerm, int count) {
        return "LOTE;" + term + ";" + leader + ";" + address + ";" + port + ";"
                + hb + ";" + lastSeq + ";" + lastTerm + ";" + count;
    }

    public static String in(String sensor, double temperature, long lamport, String vector) {
        return "IN;" + sensor + ";" + temperature + ";" + lamport + ";" + vector;
    }
}
