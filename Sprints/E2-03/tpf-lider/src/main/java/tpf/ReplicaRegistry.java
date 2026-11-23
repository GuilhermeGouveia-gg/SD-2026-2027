package tpf;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class ReplicaRegistry {
    private final Map<String, ReplicaInfo> replicas = new ConcurrentHashMap<>();
    private final Map<String, HeartbeatMonitor> monitores = new ConcurrentHashMap<>();

    public synchronized ReplicaInfo info(String id) {
        ReplicaInfo info = replicas.computeIfAbsent(id, ReplicaInfo::new);
        monitores.computeIfAbsent(id, k -> new HeartbeatMonitor(info));
        return info;
    }

    public synchronized void onHeartbeat(String id, long usados, long quota, long agora) {
        ReplicaInfo info = info(id);
        monitores.get(id).onHeartbeat(usados, quota, agora);
    }

    public synchronized void atualizarEstados(long agora) {
        for (Map.Entry<String, ReplicaInfo> entrada : replicas.entrySet()) {
            ReplicaInfo info = entrada.getValue();
            ReplicaEstado antigo = info.estado;
            ReplicaEstado novo = monitores.get(entrada.getKey()).atualizarEstado(agora);
            if (novo != antigo) {
                System.out.println("ESTADO " + entrada.getKey() + " " + antigo + " -> " + novo);
            }
        }
    }

    public synchronized List<String> reserveAndChoose(String chunkId, long tamanho,
                                                       int count, Set<String> exclude) {
        List<ReplicaInfo> candidatas = new ArrayList<>();
        for (ReplicaInfo info : replicas.values()) {
            if (!info.id.isBlank() && info.estado != ReplicaEstado.MORTA
                    && !exclude.contains(info.id)) candidatas.add(info);
        }
        List<String> escolhidas = Placement.choose(chunkId, tamanho, count, candidatas, exclude);
        for (String id : escolhidas) {
            ReplicaInfo info = replicas.get(id);
            info.reservados += tamanho;
        }
        return escolhidas;
    }

    public synchronized void confirmarReserva(String replica, long tamanho) {
        ReplicaInfo info = replicas.get(replica);
        if (info == null) return;
        info.reservados = Math.max(0, info.reservados - tamanho);
        info.usados += tamanho;
    }

    public synchronized void libertar(String replica, long tamanho) {
        ReplicaInfo info = replicas.get(replica);
        if (info != null) info.reservados = Math.max(0, info.reservados - tamanho);
    }

    public synchronized Collection<ReplicaInfo> todas() {
        return new ArrayList<>(replicas.values());
    }

    public synchronized void remover(String id) {
        replicas.remove(id);
        monitores.remove(id);
    }
}
