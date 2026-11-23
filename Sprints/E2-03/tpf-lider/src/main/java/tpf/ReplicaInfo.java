package tpf;

import java.util.concurrent.atomic.AtomicReference;

public final class ReplicaInfo {
    public final String id;
    public volatile ReplicaEstado estado = ReplicaEstado.MORTA;
    public volatile long quota;
    public volatile long usados;
    public volatile long reservados;
    public volatile long ultimoHeartbeat;

    public ReplicaInfo(String id) {
        this.id = id;
    }

    public synchronized long livre() {
        return Math.max(0, quota - usados - reservados);
    }
}
