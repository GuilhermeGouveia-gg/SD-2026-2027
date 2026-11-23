package tpf;

import java.util.List;
import java.util.concurrent.TimeUnit;

public final class HeartbeatMonitor {
    private final ReplicaInfo replica;

    public HeartbeatMonitor(ReplicaInfo replica) {
        this.replica = replica;
    }

    public synchronized void onHeartbeat(long usados, long quota, long agora) {
        replica.usados = usados;
        replica.quota = quota;
        replica.ultimoHeartbeat = agora;
        replica.estado = ReplicaEstado.VIVA;
    }

    public synchronized ReplicaEstado atualizarEstado(long agora) {
        if (agora - replica.ultimoHeartbeat >= Wire.MORTA_MS) {
            replica.estado = ReplicaEstado.MORTA;
        } else if (agora - replica.ultimoHeartbeat >= Wire.SUSPEITA_MS) {
            replica.estado = ReplicaEstado.SUSPEITA;
        } else {
            replica.estado = ReplicaEstado.VIVA;
        }
        return replica.estado;
    }
}
