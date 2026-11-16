package raft04;

import java.util.function.Consumer;

public final class HeartbeatMonitor {
    public enum State { DESCONHECIDO, ATIVO, SUSPEITO }

    private final int partition;
    private final long timeoutNanos;
    private final Consumer<String> log;
    private State state = State.DESCONHECIDO;
    private long lastNanos;
    private long lastTerm = -1L;
    private long hb = -1L;

    public HeartbeatMonitor(int partition, long periodMs, long factor, Consumer<String> log) {
        this.partition = partition;
        this.timeoutNanos = periodMs * factor * 1_000_000L;
        this.log = log;
        this.lastNanos = System.nanoTime();
    }

    public synchronized void onHeartbeat(long term, long leader, long hb) {
        if (term < lastTerm) {
            return;
        }
        if (term == lastTerm && hb <= this.hb) {
            return;
        }
        if (state == State.SUSPEITO) {
            log.accept("RETOMA;p" + partition + ";termo=" + term + ";hb=" + hb);
        }
        if (lastTerm != -1 && term != lastTerm) {
            log.accept("NOVO_LIDER;p" + partition + ";termo=" + term + ";lider=n" + leader);
        } else if (term == lastTerm && hb != this.hb + 1) {
            log.accept("HB_PERDIDO;p" + partition + ";esperado=" + (this.hb + 1) + ";recebido=" + hb);
        }
        this.lastTerm = term;
        this.hb = hb;
        this.lastNanos = System.nanoTime();
        this.state = State.ATIVO;
    }

    public synchronized void check() {
        long elapsed = System.nanoTime() - lastNanos;
        if (state != State.SUSPEITO && elapsed > timeoutNanos) {
            state = State.SUSPEITO;
            log.accept("SUSPEITA;p" + partition + ";silencio_ms=" + (elapsed / 1_000_000L));
        }
    }

    public synchronized State state() {
        return state;
    }
}
