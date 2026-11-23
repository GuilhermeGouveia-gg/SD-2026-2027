package tpf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public final class ReplicationCoordinator {
    private final Path pasta;
    private final PubSubHub hub;
    private final ReplicaRegistry registry;
    private final Catalog catalog;
    private final ScheduledExecutorService agenda =
            Executors.newScheduledThreadPool(2);
    private final Map<String, EstadoChunk> estados = new ConcurrentHashMap<>();

    private static final class EstadoChunk {
        final Set<String> tentadas = Collections.synchronizedSet(new LinkedHashSet<>());
        final Map<String, Long> pendentes = new ConcurrentHashMap<>();
        int ronda;
        final long tamanho;
        EstadoChunk(long tamanho) { this.tamanho = tamanho; }
    }

    public ReplicationCoordinator(Path pasta, PubSubHub hub,
                                  ReplicaRegistry registry, Catalog catalog) {
        this.pasta = pasta;
        this.hub = hub;
        this.registry = registry;
        this.catalog = catalog;
    }

    public synchronized void selado(String chunkId, long lsnDe, long lsnAte, long tamanho) {
        catalog.adicionar(chunkId, lsnDe, lsnAte, tamanho);
        persistirCatalogo();
        EstadoChunk estado = new EstadoChunk(tamanho);
        estados.put(chunkId, estado);
        iniciarRonda(chunkId, estado);
    }

    private void iniciarRonda(String chunkId, EstadoChunk estado) {
        if (estado.ronda >= Wire.MAX_RONDAS) return;
        estado.ronda++;
        int confirmadas = catalog.copias(chunkId).size();
        if (confirmadas >= Wire.R) return;

        List<String> escolhidas = registry.reserveAndChoose(
                chunkId, estado.tamanho, Wire.R - confirmadas, estado.tentadas);
        estado.tentadas.addAll(escolhidas);
        if (escolhidas.isEmpty()) {
            subreplicado(chunkId, confirmadas);
            agenda.schedule(() -> timeout(chunkId),
                    Wire.ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return;
        }

        for (String id : escolhidas) {
            estado.pendentes.put(id, estado.tamanho);
        }
        for (String id : escolhidas) enviarStore(chunkId, id);
        subreplicado(chunkId, confirmadas);
        System.out.println("RONDA " + chunkId + " " + estado.ronda);
        agenda.schedule(() -> timeout(chunkId),
                Wire.ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void enviarStore(String chunkId, String id) {
        try {
            Path bin = pasta.resolve("selados").resolve(chunkId + ".bin");
            Path hdr = pasta.resolve("selados").resolve(chunkId + ".hdr");
            String binB64 = Base64.getEncoder().encodeToString(Files.readAllBytes(bin));
            String hdrB64 = Base64.getEncoder().encodeToString(Files.readAllBytes(hdr));
            hub.publish("tpf-r-" + id,
                    "STORE;" + chunkId + ";" + hdrB64 + ";" + binB64);
            System.out.println("STORE " + chunkId + " -> " + id);
        } catch (IOException e) {
            System.err.println("ERRO STORE " + chunkId + " -> " + id + ": " + e.getMessage());
        }
    }

    private synchronized void timeout(String chunkId) {
        EstadoChunk estado = estados.get(chunkId);
        if (estado == null) return;
        for (var entrada : estado.pendentes.entrySet()) {
            registry.libertar(entrada.getKey(), entrada.getValue());
        }
        estado.pendentes.clear();

        int confirmadas = catalog.copias(chunkId).size();
        if (confirmadas < Wire.R && estado.ronda < Wire.MAX_RONDAS) {
            iniciarRonda(chunkId, estado);
        } else {
            subreplicado(chunkId, confirmadas);
        }
    }

    public synchronized boolean receberAck(String replicaId, String chunkId, String sha) {
        EstadoChunk estado = estados.get(chunkId);
        if (estado == null || !estado.pendentes.containsKey(replicaId)
                || !chunkId.equals(sha)) return false;
        Long tamanho = estado.pendentes.remove(replicaId);
        if (tamanho == null) return false;

        catalog.confirmar(chunkId, replicaId);
        registry.confirmarReserva(replicaId, tamanho);
        persistirCatalogo();
        estado.tentadas.add(replicaId);
        System.out.println("ACK " + chunkId + " " + replicaId);
        if (catalogoDuravel(chunkId)) {
            System.out.println("DURAVEL " + chunkId);
        }
        subreplicado(chunkId, catalog.copias(chunkId).size());
        return true;
    }

    private boolean catalogoDuravel(String chunkId) {
        int n = catalog.copias(chunkId).size();
        return n >= Wire.W_MIN && n >= Wire.R / 2 + 1;
    }

    private void subreplicado(String chunkId, int confirmadas) {
        if (confirmadas < Wire.R) {
            System.out.println("SUBREPLICADO " + chunkId + " " + confirmadas + "/" + Wire.R);
        }
    }

    private void persistirCatalogo() {
        try {
            catalog.gravarAtomico();
        } catch (IOException e) {
            System.err.println("ERRO catalogo " + e.getMessage());
        }
    }

    public void fechar() {
        agenda.shutdownNow();
    }
}
