package tpf;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public final class Catalog {
    private final Path destino;
    private final Map<String, EntradaCatalogo> entradas = new LinkedHashMap<>();

    public Catalog(Path destino) throws IOException {
        this.destino = destino;
        carregar();
    }

    public synchronized void adicionar(String chunkId, long lsnDe, long lsnAte, long tamanho) {
        entradas.putIfAbsent(chunkId, new EntradaCatalogo(lsnDe, lsnAte, tamanho, EstadoCatalogo.SELADO));
    }

    public synchronized void confirmar(String chunkId, String replica) {
        EntradaCatalogo entrada = entradas.get(chunkId);
        if (entrada != null) entrada.replicas.add(replica);
        atualizarDurabilidade(chunkId);
    }

    public synchronized Set<String> copias(String chunkId) {
        EntradaCatalogo entrada = entradas.get(chunkId);
        return entrada == null ? Set.of() : Set.copyOf(entrada.replicas);
    }

    public synchronized long tamanho(String chunkId) {
        EntradaCatalogo entrada = entradas.get(chunkId);
        return entrada == null ? 0 : entrada.tamanho;
    }

    public synchronized void atualizarDurabilidade(String chunkId) {
        EntradaCatalogo entrada = entradas.get(chunkId);
        if (entrada != null && entrada.replicas.size()
                >= Math.max(Wire.W_MIN, Wire.R / 2 + 1)) {
            entrada.estado = EstadoCatalogo.DURAVEL;
        }
    }

    public synchronized void gravarAtomico() throws IOException {
        Path temporario = destino.resolveSibling(destino.getFileName() + ".tmp");
        List<String> linhas = new ArrayList<>();
        for (var entrada : entradas.entrySet()) {
            String replicas = String.join(",", entrada.getValue().replicas);
            linhas.add(entrada.getKey() + ";" + entrada.getValue().lsnDe + ";"
                    + entrada.getValue().lsnAte + ";" + entrada.getValue().tamanho + ";"
                    + replicas + ";" + entrada.getValue().estado);
        }
        Files.writeString(temporario, linhas.isEmpty() ? "" : String.join("\n", linhas) + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        try (FileChannel canal = FileChannel.open(temporario, StandardOpenOption.WRITE)) {
            canal.force(true);
        }
        try {
            Files.move(temporario, destino, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("Move atomico nao suportado", e);
        }
    }

    private void carregar() throws IOException {
        if (!Files.exists(destino)) return;
        for (String linha : Files.readAllLines(destino, StandardCharsets.UTF_8)) {
            if (linha.isBlank()) continue;
            String[] c = linha.split(";", -1);
            if (c.length != 6) throw new IOException("Linha invalida em catalogo.txt");
            EntradaCatalogo entrada = new EntradaCatalogo(
                    Long.parseLong(c[1]), Long.parseLong(c[2]), Long.parseLong(c[3]),
                    EstadoCatalogo.valueOf(c[5]));
            if (!c[4].isBlank()) Collections.addAll(entrada.replicas, c[4].split(","));
            entradas.put(c[0], entrada);
        }
    }

    public synchronized List<String> linhasEstado() {
        List<String> linhas = new ArrayList<>();
        for (var entrada : entradas.entrySet()) {
            EntradaCatalogo e = entrada.getValue();
            linhas.add(entrada.getKey() + ";" + e.estado + ";" + e.replicas.size() + "/" + Wire.R);
        }
        return linhas;
    }

    public synchronized int copiasReplicas(String replicaId) {
        int total = 0;
        for (EntradaCatalogo entrada : entradas.values()) {
            if (entrada.replicas.contains(replicaId)) total++;
        }
        return total;
    }
}
