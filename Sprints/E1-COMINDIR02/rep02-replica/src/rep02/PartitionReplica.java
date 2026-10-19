package rep02;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

/** Cada partição tem a sua thread, ficheiro, sequência e conjunto de retidos. */
public final class PartitionReplica {
    private final int k;
    private final RecordFile file;
    private final TreeMap<Long, SensorRecord> retained = new TreeMap<>();
    private final double loss;
    private final long discardSeq;
    private boolean discardedOnce;
    private long lastSeq;
    private String host;
    private int port;
    private long receivedPubSub;
    private long receivedTcp;
    private long discarded;
    private long duplicates;
    private long written;
    private long tcpAttempts;
    private long tcpConnections;
    private long firstWrite;
    private long lastWrite;

    private PartitionReplica(String id, int k, double loss, long discardSeq) throws IOException {
        this.k = k;
        this.loss = loss;
        this.discardSeq = discardSeq;
        file = new RecordFile("replica-" + id + "-p" + k + ".txt");
        lastSeq = file.lastSeq();
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Uso: PartitionReplica <id> <partições> [portoApi]");
            return;
        }
        try {
            if (!args[0].matches("[A-Za-z0-9_-]+")) {
                throw new IllegalArgumentException("Identificador de réplica inválido.");
            }
            int apiPort = args.length > 2 ? Integer.parseInt(args[2]) : 5001;
            if (apiPort < 1 || apiPort > 65535) {
                throw new IllegalArgumentException("Porto da API inválido.");
            }
            double loss = Double.parseDouble(System.getProperty("perda", "0"));
            long discardSeq = Long.parseLong(System.getProperty("descartar", "-1"));
            if (!Double.isFinite(loss) || loss < 0 || loss > 1
                    || (discardSeq != -1 && discardSeq < 1)) {
                throw new IllegalArgumentException("Usa perda entre 0 e 1 e descartar >= 1.");
            }
            List<PartitionReplica> partitions = new ArrayList<>();
            Set<Integer> seen = new HashSet<>();
            for (String value : args[1].split(",", -1)) {
                int k = Integer.parseInt(value.trim());
                if (k < 0 || k > 65535 - Wire.PORTO_BASE || !seen.add(k)) {
                    throw new IllegalArgumentException("Partição inválida ou repetida: " + value);
                }
                partitions.add(new PartitionReplica(args[0], k, loss, discardSeq));
            }
            String api = "http://127.0.0.1:" + apiPort + "/api/v0";
            // Validar tudo antes de arrancar a primeira subscrição.
            for (PartitionReplica partition : partitions) {
                new Thread(() -> partition.run(api), "replica-" + args[0] + "-p" + partition.k).start();
            }
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("RÉPLICA FALHOU: " + e.getMessage());
        }
    }

    private void run(String api) {
        System.out.println("RÉPLICA p" + k + " | Último seq: " + lastSeq + " | API: " + api);
        try {
            new IpfsPubSub(api).subscribe(Wire.topicData(k), this::receive);
            System.err.println("SUBSCRIÇÃO TERMINOU p" + k);
        } catch (IOException e) {
            System.err.println("SUBSCRIÇÃO FALHOU p" + k + ": " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void receive(String message) {
        try {
            String[] parts = Wire.regParts(message);
            int announcedPort = Integer.parseInt(parts[1]);
            SensorRecord record = parseRecord(parts[2]);
            if (parts[0].isBlank() || announcedPort < 1 || announcedPort > 65535) {
                throw new IllegalArgumentException("Endereço/porto inválido");
            }
            // Guardar o anúncio mais recente, incluindo após um reinício do líder.
            host = parts[0];
            port = announcedPort;
            receivedPubSub++;
            boolean selected = !discardedOnce && record.getSeq() == discardSeq;
            if (selected) discardedOnce = true;
            // Só a via PubSub sofre perdas simuladas; o histórico TCP é aplicado sempre.
            if (selected || ThreadLocalRandom.current().nextDouble() < loss) {
                discarded++;
                System.out.println("DESCARTADO (simulação): " + record.getSeq() + " | p" + k);
            } else {
                apply(record);
            }
            // Também permite repetir um pedido falhado quando chega outro REG.
            recoverIfNeeded();
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + message + " | p" + k);
        } catch (IOException e) {
            System.out.println("IO p" + k + ": " + e.getMessage());
        }
        status();
    }

    private static SensorRecord parseRecord(String line) {
        SensorRecord r = SensorRecord.fromLine(line);
        if (r.getSeq() < 1 || r.getSensorId().isBlank() || !Double.isFinite(r.getTemperature())
                || r.getTimestamp().isBlank()) {
            throw new IllegalArgumentException("Registo inválido");
        }
        return r;
    }

    private void apply(SensorRecord record) throws IOException {
        long seq = record.getSeq();
        if (seq <= lastSeq || retained.containsKey(seq)) {
            duplicates++;
            System.out.println("DUPLICADO: " + seq + " | p" + k);
        } else if (seq == lastSeq + 1) {
            append(record);
            drain();
        } else {
            retained.put(seq, record);
            System.out.println("EM ESPERA: " + seq + " (falta " + (lastSeq + 1) + ") | p" + k);
        }
    }

    private void append(SensorRecord record) throws IOException {
        file.append(record);
        // Não avançar em memória antes de a escrita local terminar.
        lastSeq = record.getSeq();
        written++;
        lastWrite = System.currentTimeMillis();
        if (firstWrite == 0) firstWrite = lastWrite;
        System.out.println("ESCRITO p" + k + ": " + lastSeq + " | Instante: " + lastWrite);
    }

    private void drain() throws IOException {
        while (retained.containsKey(lastSeq + 1)) {
            SensorRecord next = retained.get(lastSeq + 1);
            append(next);
            // Só remover depois de a escrita ter sucesso.
            retained.remove(next.getSeq());
        }
    }

    private void recoverIfNeeded() {
        // Após uma recuperação podem continuar a existir outras lacunas.
        while (retained.size() > Wire.LIMITE_RETIDOS) {
            long from = lastSeq + 1;
            long to = retained.firstKey() - 1;
            try {
                recover(from, to);
                drain();
            } catch (IOException | IllegalArgumentException e) {
                System.out.println("RECUPERAÇÃO FALHOU: " + e.getMessage() + " | p" + k);
                // Não apagar nada. A chegada seguinte volta a avaliar a condição.
                return;
            }
        }
    }

    private void recover(long from, long to) throws IOException {
        tcpAttempts++;
        System.out.println("RECUPERAÇÃO: " + from + ".." + to + " pedida a "
                + host + ":" + port + " | p" + k);
        // A própria thread desta partição fica bloqueada; as outras continuam.
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), Wire.T_TCP_MS);
            tcpConnections++;
            socket.setSoTimeout(Wire.T_TCP_MS);
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
                    socket.getOutputStream(), StandardCharsets.UTF_8));
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    socket.getInputStream(), StandardCharsets.UTF_8));
            out.write("PEDIDO " + from + " " + to);
            out.newLine();
            out.flush();
            String line;
            while ((line = in.readLine()) != null) {
                if (line.equals("FIM")) {
                    if (lastSeq < to) throw new IOException("FIM antes de completar o intervalo");
                    return;
                }
                if (line.startsWith("ERRO")) throw new IOException(line);
                SensorRecord record = parseRecord(line);
                if (record.getSeq() < from || record.getSeq() > to) {
                    throw new IOException("Registo fora do intervalo pedido: " + record.getSeq());
                }
                receivedTcp++;
                System.out.println("RECUPERADO: " + record.getSeq() + " | p" + k);
                apply(record);
            }
            throw new IOException("Ligação interrompida antes de FIM");
        }
    }

    private void status() {
        System.out.println("ESTADO p" + k + " | Último seq: " + lastSeq
                + " | PubSub: " + receivedPubSub + " | Descartados: " + discarded
                + " | Duplicados: " + duplicates + " | Escritos: " + written
                + " | Retidos: " + retained.size() + " | Tentativas TCP: " + tcpAttempts
                + " | Ligações TCP: " + tcpConnections + " | Registos TCP: " + receivedTcp
                + " | Primeira escrita: " + firstWrite + " | Última escrita: " + lastWrite);
    }
}
