package raft04;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class PartitionNode {
    private final int partition;
    private final int self;
    private final int groupSize;
    private final Path workDir;
    private final Path stateFile;
    private final Path dataFile;
    private final Path eventFile;
    private final Path hbFile;
    private final Object nodeMonitor = new Object();
    private final List<SensorRecord> pendentes = new ArrayList<>();
    private final RaftState raft;
    private volatile boolean running = true;
    private volatile boolean isolated = false;
    private ScheduledExecutorService scheduler;

    private final IpfsPubSub ipfs =
            new IpfsPubSub("http://127.0.0.1:5001/api/v0");

    private long ultimoSeq = 0L;
    private long ultimoTermo = 0L;
    private long seq = 0L;
    private long L = 0L;
    private long[] V;
    private long hb = 0L;

    public PartitionNode(int partition, int self, int groupSize, Path workDir) throws IOException {
        this.partition = partition;
        this.self = self;
        this.groupSize = groupSize;
        this.workDir = workDir.toAbsolutePath().normalize();
        Files.createDirectories(this.workDir);
        this.stateFile = this.workDir.resolve("estado-p" + partition + "-n" + self + ".txt");
        this.dataFile = this.workDir.resolve("no-p" + partition + "-n" + self + ".txt");
        this.eventFile = this.workDir.resolve("eventos-p" + partition + "-n" + self + ".txt");
        this.hbFile = this.workDir.resolve("hb-p" + partition + "-n" + self + ".txt");
        V = new long[3];
        loadExistingData();
        this.raft = new RaftState(partition, self, groupSize,
                Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS, stateFile, this::logEvent);
        subscribe(Wire.topicIn(partition), this::handleInput);
        subscribe(Wire.topicData(partition), this::handleBatch);
        subscribe(Wire.topicElection(partition), this::handleElectionMessage);
        startRecoveryServer();
        startVerificationThread();
        logEvent("ARRANQUE;p" + partition + ";n" + self + ";termo=" + raft.term());
    }

    private void loadExistingData() throws IOException {
        if (!Files.exists(dataFile)) {
            return;
        }
        for (String line : Files.readAllLines(dataFile, StandardCharsets.UTF_8)) {
            if (line == null || line.isBlank()) {
                continue;
            }
            SensorRecord rec = SensorRecord.fromLine(line);
            ultimoSeq = rec.getSeq();
            ultimoTermo = rec.getTerm();

            L = rec.getLamport();
            V = SensorRecord.parseVector(rec.getVector(), 3);
        }
    }

    public void subscribe(String topic, Consumer<String> handler) {
        Thread thread = new Thread(() -> {
            try {
                ipfs.subscribe(topic, message -> {
                    boolean canalEntreNos =
                            topic.equals(Wire.topicData(partition))
                                    || topic.equals(Wire.topicElection(partition));

                    if (canalEntreNos && isIsolated()) {
                        return;
                    }

                    try {
                        handler.accept(message);
                    } catch (RuntimeException e) {
                        logEvent("ERRO;p" + partition
                                + ";rececao=" + e.getMessage());
                    }
                });
            } catch (IOException e) {
                logEvent("ERRO;p" + partition
                        + ";subscricao=" + e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "sub-p" + partition + "-n" + self + "-" + topic);

        thread.setDaemon(true);
        thread.start();
    }

    public void publish(String topic, String message) {
        if (topic == null || topic.isBlank()
                || message == null || message.isBlank()) {
            return;
        }

        boolean canalEntreNos =
                topic.equals(Wire.topicData(partition))
                        || topic.equals(Wire.topicElection(partition));

        if (canalEntreNos && isIsolated()) {
            return;
        }

        try {
            ipfs.publish(topic, message);
        } catch (IOException e) {
            logEvent("ERRO;p" + partition
                    + ";publicacao=" + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void receive(String message) {
        if (message == null || message.isBlank()) {
            return;
        }
        handleMessage(message.trim());
    }

    private void handleMessage(String message) {
        if (message.startsWith("IN;")) {
            handleInput(message);
        } else if (message.startsWith("PEDIDO_VOTO;")) {
            handleVoteRequest(message);
        } else if (message.startsWith("VOTO;")) {
            handleVoteReply(message);
        } else if (message.startsWith("LOTE;")) {
            handleBatch(message);
        }
    }

    private void handleElectionMessage(String message) {
        if (message.startsWith("PEDIDO_VOTO;")) {
            handleVoteRequest(message);
        } else if (message.startsWith("VOTO;")) {
            handleVoteReply(message);
        }
    }

    private void handleInput(String message) {
        if (message == null || !message.startsWith("IN;")) {
            return;
        }

        String[] parts = message.split(";", -1);

        if (parts.length != 5) {
            logEvent("IN_INVALIDO;p" + partition + ";" + message);
            return;
        }

        try {
            String sensor = parts[1];
            double temperature = Double.parseDouble(parts[2]);
            long receivedLamport = Long.parseLong(parts[3]);
            long[] receivedVector =
                    SensorRecord.parseVector(parts[4], V.length);

            if (sensor.isBlank()
                    || !Double.isFinite(temperature)
                    || receivedLamport < 0) {
                throw new IllegalArgumentException("Leitura inválida");
            }

            int targetPartition =
                    Math.floorMod(sensor.hashCode(), V.length);

            if (targetPartition != partition) {
                logEvent("PARTIÇÃO ERRADA;p" + partition
                        + ";" + sensor
                        + ";destino=p" + targetPartition);
                return;
            }

            synchronized (nodeMonitor) {
                long term = raft.termIfLeader();

                if (term == -1) {
                    logEvent("IGNORADO;p" + partition
                            + ";n" + self
                            + ";nao_sou_lider;" + message);
                    return;
                }

                long nextSeq = ultimoSeq + 1;

                // Atualiza o relógio de Lamport.
                long nextL = Math.max(L, receivedLamport) + 1;

                // Incorpora o vetor recebido e incrementa a componente local.
                long[] nextV = V.clone();

                for (int i = 0; i < nextV.length; i++) {
                    nextV[i] = Math.max(nextV[i], receivedVector[i]);
                }

                nextV[partition]++;

                if (nextV[partition] != nextSeq) {
                    throw new IllegalArgumentException(
                            "V[k] diferente de seq");
                }

                SensorRecord rec = new SensorRecord(
                        nextSeq,
                        term,
                        sensor,
                        temperature,
                        System.currentTimeMillis(),
                        nextL,
                        SensorRecord.formatVector(nextV)
                );

                // Primeiro escreve; depois atualiza o estado em memória.
                appendRecord(rec);
                L = nextL;
                V = nextV;

                // O registo será publicado no próximo lote.
                pendentes.add(rec);

                logEvent("RECEBIDO;p" + partition
                        + ";n" + self
                        + ";seq=" + nextSeq
                        + ";termo=" + term);
            }
        } catch (IllegalArgumentException e) {
            logEvent("IN_INVALIDO;p" + partition
                    + ";" + e.getMessage()
                    + ";" + message);
        }
    }

    private void handleVoteRequest(String message) {
        VoteRequest request = VoteRequest.parse(message);

        if (request.candidate == self) {
            return;
        }

        RaftState.Vote vote = raft.onVoteRequest(request.term, request.candidate,
                request.lastSeq, request.lastTerm, ultimoSeq, ultimoTermo);
        String reply = new VoteReply(vote.term, self, request.candidate, vote.granted).encode();
        if (Wire.isolated(partition, self)) {
            return;
        }
        publish(Wire.topicElection(partition), reply);
    }

    private void handleVoteReply(String message) {
        VoteReply reply = VoteReply.parse(message);
        if (reply.candidate != self) {
            return;
        }
        if (raft.onVote(reply.term, reply.voter, reply.candidate, reply.granted)) {
            becomeLeader();
        }
    }

    private void handleBatch(String message) {
        Batch batch = Batch.parse(message);
        if (batch.leader == self) {
            return;
        }
        if (batch.term < raft.term()) {
            logEvent("IGNORADO;p" + partition + ";n" + self + ";termo_antigo=" + batch.term);
            return;
        }
        if (!raft.onBatch(batch.term, batch.leader)) {
            logEvent("IGNORADO;p" + partition + ";n" + self + ";termo=" + batch.term + ";lider=n" + batch.leader);
            return;
        }
        logEvent("RECEBIDO;LOTE;p" + partition + ";termo=" + batch.term + ";lider=n" + batch.leader + ";n=" + batch.records.size());
        applyIncomingBatch(batch);
    }

    private void applyIncomingBatch(Batch batch) {
        synchronized (nodeMonitor) {
            if (batch.records.isEmpty()) {
                return;
            }
            for (SensorRecord rec : batch.records) {
                if (rec.getSeq() <= ultimoSeq) {
                    if (rec.getSeq() == ultimoSeq && rec.getTerm() != ultimoTermo) {
                        logEvent("DIVERGENCIA;p" + partition + ";seq=" + rec.getSeq() + ";local=" + ultimoTermo + ";lote=" + rec.getTerm());
                    }
                    continue;
                }
                appendRecord(rec);
            }
            if (ultimoSeq < batch.ultimoSeq || ultimoTermo != batch.ultimoTermo) {
                ultimoSeq = Math.max(ultimoSeq, batch.ultimoSeq);
                ultimoTermo = batch.ultimoTermo;
            }
        }
    }

    private void appendRecord(SensorRecord rec) {
        try {
            Files.writeString(dataFile, rec.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            ultimoSeq = Math.max(ultimoSeq, rec.getSeq());
            ultimoTermo = Math.max(ultimoTermo, rec.getTerm());
        } catch (IOException e) {
            throw new IllegalStateException("Falha ao escrever o registo no ficheiro do nó", e);
        }
    }

    private void becomeLeader() {
        synchronized (nodeMonitor) {
            seq = ultimoSeq;
            hb = 0L;
            try {
                loadExistingData();
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Falha ao restaurar os relógios", e);
            }
            pendentes.clear();
            if (scheduler == null || scheduler.isShutdown()) {
                scheduler = Executors.newSingleThreadScheduledExecutor();
                scheduler.scheduleAtFixedRate(this::sendBatch, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
            }
            logEvent("LIDER;p" + partition + ";termo=" + raft.term() + ";n=" + self);
        }
    }

    private void sendBatch() {
        synchronized (nodeMonitor) {
            long currentTerm = raft.termIfLeader();
            if (currentTerm == -1) {
                if (scheduler != null) {
                    scheduler.shutdownNow();
                    scheduler = null;
                }
                return;
            }
            List<SensorRecord> snapshot = new ArrayList<>(pendentes);
            pendentes.clear();
            hb++;
            Batch batch = new Batch(currentTerm, self, "127.0.0.1", Wire.nodePort(partition, self),
                    hb, ultimoSeq, ultimoTermo, snapshot);
            publish(Wire.topicData(partition), batch.toLine());
            appendHeartbeat(batch);
        }
    }

    private void appendHeartbeat(Batch batch) {
        try {
            Files.writeString(hbFile,
                    System.currentTimeMillis() + ";termo=" + batch.term + ";hb=" + batch.hb
                            + ";ultimoSeq=" + batch.ultimoSeq + ";n=" + batch.records.size() + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException("Falha ao escrever o heartbeat do nó", e);
        }
    }

    private void logEvent(String message) {
        String line = System.currentTimeMillis() + ";" + message;
        System.out.println(line);
        try {
            Files.writeString(eventFile, line + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException("Falha ao escrever o log de eventos", e);
        }
    }

    private void startVerificationThread() {
        Thread verifier = new Thread(() -> {
            while (running) {
                try {
                    long term = raft.tick();
                    if (term > 0) {
                        String voteRequest = new VoteRequest(term, self, ultimoSeq, ultimoTermo).encode();
                        publish(Wire.topicElection(partition), voteRequest);
                    }
                    Thread.sleep(Wire.VERIFICACAO_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "verif-p" + partition + "-n" + self);
        verifier.setDaemon(true);
        verifier.start();
    }

    private void startRecoveryServer() {
        Thread server = new Thread(() -> {
            try (ServerSocket socket = new ServerSocket(Wire.nodePort(partition, self))) {
                while (running) {
                    try (Socket client = socket.accept()) {
                        handlePeerConnection(client);
                    }
                }
            } catch (IOException e) {
                logEvent("RECUPERACAO_FALHOU;p" + partition + ";n" + self + ";" + e.getMessage());
            }
        }, "tcp-p" + partition + "-n" + self);
        server.setDaemon(true);
        server.start();
    }

    private void handlePeerConnection(Socket client) throws IOException {
        try (DataInputStream in = new DataInputStream(client.getInputStream());
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8))) {
            int length = in.readInt();
            if (length < 0) {
                out.write("ERRO;pedido_invalido");
                out.newLine();
                out.flush();
                return;
            }
            byte[] payload = new byte[length];
            int read = 0;
            while (read < length) {
                int n = in.read(payload, read, length - read);
                if (n < 0) {
                    throw new IOException("Conexão terminada antes do fim da mensagem");
                }
                read += n;
            }
            String message = new String(payload, StandardCharsets.UTF_8);
            if (message.startsWith("PEDIDO ")) {
                handleRecoveryRequest(message, out);
                return;
            }
            receive(message);
            out.write("OK");
            out.newLine();
            out.flush();
        }
    }

    private void handleRecoveryRequest(String request, BufferedWriter out) throws IOException {
        try {
            String[] parts = request.trim().split("\\s+");
            if (parts.length != 3 || !"PEDIDO".equals(parts[0])) {
                out.write("ERRO;pedido_invalido");
                out.newLine();
                out.flush();
                return;
            }
            long from = Long.parseLong(parts[1]);
            long to = Long.parseLong(parts[2]);
            List<SensorRecord> records = loadRange(from, to);
            for (SensorRecord rec : records) {
                out.write(rec.toLine());
                out.newLine();
            }
            out.write("FIM");
            out.newLine();
            out.flush();
        } catch (NumberFormatException e) {
            throw new IOException("Pedido de recuperação inválido", e);
        }
    }

    private List<SensorRecord> loadRange(long from, long to) throws IOException {
        if (!Files.exists(dataFile)) {
            return new ArrayList<>();
        }
        List<SensorRecord> result = new ArrayList<>();
        for (String line : Files.readAllLines(dataFile, StandardCharsets.UTF_8)) {
            if (line == null || line.isBlank()) {
                continue;
            }
            SensorRecord rec = SensorRecord.fromLine(line);
            if (rec.getSeq() >= from && rec.getSeq() <= to) {
                result.add(rec);
            }
        }
        return result;
    }

    public boolean isLeader() {
        return raft.role() == RaftState.Role.LIDER;
    }

    public long termIfLeader() {
        return raft.termIfLeader();
    }

    public void setIsolated(boolean isolated) {
        this.isolated = isolated;
    }

    public boolean isIsolated() {
        return isolated || Wire.isolated(partition, self);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Uso: PartitionNode <k> <i> [N]");
            return;
        }
        int k = Integer.parseInt(args[0]);
        int i = Integer.parseInt(args[1]);
        int n = args.length >= 3 ? Integer.parseInt(args[2]) : 3;
        Path dir = Files.createDirectories(Path.of("work"));
        Path stateFile = dir.resolve("estado-p" + k + "-n" + i + ".txt");
        PartitionNode node = new PartitionNode(k, i, n, dir);
        node.logEvent("START;p" + k + ";n" + i + ";N=" + n);
        while (true) {
            Thread.sleep(1000L);
        }
    }
}
