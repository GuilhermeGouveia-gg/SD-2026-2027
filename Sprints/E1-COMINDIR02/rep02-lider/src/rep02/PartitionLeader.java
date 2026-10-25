package rep02;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Um processo por partição: entrada por PubSub e recuperação por TCP. */
public final class PartitionLeader {
    private final int k;
    private final Partitioner partitioner;
    private final String host;
    private final int port;
    private final IpfsPubSub ps;
    private final RecordFile file;
    // Protegido pelo monitor de file, também usado por append, lastSeq e range.
    private long lastSeq;

    private PartitionLeader(int k, int n, String host, String api) throws IOException {
        partitioner = new Partitioner(n);
        if (k < 0 || k >= n || k > 65535 - Wire.PORTO_BASE) {
            throw new IllegalArgumentException("Partição/porto inválido: " + k);
        }
        if (host.isBlank() || host.contains(";") || host.contains("\n") || host.contains("\r")) {
            throw new IllegalArgumentException("Endereço anunciado inválido.");
        }
        this.k = k;
        this.host = host;
        port = Wire.PORTO_BASE + k;
        ps = new IpfsPubSub(api);
        file = new RecordFile("lider-p" + k + ".txt");
        lastSeq = file.lastSeq();
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 4) {
            System.err.println("Uso: PartitionLeader <k> <N> [endereço] [api]");
            return;
        }
        try {
            int k = Integer.parseInt(args[0]);
            int n = Integer.parseInt(args[1]);
            String host = args.length > 2 ? args[2] : "127.0.0.1";
            String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";
            new PartitionLeader(k, n, host, api).run();
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("LÍDER FALHOU: " + e.getMessage());
        }
    }

    private void run() throws IOException {
        // Abrir o porto primeiro evita deixar a entrada ativa se o bind falhar.
        try (ServerSocket server = new ServerSocket(port)) {
            Thread entrada = new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicIn(k), this::receive);
                    System.err.println("ENTRADA TERMINOU p" + k);
                } catch (IOException e) {
                    System.err.println("ENTRADA FALHOU p" + k + ": " + e.getMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "entrada-p" + k);
            System.out.println("LÍDER p" + k + " | TCP " + port + " | Anúncio: "
                    + host + ":" + port + " | Último seq: " + lastSeq);
            entrada.start();
            while (true) {
                // A thread principal serve uma ligação de cada vez.
                try (Socket socket = server.accept()) {
                    serve(socket);
                } catch (IOException e) {
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        }
    }

    private void receive(String message) {
        try {
            if (!Wire.type(message).equals("IN")) throw new IllegalArgumentException("Tipo inválido");
            String[] fields = Wire.body(message).split(";", -1);
            if (fields.length != 2 || fields[0].isBlank()) {
                throw new IllegalArgumentException("Campos inválidos");
            }
            String sensor = fields[0];
            double temp = Double.parseDouble(fields[1]);
            if (!Double.isFinite(temp)) throw new IllegalArgumentException("Temperatura inválida");
            int expected = partitioner.of(sensor);
            if (expected != k) {
                System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + expected);
                return;
            }
            SensorRecord record;
            synchronized (file) {
                // Só avançar lastSeq depois de a escrita ter tido sucesso.
                record = SensorRecord.now(Math.addExact(lastSeq, 1), sensor, temp);
                file.append(record);
                lastSeq = record.getSeq();
            }
            long written = System.currentTimeMillis();
            System.out.println("ESCRITO p" + k + ": " + record.getSeq() + " | Instante: " + written);
            // O histórico já existe quando uma réplica vê o anúncio REG.
            ps.publish(Wire.topicData(k), Wire.reg(host, port, record));
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + message);
        } catch (IOException | ArithmeticException e) {
            System.out.println("ENTRADA FALHOU p" + k + ": " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void serve(Socket socket) throws IOException {
        socket.setSoTimeout(Wire.T_TCP_MS);
        BufferedReader in = new BufferedReader(new InputStreamReader(
                socket.getInputStream(), StandardCharsets.UTF_8));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
                socket.getOutputStream(), StandardCharsets.UTF_8));
        String request = in.readLine();
        long from;
        long to;
        try {
            String[] f = request == null ? new String[0] : request.trim().split("\\s+");
            if (f.length != 3 || !f[0].equals("PEDIDO")) {
                throw new IllegalArgumentException("Usa PEDIDO <de> <até>");
            }
            from = Long.parseLong(f[1]);
            to = Long.parseLong(f[2]);
            if (from < 1 || from > to) throw new IllegalArgumentException("Intervalo inválido");
        } catch (IllegalArgumentException e) {
            sendLine(out, "ERRO " + e.getMessage());
            out.flush();
            return;
        }
        List<SensorRecord> records;
        synchronized (file) {
            to = Math.min(to, lastSeq);
            // Snapshot coerente; o envio TCP ocorre fora do bloqueio da escrita.
            records = file.range(from, to);
        }
        for (SensorRecord r : records) sendLine(out, r.toLine());
        sendLine(out, "FIM");
        out.flush();
        System.out.println("RECUPERAÇÃO: " + from + ".." + to + " para "
                + socket.getRemoteSocketAddress() + " (" + records.size() + " registos)");
    }

    private static void sendLine(BufferedWriter out, String line) throws IOException {
        out.write(line);
        out.newLine();
    }
}
