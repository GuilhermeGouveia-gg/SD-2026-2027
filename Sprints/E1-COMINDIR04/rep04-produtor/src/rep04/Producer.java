package rep04;

import java.io.IOException;
import java.util.Locale;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

/** Publica leituras no tópico de entrada da partição do sensor. */
public final class Producer {
    private final Partitioner partitioner;
    private final IpfsPubSub ps;
    private final HeartbeatMonitor[] monitors;
    private final MonitorLog log = new MonitorLog("produtor");

    private Producer(int n, String api) {
        partitioner = new Partitioner(n);
        ps = new IpfsPubSub(api);
        monitors = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) monitors[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, log::event);
    }

    public static void main(String[] args) {
        if (args.length < 1 || args.length > 2) {
            System.err.println("Uso: Producer <N> [api]");
            return;
        }
        try {
            int n = Integer.parseInt(args[0]);
            String api = args.length == 2 ? args[1] : "http://127.0.0.1:5001/api/v0";
            new Producer(n, api).run();
        } catch (IllegalArgumentException e) {
            System.err.println("Configuração inválida: " + e.getMessage());
        }
    }

    private void run() {
        log.start(monitors);
        for (int k = 0; k < monitors.length; k++) {
            final int partition = k;
            Thread t = new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicData(partition), message -> {
                        try {
                            Batch h = Batch.header(message);
                            monitors[partition].onHeartbeat(h.inc, h.hb);
                            log.event("RECEBIDO;p" + partition + ";" + h.detail());
                        } catch (IllegalArgumentException e) {
                            System.err.println("LOTE INVÁLIDO: " + e.getMessage());
                        }
                    });
                } catch (IOException e) {
                    System.err.println("SUBSCRIÇÃO FALHOU p" + partition + ": " + e.getMessage());
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }, "produtor-p" + k);
            t.setDaemon(true);
            t.start();
        }
        System.out.println("Comandos: <sensor> <temperatura> | rajada <n> <intervaloMs>"
                + " | rajada-quente <n> <intervaloMs> | sair");
        try (Scanner sc = new Scanner(System.in)) {
            while (sc.hasNextLine()) {
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;
                if (line.isEmpty()) continue;
                String[] p = line.split("\\s+");
                try {
                    boolean quente = p[0].equalsIgnoreCase("rajada-quente");
                    if (quente || p[0].equalsIgnoreCase("rajada")) {
                        if (p.length != 3) {
                            throw new IllegalArgumentException("Usa: " + p[0] + " <n> <intervaloMs>");
                        }
                        burst(Integer.parseInt(p[1]), Integer.parseInt(p[2]), quente);
                    } else {
                        if (p.length != 2 || p[0].contains(";")) {
                            throw new IllegalArgumentException("Usa: <sensor> <temperatura>");
                        }
                        double temp = Double.parseDouble(p[1]);
                        if (!Double.isFinite(temp)) {
                            throw new IllegalArgumentException("Temperatura tem de ser finita.");
                        }
                        publish(p[0], temp);
                    }
                } catch (IllegalArgumentException e) {
                    System.out.println("COMANDO INVÁLIDO: " + e.getMessage());
                } catch (IOException e) {
                    // Publicar não é uma confirmação de que o líder recebeu a leitura.
                    System.out.println("PUBLICAÇÃO FALHOU: " + e.getMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        } finally { log.close(); }
    }

    private int publish(String sensor, double temp) throws IOException, InterruptedException {
        int k = partitioner.of(sensor);
        if (monitors[k].state() == HeartbeatMonitor.State.SUSPEITO) {
            System.out.println("AVISO: líder p" + k + " suspeito; leitura " + sensor + " pode perder-se");
        }
        ps.publish(Wire.topicIn(k), Wire.in(sensor, temp, 0, new VectorClock(partitioner.count())));
        System.out.println("ENVIADO: " + sensor + " " + temp + " → p" + k);
        return k;
    }

    private void burst(int n, int intervalMs, boolean quente) throws IOException, InterruptedException {
        if (n < 1 || intervalMs < 0) {
            throw new IllegalArgumentException("Quantidade positiva e intervalo não negativo.");
        }
        int sensors = 10;
        long[] counts = new long[partitioner.count()];
        int failures = 0;
        long first = 0;
        long last = 0;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < n; i++) {
            int sensor;
            if (quente) {
                // Os 20% restantes excluem S01, conforme a especificação.
                sensor = random.nextDouble() < 0.8 ? 1 : (int) random.nextLong(2, (long) sensors + 1);
            } else {
                sensor = (int) random.nextLong(1, (long) sensors + 1);
            }
            if (i > 0) Thread.sleep(intervalMs);
            String name = String.format(Locale.ROOT, "S%02d", sensor);
            long now = System.currentTimeMillis();
            if (i == 0) first = now;
            last = now;
            try {
                counts[publish(name, random.nextDouble(15.0, 30.0))]++;
            } catch (IOException e) {
                // Uma falha não cancela as restantes leituras nem é retransmitida.
                failures++;
                System.out.println("PUBLICAÇÃO FALHOU: " + name + " | " + e.getMessage());
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("RAJADA: " + n + " leituras | Primeiro envio: " + first
                + " | Último envio: " + last + " | Fim: " + end
                + " | Tempo de envio (ms): " + (end - first) + " | Falhas: " + failures);
        for (int k = 0; k < counts.length; k++) {
            System.out.println("ENVIADOS p" + k + ": " + counts[k]);
        }
    }
}
