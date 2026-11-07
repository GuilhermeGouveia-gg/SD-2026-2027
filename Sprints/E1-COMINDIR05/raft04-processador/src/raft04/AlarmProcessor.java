package raft04;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class AlarmProcessor implements AutoCloseable {
    private final int n;
    private final IpfsPubSub ipfs;
    private final HeartbeatMonitor[] monitors;

    private final Path monitorFile =
            Path.of("monitor-processador.txt");

    // Protege os relógios, os termos observados e os duplicados.
    private final Object stateLock = new Object();

    // Protege as escritas no ficheiro de eventos.
    private final Object logLock = new Object();

    private long Lp = 0;
    private final long[] Vp;
    private final long[] highestTerms;

    // Um conjunto por partição. Chave: seq;termo do registo.
    private final List<Set<String>> processed = new ArrayList<>();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final CountDownLatch stopped = new CountDownLatch(1);

    private final ScheduledExecutorService verifier =
            Executors.newSingleThreadScheduledExecutor();

    public AlarmProcessor(int n) throws IOException {
        this(
                n,
                System.getProperty(
                        "raft04.api",
                        "http://127.0.0.1:5001/api/v0"
                )
        );
    }

    public AlarmProcessor(int n, String api) throws IOException {
        if (n < 1) {
            throw new IllegalArgumentException(
                    "Número de partições inválido"
            );
        }

        this.n = n;
        this.ipfs = new IpfsPubSub(api);
        this.monitors = new HeartbeatMonitor[n];
        this.Vp = new long[n];
        this.highestTerms = new long[n];

        Files.createDirectories(
                monitorFile.toAbsolutePath().getParent()
        );

        for (int k = 0; k < n; k++) {
            processed.add(new HashSet<>());

            monitors[k] = new HeartbeatMonitor(
                    k,
                    Wire.PERIODO_HB_MS,
                    Wire.FATOR_TIMEOUT,
                    this::log
            );
        }
    }

    private void log(String event) {
        String line = System.currentTimeMillis() + ";" + event;

        synchronized (logLock) {
            System.out.println(line);

            try {
                Files.writeString(
                        monitorFile,
                        line + System.lineSeparator(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                );
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Falha ao escrever monitor-processador.txt",
                        e
                );
            }
        }
    }

    public void run() throws InterruptedException {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    "O processador já foi iniciado"
            );
        }

        for (int k = 0; k < n; k++) {
            startSubscription(k);
        }

        verifier.scheduleAtFixedRate(
                this::checkMonitors,
                Wire.VERIFICACAO_MS,
                Wire.VERIFICACAO_MS,
                TimeUnit.MILLISECONDS
        );

        System.out.println(
                "Processador ativo. À espera de lotes de "
                        + n + " partições."
        );

        // Mantém o programa ativo sem ler comandos da consola.
        stopped.await();
    }

    private void startSubscription(int partition) {
        Thread thread = new Thread(() -> {
            try {
                ipfs.subscribe(
                        Wire.topicData(partition),
                        message -> receiveBatch(partition, message)
                );

                if (running.get()) {
                    log("ERRO;p" + partition
                            + ";subscricao terminou");
                    close();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                if (running.get()) {
                    close();
                }
            } catch (IOException | RuntimeException e) {
                if (running.get()) {
                    System.err.println(
                            "Falha na subscrição de p"
                                    + partition + ": "
                                    + e.getMessage()
                    );
                    close();
                }
            }
        }, "processador-sub-p" + partition);

        thread.setDaemon(true);
        thread.start();
    }

    private void checkMonitors() {
        if (!running.get()) {
            return;
        }

        try {
            for (HeartbeatMonitor monitor : monitors) {
                monitor.check();
            }
        } catch (RuntimeException e) {
            System.err.println(
                    "Falha no monitor: " + e.getMessage()
            );
            close();
        }
    }

    private void receiveBatch(int partition, String message) {
        if (!running.get()) {
            return;
        }

        try {
            Batch batch = Batch.decode(message, n);
            processBatch(partition, batch);
        } catch (IllegalArgumentException e) {
            log("REJEITADO;p" + partition
                    + ";" + e.getMessage());
        }
    }

    private void processBatch(int partition, Batch batch) {
        List<PendingAlarm> alarms = new ArrayList<>();

        synchronized (stateLock) {
            // Não processar lotes de uma liderança obsoleta.
            if (batch.term < highestTerms[partition]) {
                log("IGNORADO;p" + partition
                        + ";termo_antigo=" + batch.term);
                return;
            }

            highestTerms[partition] = batch.term;

            monitors[partition].onHeartbeat(
                    batch.term,
                    batch.leader,
                    batch.hb
            );

            String range = batch.records.isEmpty()
                    ? "vazio"
                    : batch.records.get(0).getSeq()
                    + ".."
                    + batch.records.get(
                    batch.records.size() - 1
            ).getSeq();

            log("RECEBIDO;p" + partition
                    + ";termo=" + batch.term
                    + ";lider=n" + batch.leader
                    + ";hb=" + batch.hb
                    + ";ultimoSeq=" + batch.ultimoSeq
                    + ";n=" + batch.records.size()
                    + ";intervalo=" + range);

            for (SensorRecord record : batch.records) {
                String sensor = record.getSensor();

                // Evita alarmes sobre alarmes.
                if (sensor.startsWith("ALARM-")) {
                    continue;
                }

                String key = record.getSeq()
                        + ";" + record.getTerm();

                if (processed.get(partition).contains(key)) {
                    continue;
                }

                long[] receivedVector =
                        SensorRecord.parseVector(
                                record.getVector(),
                                n
                        );

                processed.get(partition).add(key);

                // O processador acumula conhecimento, sem tick.
                Lp = Math.max(Lp, record.getLamport());

                for (int i = 0; i < n; i++) {
                    Vp[i] = Math.max(
                            Vp[i],
                            receivedVector[i]
                    );
                }

                if (record.getTemperature()
                        >= Wire.LIMIAR_ALARME) {
                    String alarmSensor = "ALARM-" + sensor;

                    int destination = Math.floorMod(
                            alarmSensor.hashCode(),
                            n
                    );

                    // A String preserva uma cópia do vetor
                    // observado neste instante.
                    String vectorSnapshot =
                            SensorRecord.formatVector(Vp.clone());

                    String input = Wire.in(
                            alarmSensor,
                            record.getTemperature(),
                            Lp,
                            vectorSnapshot
                    );

                    alarms.add(new PendingAlarm(
                            partition,
                            record.getSeq(),
                            record.getTerm(),
                            sensor,
                            alarmSensor,
                            destination,
                            input
                    ));
                }
            }
        }

        // HTTP fora da secção sincronizada.
        for (PendingAlarm alarm : alarms) {
            publishAlarm(alarm);
        }
    }

    private void publishAlarm(PendingAlarm alarm) {
        if (!running.get()) {
            return;
        }

        if (monitors[alarm.destination].state()
                == HeartbeatMonitor.State.SUSPEITO) {
            log("AVISO;p" + alarm.destination
                    + ";sem_lider_conhecido;alarme="
                    + alarm.alarmSensor
                    + ";pode_perder_se");
        }

        try {
            // Publica mesmo quando o líder está suspeito.
            ipfs.publish(
                    Wire.topicIn(alarm.destination),
                    alarm.message
            );

            log("ALARME;p" + alarm.sourcePartition
                    + ";sensor=" + alarm.sourceSensor
                    + ";seq=" + alarm.sourceSeq
                    + ";termo_registo=" + alarm.sourceTerm
                    + ";destino=p" + alarm.destination
                    + ";alarme=" + alarm.alarmSensor);

        } catch (IOException e) {
            // Não há reenvio automático nesta ficha.
            log("ERRO;p" + alarm.destination
                    + ";publicacao_alarme="
                    + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class PendingAlarm {
        final int sourcePartition;
        final long sourceSeq;
        final long sourceTerm;
        final String sourceSensor;
        final String alarmSensor;
        final int destination;
        final String message;

        PendingAlarm(
                int sourcePartition,
                long sourceSeq,
                long sourceTerm,
                String sourceSensor,
                String alarmSensor,
                int destination,
                String message
        ) {
            this.sourcePartition = sourcePartition;
            this.sourceSeq = sourceSeq;
            this.sourceTerm = sourceTerm;
            this.sourceSensor = sourceSensor;
            this.alarmSensor = alarmSensor;
            this.destination = destination;
            this.message = message;
        }
    }

    @Override
    public void close() {
        if (running.getAndSet(false)) {
            verifier.shutdownNow();
            stopped.countDown();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            System.out.println(
                    "Uso: AlarmProcessor <N> [api]"
            );
            return;
        }

        int n = Integer.parseInt(args[0]);

        String api = args.length == 2
                ? args[1]
                : System.getProperty(
                "raft04.api",
                "http://127.0.0.1:5001/api/v0"
        );

        try (AlarmProcessor processor =
                     new AlarmProcessor(n, api)) {

            Runtime.getRuntime().addShutdownHook(
                    new Thread(processor::close)
            );

            processor.run();
        }
    }
}