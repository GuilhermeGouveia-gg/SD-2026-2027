package rep03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class AlarmProcessor {
    private final int n;
    private final Partitioner partitioner;
    private final IpfsPubSub ps;
    private final Object lock = new Object();
    private long lp = 0;
    private final VectorClock vp;
    private final long[] lastSeq;

    public AlarmProcessor(int n, String api) {
        if (n < 1) {
            throw new IllegalArgumentException("N tem de ser positivo.");
        }
        this.n = n;
        this.partitioner = new Partitioner(n);
        this.ps = new IpfsPubSub(api);
        this.vp = new VectorClock(n);
        this.lastSeq = new long[n];
    }

    public static void main(String[] args) {
        if (args.length < 1 || args.length > 2) {
            System.err.println("Uso: AlarmProcessor <N> [api]");
            return;
        }
        try {
            int n = Integer.parseInt(args[0]);
            String api = args.length == 2 ? args[1] : "http://127.0.0.1:5001/api/v0";
            new AlarmProcessor(n, api).run();
        } catch (IllegalArgumentException e) {
            System.err.println("Configuração inválida: " + e.getMessage());
        }
    }

    public void run() {
        System.out.println("PROCESSADOR DE ALARMES | N=" + n);
        List<Thread> threads = new ArrayList<>();
        for (int k = 0; k < n; k++) {
            final int partition = k;
            Thread t = new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicData(partition), msg -> handle(partition, msg));
                } catch (IOException e) {
                    System.err.println("SUBSCRIÇÃO FALHOU p" + partition + ": " + e.getMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "proc-p" + partition);
            t.start();
            threads.add(t);
        }
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void handle(int k, String message) {
        try {
            if (!Wire.type(message).equals("REG")) return;
            String[] parts = Wire.regParts(message);
            SensorRecord record = SensorRecord.fromLine(parts[2], n);

            // Ignorar alarmes sobre alarmes
            if (record.getSensorId().startsWith("ALARM-")) {
                return;
            }

            long lToSend = 0;
            VectorClock vToSend = null;
            boolean shouldAlarm = false;
            int targetPartition = -1;
            String alarmSensor = null;

            synchronized (lock) {
                // Ignorar duplicados
                if (record.getSeq() <= lastSeq[k]) {
                    return;
                }
                lastSeq[k] = record.getSeq();

                // Atualizar conhecimento causal com todos os registos observados
                lp = Math.max(lp, record.getLamport());
                vp.merge(record.getVector());

                if (record.getTemperature() >= Wire.LIMIAR_ALARME) {
                    shouldAlarm = true;
                    lToSend = lp;
                    vToSend = vp.copy();
                    alarmSensor = "ALARM-" + record.getSensorId();
                    targetPartition = partitioner.of(alarmSensor);
                }
            }

            if (shouldAlarm) {
                ps.publish(Wire.topicIn(targetPartition),
                        Wire.in(alarmSensor, record.getTemperature(), lToSend, vToSend));
                System.out.println("ALARME: " + record.getSensorId() + " p" + k + "#"
                        + record.getSeq() + " → p" + targetPartition);
            }
        } catch (IllegalArgumentException e) {
            System.err.println("MENSAGEM INVÁLIDA p" + k + ": " + e.getMessage());
        } catch (IOException e) {
            System.err.println("FALHA AO PUBLICAR ALARME: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
