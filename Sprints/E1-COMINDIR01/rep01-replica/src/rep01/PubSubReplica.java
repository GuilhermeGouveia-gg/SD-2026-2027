package rep01;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class PubSubReplica {
    static final String TOPIC = "rep01-temperaturas";

    static int recebidos = 0;
    static int escritos = 0;
    static int duplicados = 0;
    static long primeiro = 0;
    static long ultimo = 0;

    public static void main(String[] args)
            throws IOException, InterruptedException {

        String id = args.length > 0 ? args[0] : "1";
        String api = args.length > 1
                ? args[1]
                : "http://127.0.0.1:5001/api/v0";

        RecordFile file = new RecordFile("replica-" + id + ".txt");
        AtomicLong lastSeq = new AtomicLong(file.lastSeq());
        Map<Long, SensorRecord> buffered = new TreeMap<>();

        IpfsPubSub ipfs = new IpfsPubSub(api);
        ipfs.subscribe(TOPIC,
                line -> processMessage(line, file, lastSeq, buffered));
    }

    private static void processMessage(
            String line,
            RecordFile file,
            AtomicLong lastSeq,
            Map<Long, SensorRecord> buffered
    ) {
        long instanteRececao = System.currentTimeMillis();

        SensorRecord r;

        try {
            r = SensorRecord.fromLine(line);
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + line);
            return;
        }

        recebidos++;

        if (recebidos == 1) {
            primeiro = instanteRececao;
        }
        ultimo = instanteRececao;

        long seq = r.getSeq();

        try {
            if (seq <= lastSeq.get() || buffered.containsKey(seq)) {
                duplicados++;
                System.out.println("DUPLICADO: " + seq);

            } else if (seq == lastSeq.get() + 1) {
                file.append(r);
                escritos++;
                lastSeq.set(seq);
                System.out.println("Aplicado: " + r.toLine());

                while (buffered.containsKey(lastSeq.get() + 1)) {
                    SensorRecord next =
                            buffered.get(lastSeq.get() + 1);

                    file.append(next);
                    buffered.remove(next.getSeq());
                    escritos++;
                    lastSeq.set(next.getSeq());

                    System.out.println(
                            "Aplicado em espera: " + next.toLine()
                    );
                }

            } else {
                buffered.put(seq, r);
                System.out.println("EM ESPERA: " + seq
                        + " (falta " + (lastSeq.get() + 1) + ")");
            }

        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }

        System.out.println(
                "Recebidos: " + recebidos
                        + " | Duplicados: " + duplicados
                        + " | Escritos: " + escritos
                        + " | Em espera: " + buffered.size()
                        + " | Primeira receção: " + primeiro
                        + " | Última receção: " + ultimo
        );
    }
}
