package rep01;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class PubSubReplica {
    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) throws IOException, InterruptedException {
        String id = args.length > 0 ? args[0] : "1";
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        RecordFile file = new RecordFile("replica-" + id + ".txt");
        AtomicLong lastSeq = new AtomicLong(file.lastSeq());
        Map<Long, SensorRecord> buffered = new TreeMap<>();

        IpfsPubSub ipfs = new IpfsPubSub(api);
        ipfs.subscribe(TOPIC, json -> processMessage(json, file, lastSeq, buffered));
    }

    private static void processMessage(
            String json,
            RecordFile file,
            AtomicLong lastSeq,
            Map<Long, SensorRecord> buffered
    ) {
        try {
            SensorRecord r = SensorRecord.fromLine(json);
            long seq = r.getSeq();
            long current = lastSeq.get();

            if (seq <= current || buffered.containsKey(seq)) {
                System.out.println("DUPLICADO: " + seq);
                return;
            }

            if (seq == current + 1) {
                file.append(r);
                lastSeq.set(seq);
                System.out.println("Aplicado: " + r.toLine());

                while (buffered.containsKey(lastSeq.get() + 1)) {
                    SensorRecord next = buffered.remove(lastSeq.get() + 1);
                    file.append(next);
                    lastSeq.set(next.getSeq());
                    System.out.println("Aplicado em espera: " + next.toLine());
                }
            } else {
                buffered.put(seq, r);
                System.out.println("EM ESPERA: " + seq + " (falta " + (current + 1) + ")");
            }
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + json);
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}
