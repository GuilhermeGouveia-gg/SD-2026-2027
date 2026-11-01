package rep04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Um cabeçalho de heartbeat e zero ou mais linhas imutáveis de registos. */
public final class Batch {
    public final String address;
    public final int port;
    public final long inc, hb, lastSeq;
    public final List<SensorRecord> records;
    public final int count;

    public Batch(String address, int port, long inc, long hb, long lastSeq,
                 List<SensorRecord> records) {
        this(address, port, inc, hb, lastSeq, records.size(), records);
    }

    private Batch(String address, int port, long inc, long hb, long lastSeq,
                  int count, List<SensorRecord> records) {
        if (address.isBlank() || address.contains(";") || address.contains("\n")
                || address.contains("\r") || port < 1 || port > 65535
                || inc < 0 || hb < 1 || lastSeq < 0 || count < 0) {
            throw new IllegalArgumentException("Cabeçalho LOTE inválido");
        }
        this.address = address; this.port = port; this.inc = inc;
        this.hb = hb; this.lastSeq = lastSeq; this.count = count;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public String encode() {
        StringBuilder out = new StringBuilder("LOTE;" + address + ";" + port
                + ";" + inc + ";" + hb + ";" + lastSeq + ";" + records.size());
        for (SensorRecord r : records) out.append('\n').append(r.toLine());
        return out.toString();
    }

    /** O produtor só lê esta linha: não interpreta os registos do corpo. */
    public static Batch header(String message) {
        String[] f = message.split("\\R", 2)[0].split(";", -1);
        if (f.length != 7 || !f[0].equals("LOTE")) {
            throw new IllegalArgumentException("Cabeçalho LOTE mal formado");
        }
        return new Batch(f[1], Integer.parseInt(f[2]), Long.parseLong(f[3]),
                Long.parseLong(f[4]), Long.parseLong(f[5]), Integer.parseInt(f[6]),
                Collections.emptyList());
    }

    /** n é a dimensão dos vetores; count é o número de linhas no cabeçalho. */
    public static Batch decode(String message, int n) {
        Batch h = header(message);
        String[] lines = message.split("\\R", -1);
        int length = lines.length;
        if (length > 1 && lines[length - 1].isEmpty()) length--;
        if (length - 1 != h.count) throw new IllegalArgumentException("Contagem LOTE incorreta");
        List<SensorRecord> records = new ArrayList<>();
        long previous = 0;
        for (int i = 1; i < length; i++) {
            SensorRecord r = n > 0 ? SensorRecord.fromLine(lines[i], n)
                    : SensorRecord.fromLine(lines[i]);
            if (r.getSeq() <= previous || (previous > 0 && r.getSeq() != previous + 1)
                    || r.getSensorId().isBlank() || !Double.isFinite(r.getTemperature())
                    || r.getTimestamp().isBlank() || r.getLamport() < 0) {
                throw new IllegalArgumentException("Registos LOTE inválidos ou desordenados");
            }
            previous = r.getSeq();
            records.add(r);
        }
        return new Batch(h.address, h.port, h.inc, h.hb, h.lastSeq, records);
    }

    public String detail() {
        return "inc=" + inc + ";hb=" + hb + ";ultimoSeq=" + lastSeq + ";n=" + count
                + ";seq=" + (count == 0 ? "-" : records.isEmpty()
                ? (lastSeq - count + 1) + ".." + lastSeq
                : records.get(0).getSeq() + ".." + records.get(records.size() - 1).getSeq());
    }
}
