package raft04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Batch {
    public final long term;
    public final int leader;
    public final String address;
    public final int port;
    public final long hb;
    public final long ultimoSeq;
    public final long ultimoTermo;
    public final List<SensorRecord> records;

    public Batch(long term, int leader, String address, int port,
                long hb, long ultimoSeq, long ultimoTermo,
                List<SensorRecord> records) {
        if (term < 1 || leader < 1 || address == null || address.isBlank()
                || port < 1 || port > 65535 || hb < 0 || ultimoSeq < 0 || ultimoTermo < 0
                || ultimoTermo > term || records == null) {
            throw new IllegalArgumentException("Cabeçalho LOTE inválido");
        }
        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.ultimoSeq = ultimoSeq;
        this.ultimoTermo = ultimoTermo;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public String toLine() {
        StringBuilder sb = new StringBuilder(
                Wire.encodeBatch(term, leader, address, port, hb, ultimoSeq, ultimoTermo, records.size()));
        for (SensorRecord r : records) {
            sb.append('\n').append(r.toLine());
        }
        return sb.toString();
    }

    public static Batch parse(String message) {
        if (message == null || message.isBlank()) {
            throw new IllegalArgumentException("LOTE vazio");
        }
        String[] lines = message.split("\\R", -1);
        if (lines.length == 0 || lines[0].isBlank()) {
            throw new IllegalArgumentException("Cabeçalho LOTE mal formado");
        }
        String[] h = lines[0].split(";", -1);
        if (h.length != 9 || !"LOTE".equals(h[0])) {
            throw new IllegalArgumentException("Cabeçalho LOTE mal formado");
        }
        int count = Integer.parseInt(h[8]);
        if (count < 0 || lines.length - 1 != count) {
            throw new IllegalArgumentException("Número de registos diferente do indicado no cabeçalho");
        }

        List<SensorRecord> records = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            if (!lines[i].isBlank()) {
                records.add(SensorRecord.fromLine(lines[i]));
            }
        }

        if (records.size() != count) {
            throw new IllegalArgumentException("Número de registos diferente do indicado no cabeçalho");
        }

        return new Batch(Long.parseLong(h[1]), Integer.parseInt(h[2]), h[3],
                Integer.parseInt(h[4]), Long.parseLong(h[5]), Long.parseLong(h[6]),
                Long.parseLong(h[7]), records);
    }

    public static Batch decode(String message) {
        return parse(message);
    }

    public static Batch decode(String message, int n) {
        Batch batch = decode(message);

        for (SensorRecord record : batch.records) {
            SensorRecord.parseVector(record.getVector(), n);
        }

        return batch;
    }
}
