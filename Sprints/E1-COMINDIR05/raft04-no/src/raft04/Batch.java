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
        if (term < 0 || leader < 0 || address == null || address.isBlank()
                || port < 1 || port > 65535 || hb < 0 || ultimoSeq < 0 || ultimoTermo < 0
                || records == null) {
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
        for (SensorRecord rec : records) {
            sb.append('\n').append(rec.toLine());
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
        String[] header = lines[0].split(";", -1);
        if (header.length != 9 || !"LOTE".equals(header[0])) {
            throw new IllegalArgumentException("Cabeçalho LOTE mal formado");
        }

        long term = Long.parseLong(header[1]);
        int leader = Integer.parseInt(header[2]);
        String address = header[3];
        int port = Integer.parseInt(header[4]);
        long hb = Long.parseLong(header[5]);
        long lastSeq = Long.parseLong(header[6]);
        long lastTerm = Long.parseLong(header[7]);
        int count = Integer.parseInt(header[8]);

        List<SensorRecord> records = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            if (!lines[i].isBlank()) {
                records.add(SensorRecord.fromLine(lines[i]));
            }
        }

        if (count != records.size()) {
            throw new IllegalArgumentException("Número de registos diferente do indicado no cabeçalho");
        }

        return new Batch(term, leader, address, port, hb, lastSeq, lastTerm, records);
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
