package raft04;

import java.time.Instant;

public class SensorRecord {
    private final long seq;
    private final long term;
    private final String sensor;
    private final double temperature;
    private final long timestamp;
    private final long lamport;
    private final String vector;

    public SensorRecord(long seq, long term, String sensor, double temperature, long timestamp, long lamport, String vector) {
        this.seq = seq;
        this.term = term;
        this.sensor = sensor;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector;
    }

    public static SensorRecord now(long seq, long term, String sensor, double temperature, long lamport, String vector) {
        return new SensorRecord(seq, term, sensor, temperature, Instant.now().toEpochMilli(), lamport, vector);
    }

    public String toLine() {
        return seq + ";" + term + ";" + sensor + ";" + temperature + ";" + timestamp + ";" + lamport + ";" + vector;
    }

    public static SensorRecord fromLine(String line) {
        String[] f = line.trim().split(";", -1);
        if (f.length != 7) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return new SensorRecord(Long.parseLong(f[0]), Long.parseLong(f[1]), f[2], Double.parseDouble(f[3]), Long.parseLong(f[4]), Long.parseLong(f[5]), f[6]);
    }

    public long getSeq() { return seq; }
    public long getTerm() { return term; }
    public String getSensor() { return sensor; }
    public double getTemperature() { return temperature; }
    public long getTimestamp() { return timestamp; }
    public long getLamport() { return lamport; }
    public String getVector() { return vector; }

    public static long[] parseVector(String text, int n) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("Vetor vazio");
        }

        String[] fields = text.split(",", -1);

        if (n >= 0 && fields.length != n) {
            throw new IllegalArgumentException(
                    "Dimensão do vetor inválida: esperado "
                            + n + ", recebido " + fields.length
            );
        }

        long[] values = new long[fields.length];

        for (int i = 0; i < fields.length; i++) {
            values[i] = Long.parseLong(fields[i].trim());

            if (values[i] < 0) {
                throw new IllegalArgumentException(
                        "Componente negativa no vetor"
                );
            }
        }

        return values;
    }
}
