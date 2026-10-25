package rep02;

/** Vocabulário comum dos três programas: tópicos, mensagens e limites. */
public final class Wire {
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;

    private Wire() { }

    public static String topicIn(int k) { return "rep02-in-p" + k; }
    public static String topicData(int k) { return "rep02-p" + k; }

    public static String in(String sensor, double temp) {
        return "IN;" + sensor + ";" + temp;
    }

    // Cada publicação anuncia onde a réplica pode recuperar o histórico.
    public static String reg(String host, int port, SensorRecord r) {
        return "REG;" + host + ";" + port + ";" + r.toLine();
    }

    public static String type(String msg) {
        String m = msg.trim();
        int i = m.indexOf(';');
        if (i <= 0) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        }
        return m.substring(0, i);
    }

    public static String body(String msg) {
        String m = msg.trim();
        return m.substring(m.indexOf(';') + 1);
    }

    public static String[] regParts(String msg) {
        // O limite 4 preserva a linha completa do SensorRecord no último campo.
        String[] f = msg.trim().split(";", 4);
        if (f.length != 4 || !f[0].equals("REG")) {
            throw new IllegalArgumentException("REG mal formado: " + msg);
        }
        return new String[] { f[1], f[2], f[3] };
    }
}
