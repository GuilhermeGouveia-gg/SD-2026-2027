package rep04;

/** Vocabulário comum dos programas: tópicos, mensagens e limites. */
public final class Wire {
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;
    public static final double LIMIAR_ALARME = 28.0;

    public static final long PERIODO_HB_MS = Long.parseLong(System.getProperty("rep04.periodo", "5000"));
    public static final int FATOR_TIMEOUT = Integer.parseInt(System.getProperty("rep04.fator", "3"));
    public static final long VERIFICACAO_MS = 1000;
    static {
        if (PERIODO_HB_MS < 1 || FATOR_TIMEOUT < 1) throw new IllegalArgumentException("Período e fator têm de ser positivos");
        Math.multiplyExact(Math.multiplyExact(PERIODO_HB_MS, FATOR_TIMEOUT), 1_000_000L);
    }
    private Wire() { }

    public static String topicIn(int k) { return "rep04-in-p" + k; }
    public static String topicData(int k) { return "rep04-p" + k; }

    public static String in(String sensor, double temp, long l, VectorClock v) {
        return "IN;" + sensor + ";" + temp + ";" + l + ";" + v;
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

    public static String[] inParts(String msg) {
        String[] f = msg.trim().split(";");
        if (f.length != 5 || !f[0].equals("IN")) {
            throw new IllegalArgumentException("IN mal formado: " + msg);
        }
        // sensor, temp, L, V
        return new String[] { f[1], f[2], f[3], f[4] };
    }

}
