package tpf;

/** Parâmetros deste sprint; os valores são lidos uma vez, no arranque. */
public final class Wire {
    public static final int CHUNK_MAX_BYTES = positivo("tpf.chunkMax", 65536);
    public static final int PAD_BYTES = positivo("tpf.pad", 4096);
    public static final int SELAGEM_MS = positivo("tpf.selagem", 5000);
    public static final int PORTO = positivo("tpf.porto", 8765);
    public static final int PERIODO_HB_MS = positivo("tpf.periodoHb", 2000);
    public static final int SUSPEITA_MS = positivo("tpf.suspeita", 6000);
    public static final int MORTA_MS = positivo("tpf.morta", 10000);
    public static final long TOKEN_VALIDADE_MS = positivoLong("tpf.tokenValidade", 600000);
    public static final int R = positivo("tpf.r", 3);
    public static final int W_MIN = positivo("tpf.wMin", 2);
    public static final int ACK_TIMEOUT_MS = positivo("tpf.ackTimeout", 4000);
    public static final int MAX_RONDAS = positivo("tpf.maxRondas", 3);
    public static final int MARGEM_BYTES = positivo("tpf.margem", 65536);
    public static final double ALFA = positivoDouble("tpf.alfa", 1.0);
    public static final int BACKOFF_MIN_MS = positivo("tpf.backoffMin", 1000);
    public static final int BACKOFF_MAX_MS = positivo("tpf.backoffMax", 30000);

    private Wire() { }

    private static int positivo(String nome, int valorPadrao) {
        int valor = Integer.parseInt(System.getProperty(nome, String.valueOf(valorPadrao)));
        if (valor <= 0) throw new IllegalArgumentException(nome + " deve ser positivo");
        return valor;
    }

    private static long positivoLong(String nome, long valorPadrao) {
        long valor = Long.parseLong(System.getProperty(nome, String.valueOf(valorPadrao)));
        if (valor <= 0) throw new IllegalArgumentException(nome + " deve ser positivo");
        return valor;
    }

    private static double positivoDouble(String nome, double valorPadrao) {
        double valor = Double.parseDouble(System.getProperty(nome, String.valueOf(valorPadrao)));
        if (!Double.isFinite(valor) || valor < 0) throw new IllegalArgumentException(nome + " deve ser nao negativo");
        return valor;
    }
}
