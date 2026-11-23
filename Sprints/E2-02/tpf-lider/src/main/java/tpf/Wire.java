package tpf;

/** Parâmetros deste sprint; os valores são lidos uma vez, no arranque. */
public final class Wire {
    public static final int CHUNK_MAX_BYTES = positivo("tpf.chunkMax", 65536);
    public static final int PAD_BYTES = positivo("tpf.pad", 4096);
    public static final int SELAGEM_MS = positivo("tpf.selagem", 5000);

    private Wire() { }

    private static int positivo(String nome, int valorPadrao) {
        int valor = Integer.parseInt(System.getProperty(nome, String.valueOf(valorPadrao)));
        if (valor <= 0) throw new IllegalArgumentException(nome + " deve ser positivo");
        return valor;
    }
}
