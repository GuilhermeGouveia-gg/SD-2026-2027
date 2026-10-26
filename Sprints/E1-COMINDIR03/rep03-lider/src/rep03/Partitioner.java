package rep03;

/** A mesma chave e o mesmo N produzem a mesma partição em qualquer JVM. */
public final class Partitioner {
    private final int n;

    public Partitioner(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("Número de partições inválido: " + n);
        }
        this.n = n;
    }

    public int of(String sensorId) {
        // floorMod mantém o resultado em [0, N[, mesmo com hashCode negativo.
        return Math.floorMod(sensorId.hashCode(), n);
    }

    public int count() { return n; }
}
