package tpf;

import java.util.LinkedHashSet;
import java.util.Set;

public final class EntradaCatalogo {
    public final long lsnDe;
    public final long lsnAte;
    public final long tamanho;
    public final Set<String> replicas = new LinkedHashSet<>();
    public volatile EstadoCatalogo estado;

    public EntradaCatalogo(long lsnDe, long lsnAte, long tamanho, EstadoCatalogo estado) {
        this.lsnDe = lsnDe;
        this.lsnAte = lsnAte;
        this.tamanho = tamanho;
        this.estado = estado;
    }
}
