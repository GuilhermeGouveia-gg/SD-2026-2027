package tpf;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/** Instante em milissegundos desde o epoch; autor e canal são IDs do Discord. */
public record Registo(long lsn, long instante, String autor, String canal, String texto) {
    public Registo {
        if (lsn < 1 || instante < 0) throw new IllegalArgumentException("LSN/instante inválido");
        Objects.requireNonNull(autor);
        Objects.requireNonNull(canal);
        Objects.requireNonNull(texto);
        // Estes campos são IDs, não nomes livres: não podem quebrar as linhas do log.
        if (autor.matches("(?s).*[;\\r\\n].*") || canal.matches("(?s).*[;\\r\\n].*"))
            throw new IllegalArgumentException("Autor/canal contém separadores");
    }

    public String linha() {
        return lsn + ";" + instante + ";" + autor + ";" + canal + ";"
                + Base64.getEncoder().encodeToString(texto.getBytes(StandardCharsets.UTF_8));
    }

    public static Registo lerLinha(String linha) {
        String[] campos = linha.split(";", -1);
        if (campos.length != 5) throw new IllegalArgumentException("Linha de log inválida");
        return new Registo(Long.parseLong(campos[0]), Long.parseLong(campos[1]), campos[2], campos[3],
                new String(Base64.getDecoder().decode(campos[4]), StandardCharsets.UTF_8));
    }
}
