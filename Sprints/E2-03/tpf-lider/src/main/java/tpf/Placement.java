package tpf;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

public final class Placement {
    private Placement() { }

    public static List<String> choose(String chunkId, long tamanho, int count,
                                      List<ReplicaInfo> vivas, Set<String> exclude) {
        return choose(chunkId, tamanho, count, vivas, exclude, Wire.ALFA);
    }

    public static List<String> choose(String chunkId, long tamanho, int count,
                                      List<ReplicaInfo> vivas, Set<String> exclude,
                                      double alfa) {
        record Candidato(String id, double pontuacao) { }
        List<Candidato> candidatos = new ArrayList<>();
        for (ReplicaInfo replica : vivas) {
            if (exclude.contains(replica.id)) continue;
            long livre = replica.livre();
            if (livre < tamanho + Wire.MARGEM_BYTES) continue;
            double u = u(chunkId, replica.id);
            double pontuacao = Math.pow(livre, alfa) / -Math.log(u);
            candidatos.add(new Candidato(replica.id, pontuacao));
        }
        candidatos.sort(Comparator.comparingDouble(Candidato::pontuacao).reversed()
                .thenComparing(Candidato::id));
        List<String> resultado = new ArrayList<>();
        for (Candidato candidato : candidatos) {
            if (resultado.size() == count) break;
            resultado.add(candidato.id);
        }
        return resultado;
    }

    private static double u(String chunkId, String replicaId) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                    .digest((chunkId + "|" + replicaId).getBytes(StandardCharsets.UTF_8));
            long v = 0;
            for (int i = 0; i < 7; i++) v = (v << 8) | (hash[i] & 0xffL);
            v >>>= 3;
            return (v + 0.5d) / 9007199254740992d;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
