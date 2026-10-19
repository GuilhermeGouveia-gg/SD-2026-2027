package rep02;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Verifica uma fotografia dos ficheiros: executar depois de parar a escrita. */
public final class PartitionCheck {
    private static final int R = 2;
    private static final Pattern LEADER = Pattern.compile("lider-p(\\d+)\\.txt");
    private static final Pattern REPLICA = Pattern.compile("replica-([A-Za-z0-9_-]+)-p(\\d+)\\.txt");

    private final Partitioner partitioner;
    private final Path folder;
    private boolean valid = true;

    private PartitionCheck(int n, Path folder) {
        partitioner = new Partitioner(n);
        this.folder = folder;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Uso: PartitionCheck <N> <pasta>");
            System.exit(1);
        }
        try {
            PartitionCheck check = new PartitionCheck(Integer.parseInt(args[0]), Paths.get(args[1]));
            if (!check.run()) System.exit(1);
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("VERIFICAÇÃO FALHOU: " + e.getMessage());
            System.exit(1);
        }
    }

    private boolean run() throws IOException {
        List<Path> files;
        try (Stream<Path> stream = Files.list(folder)) {
            files = stream.filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .collect(Collectors.toList());
        }
        // Detetar também ficheiros de uma configuração antiga com outro N.
        for (Path path : files) {
            String name = path.getFileName().toString();
            Matcher leader = LEADER.matcher(name);
            Matcher replica = REPLICA.matcher(name);
            if (leader.matches()) checkPartitionName(name, Integer.parseInt(leader.group(1)));
            if (replica.matches()) checkPartitionName(name, Integer.parseInt(replica.group(2)));
        }
        long total = 0;
        Set<String> identities = new HashSet<>();
        List<String> terms = new ArrayList<>();
        for (int k = 0; k < partitioner.count(); k++) {
            System.out.println("PARTIÇÃO p" + k);
            Path leaderPath = folder.resolve("lider-p" + k + ".txt");
            if (!Files.isRegularFile(leaderPath)) {
                error("Falta " + leaderPath.getFileName());
                continue;
            }
            List<String> leaderLines = Files.readAllLines(leaderPath, StandardCharsets.UTF_8);
            Set<String> sensors = inspect(leaderPath, leaderLines, k, identities);
            total += leaderLines.size();
            terms.add(Integer.toString(leaderLines.size()));
            System.out.println("  Registos: " + leaderLines.size() + " | Sensores distintos: " + sensors.size());
            List<Path> replicas = new ArrayList<>();
            for (Path path : files) {
                Matcher matcher = REPLICA.matcher(path.getFileName().toString());
                if (matcher.matches() && Integer.parseInt(matcher.group(2)) == k) replicas.add(path);
            }
            System.out.println("  Réplicas: " + replicas.size() + " | R esperado: " + R);
            if (replicas.size() != R) error("p" + k + " não tem exatamente " + R + " réplicas");
            for (Path replica : replicas) {
                List<String> lines = Files.readAllLines(replica, StandardCharsets.UTF_8);
                inspect(replica, lines, k, null);
                boolean prefix = lines.size() <= leaderLines.size()
                        && leaderLines.subList(0, lines.size()).equals(lines);
                String name = replica.getFileName().toString();
                if (!prefix) {
                    error(name + " diverge do líder (não é prefixo)");
                } else {
                    int missing = leaderLines.size() - lines.size();
                    System.out.println("  " + name + ": " + (missing == 0 ? "IGUAL" : "PREFIXO")
                            + " | Em falta: " + missing);
                    if (missing > 0) error(name + " ainda não convergiu");
                }
            }
        }
        // A identidade inclui a partição: seq=1 de p0 e seq=1 de p1 são distintos.
        System.out.println("TOTAL DO SISTEMA (sem contar cópias): " + identities.size());
        System.out.println("SOMA DOS REGISTOS DAS PARTIÇÕES: " + String.join(" + ", terms) + " = " + total);
        if (identities.size() != total) error("Total diferente da soma: há registos inválidos/repetidos");
        System.out.println(valid ? "VERIFICAÇÃO: OK" : "VERIFICAÇÃO: FALHOU");
        return valid;
    }

    private Set<String> inspect(Path path, List<String> lines, int k, Set<String> identities) {
        Set<String> sensors = new HashSet<>();
        for (int i = 0; i < lines.size(); i++) {
            try {
                SensorRecord r = SensorRecord.fromLine(lines.get(i));
                if (r.getSeq() != (long) i + 1) {
                    error(path.getFileName() + ": sequência inválida na linha " + (i + 1));
                }
                if (r.getSensorId().isBlank() || !Double.isFinite(r.getTemperature())
                        || r.getTimestamp().isBlank()) {
                    throw new IllegalArgumentException("Campos inválidos");
                }
                if (partitioner.of(r.getSensorId()) != k) {
                    error(path.getFileName() + ": sensor " + r.getSensorId() + " na partição errada");
                }
                sensors.add(r.getSensorId());
                if (identities != null) identities.add(k + ":" + r.getSeq());
            } catch (IllegalArgumentException e) {
                error(path.getFileName() + ": linha " + (i + 1) + " inválida: " + e.getMessage());
            }
        }
        return sensors;
    }

    private void checkPartitionName(String name, int k) {
        if (k >= partitioner.count()) error(name + ": partição fora de 0.." + (partitioner.count() - 1));
    }

    private void error(String message) {
        valid = false;
        System.out.println("  ERRO: " + message);
    }
}
