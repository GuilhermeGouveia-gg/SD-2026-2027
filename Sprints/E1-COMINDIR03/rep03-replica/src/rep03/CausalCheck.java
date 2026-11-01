package rep03;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

public final class CausalCheck {
    private static final class Entry {
        final int k;
        final SensorRecord r;

        Entry(int k, SensorRecord r) {
            this.k = k;
            this.r = r;
        }
    }

    private final int n;
    private final Path folder;

    public CausalCheck(int n, Path folder) {
        if (n < 1) {
            throw new IllegalArgumentException("N tem de ser positivo.");
        }
        this.n = n;
        this.folder = folder;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Uso: CausalCheck <N> <pasta>");
            System.exit(1);
        }
        try {
            int n = Integer.parseInt(args[0]);
            Path folder = Paths.get(args[1]);
            new CausalCheck(n, folder).run();
        } catch (Exception e) {
            System.err.println("ERRO CAUSALCHECK: " + e.getMessage());
            System.exit(1);
        }
    }

    public void run() throws IOException {
        List<List<Entry>> byPartition = new ArrayList<>();
        List<Entry> all = new ArrayList<>();
        int seqErrors = 0;

        for (int k = 0; k < n; k++) {
            List<Entry> pList = new ArrayList<>();
            Path file = folder.resolve("lider-p" + k + ".txt");
            if (Files.exists(file)) {
                List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
                for (String line : lines) {
                    if (line.isBlank()) continue;
                    SensorRecord r = SensorRecord.fromLine(line, n);
                    if (r.getVector().get(k) != r.getSeq()) {
                        System.err.println("VIOLAÇÃO V[" + k + "] != seq em p" + k + ": " + line);
                        seqErrors++;
                    }
                    Entry e = new Entry(k, r);
                    pList.add(e);
                    all.add(e);
                }
            }
            byPartition.add(pList);
        }

        // Comparação de pares entre partições diferentes
        long totalPairs = 0;
        long causalPairs = 0;
        long concurrentPairs = 0;
        long concurrentDiffL = 0;
        int lamportViolations = 0;

        for (int p = 0; p < n; p++) {
            for (int q = p + 1; q < n; q++) {
                List<Entry> listP = byPartition.get(p);
                List<Entry> listQ = byPartition.get(q);

                for (Entry ep : listP) {
                    for (Entry eq : listQ) {
                        totalPairs++;
                        VectorClock.Order order = ep.r.getVector().compare(eq.r.getVector());

                        if (order == VectorClock.Order.BEFORE) {
                            causalPairs++;
                            if (ep.r.getLamport() >= eq.r.getLamport()) {
                                System.err.println("VIOLAÇÃO LAMPORT: " + ep.r.toLine() + " -> " + eq.r.toLine()
                                        + " mas L(" + ep.r.getLamport() + ") >= L(" + eq.r.getLamport() + ")");
                                lamportViolations++;
                            }
                        } else if (order == VectorClock.Order.AFTER) {
                            causalPairs++;
                            if (eq.r.getLamport() >= ep.r.getLamport()) {
                                System.err.println("VIOLAÇÃO LAMPORT: " + eq.r.toLine() + " -> " + ep.r.toLine()
                                        + " mas L(" + eq.r.getLamport() + ") >= L(" + ep.r.getLamport() + ")");
                                lamportViolations++;
                            }
                        } else {
                            concurrentPairs++;
                            if (ep.r.getLamport() != eq.r.getLamport()) {
                                concurrentDiffL++;
                            }
                        }
                    }
                }
            }
        }

        // Escrever vista-lamport.txt ordenado por (L, k)
        all.sort(Comparator.comparingLong((Entry e) -> e.r.getLamport())
                .thenComparingInt(e -> e.k)
                .thenComparingLong(e -> e.r.getSeq()));

        Path vistaFile = folder.resolve("vista-lamport.txt");
        try (BufferedWriter bw = Files.newBufferedWriter(vistaFile, StandardCharsets.UTF_8)) {
            for (Entry e : all) {
                bw.write(e.r.toLine());
                bw.newLine();
            }
        }

        // Relatório
        System.out.println("=== RELATÓRIO CAUSALCHECK (N = " + n + ") ===");
        System.out.println("Pasta analisada: " + folder);
        for (int k = 0; k < n; k++) {
            System.out.println("  - lider-p" + k + ".txt: " + byPartition.get(k).size() + " registos");
        }
        System.out.println();
        System.out.println("1. Equivalência V[k] == seq:");
        if (seqErrors == 0) {
            System.out.println("   [OK] Todos os registos cumprem V[k] == seq.");
        } else {
            System.out.println("   [ERRO] Detetadas " + seqErrors + " violações de V[k] == seq.");
        }
        System.out.println();
        System.out.println("2. Classificação de pares entre partições diferentes:");
        System.out.println("   Total de pares entre partições: " + totalPairs);
        System.out.println("   - Pares causalmente relacionados: " + causalPairs);
        System.out.println("   - Pares concorrentes: " + concurrentPairs);
        System.out.println();
        System.out.println("3. Validação da condição de Lamport (a -> b => L(a) < L(b)):");
        if (lamportViolations == 0) {
            System.out.println("   [OK] Nenhuma violação da condição de Lamport.");
        } else {
            System.out.println("   [ERRO] Detetadas " + lamportViolations + " violações de Lamport.");
        }
        System.out.println();
        System.out.println("4. Pares concorrentes com L distinto:");
        double prop = concurrentPairs > 0 ? (double) concurrentDiffL / concurrentPairs * 100.0 : 0.0;
        System.out.println(String.format(Locale.ROOT,
                "   Pares concorrentes com L distinto: %d / %d (%.2f%%)",
                concurrentDiffL, concurrentPairs, prop));
        System.out.println();
        System.out.println("5. Vista global de Lamport:");
        System.out.println("   Escrita em: " + vistaFile + " (" + all.size() + " registos ordenados por (L, k))");
        System.out.println("=============================================");
    }
}
