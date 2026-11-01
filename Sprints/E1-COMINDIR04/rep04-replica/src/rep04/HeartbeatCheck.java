package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Análise a posteriori; compara relógios físicos da mesma máquina. */
public final class HeartbeatCheck {
    private static final class Stats {
        long count, max;
        double sum;
        void add(long v) { count++; sum += v; max = count == 1 ? v : Math.max(max, v); }
        String summary() { return count == 0 ? "N/A" : String.format(Locale.ROOT,
                "media=%.3f max=%d amostras=%d", sum / count, max, count); }
    }
    private static final class HB {
        final int k;
        final long inc, hb, time, last, count, bytes;
        HB(int k, String line) {
            String[] f = line.split(";", -1);
            if (f.length != 6) throw new IllegalArgumentException("Linha hb inválida: " + line);
            this.k = k;
            inc = Long.parseLong(f[0]); hb = Long.parseLong(f[1]); time = Long.parseLong(f[2]);
            last = Long.parseLong(f[3]); count = Long.parseLong(f[4]); bytes = Long.parseLong(f[5]);
            if (inc < 0 || hb < 1 || time < 0 || last < 0 || count < 0 || count > last || bytes < 1)
                throw new IllegalArgumentException("Valores hb inválidos: " + line);
        }
    }
    private static final class Event {
        final long time;
        final String type;
        final int k;
        final Map<String, String> data = new HashMap<>();
        Event(String line) {
            String[] f = line.split(";", -1);
            if (f.length < 4 || !f[2].matches("p\\d+"))
                throw new IllegalArgumentException("Linha monitor inválida: " + line);
            time = Long.parseLong(f[0]); type = f[1]; k = Integer.parseInt(f[2].substring(1));
            for (int i = 3; i < f.length; i++) {
                int eq = f[i].indexOf('=');
                if (eq > 0) data.put(f[i].substring(0, eq), f[i].substring(eq + 1));
            }
        }
        long number(String key) { return Long.parseLong(data.get(key)); }
    }

    private HeartbeatCheck() { }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Uso: HeartbeatCheck <N> <pasta> [falhas.txt]");
            System.exit(1);
        }
        try {
            int n = Integer.parseInt(args[0]);
            if (n < 1) throw new IllegalArgumentException("N tem de ser positivo");
            Path folder = Path.of(args[1]);
            String report = analyze(n, folder, args.length == 3 ? Path.of(args[2]) : null);
            Files.writeString(folder.resolve("relatorio-heartbeat.txt"), report, StandardCharsets.UTF_8);
            System.out.print(report);
        } catch (Exception e) {
            System.err.println("ERRO HEARTBEATCHECK: " + e.getMessage());
            System.exit(1);
        }
    }

    private static List<String> lines(Path path) throws IOException {
        return Files.readAllLines(path, StandardCharsets.UTF_8).stream()
                .filter(s -> !s.isBlank()).collect(Collectors.toList());
    }

    private static String analyze(int n, Path folder, Path failuresFile) throws IOException {
        StringBuilder out = new StringBuilder("RELATÓRIO HEARTBEATCHECK\n");
        out.append("Medições físicas: processos na mesma máquina; unidade ms salvo indicação.\n");
        List<HB> published = new ArrayList<>();
        Map<Integer, Map<Long, SensorRecord>> records = new HashMap<>();
        for (int k = 0; k < n; k++) {
            Map<Long, SensorRecord> rs = new LinkedHashMap<>();
            for (String line : lines(folder.resolve("lider-p" + k + ".txt"))) {
                SensorRecord r = SensorRecord.fromLine(line, n);
                if (rs.put(r.getSeq(), r) != null) throw new IllegalArgumentException("seq repetido no líder p" + k);
            }
            records.put(k, rs);
            List<HB> hbs = new ArrayList<>();
            for (String line : lines(folder.resolve("hb-p" + k + ".txt"))) hbs.add(new HB(k, line));
            published.addAll(hbs);
            Map<Long, List<HB>> incarnations = hbs.stream().collect(Collectors.groupingBy(
                    h -> h.inc, LinkedHashMap::new, Collectors.toList()));
            if (hbs.isEmpty()) out.append("p").append(k).append(": sem publicações\n");
            for (Map.Entry<Long, List<HB>> entry : incarnations.entrySet()) {
                Stats sizes = new Stats(), intervals = new Stats();
                long empty = 0, previousTime = -1, previousHb = 0;
                for (HB h : entry.getValue()) {
                    sizes.add(h.count);
                    if (h.count == 0) empty++;
                    if (h.hb <= previousHb) throw new IllegalArgumentException("hb não crescente em p" + k);
                    if (previousTime >= 0) intervals.add(h.time - previousTime);
                    previousTime = h.time; previousHb = h.hb;
                }
                out.append(String.format(Locale.ROOT,
                        "p%d inc=%d lotes=%d vazios=%d proporcao_vazios=%.6f registos/lote[%s] intervalo_ms[%s]\n",
                        k, entry.getKey(), sizes.count, empty, (double) empty / sizes.count,
                        sizes.summary(), intervals.summary()));
            }
        }
        List<long[]> failures = new ArrayList<>();
        if (failuresFile != null) {
            for (String line : lines(failuresFile)) {
                String[] f = line.split(";", -1);
                if (f.length != 2 || !f[0].matches("p\\d+")) throw new IllegalArgumentException("falhas.txt inválido");
                int k = Integer.parseInt(f[0].substring(1));
                if (k >= n) throw new IllegalArgumentException("Partição de falha fora de N");
                failures.add(new long[] {k, Long.parseLong(f[1])});
            }
        }
        List<Path> monitorFiles;
        try (Stream<Path> stream = Files.list(folder)) {
            monitorFiles = stream.filter(p -> p.getFileName().toString().matches("monitor-.+\\.txt"))
                    .sorted().collect(Collectors.toList());
        }
        if (monitorFiles.isEmpty()) throw new IllegalArgumentException("Sem ficheiros monitor-*.txt");
        Map<Integer, Stats> latency = new HashMap<>();
        StringBuilder samples = new StringBuilder("participante;particao;seq;recebido_ms;registo_ms;latencia_ms\n");
        for (Path file : monitorFiles) {
            String id = file.getFileName().toString().replace("monitor-", "").replace(".txt", "");
            List<Event> events = new ArrayList<>();
            for (String line : lines(file)) {
                Event e = new Event(line);
                if (e.k >= n) throw new IllegalArgumentException("Partição de monitor fora de N");
                events.add(e);
            }
            // A ordem do ficheiro resolve empates no mesmo milissegundo.
            long[] inc = new long[n], hb = new long[n];
            java.util.Arrays.fill(inc, -1);
            Map<Integer, Stats> localLatency = new HashMap<>();
            Set<String> measured = new HashSet<>();
            long lost = 0, recovered = 0;
            for (int index = 0; index < events.size(); index++) {
                Event e = events.get(index);
                if (e.type.equals("RECEBIDO")) {
                    long ei = e.number("inc"), eh = e.number("hb");
                    if (ei > inc[e.k] || (ei == inc[e.k] && eh > hb[e.k])) {
                        inc[e.k] = ei; hb[e.k] = eh;
                    }
                    String range = e.data.get("seq");
                    if (!id.equals("produtor") && range != null && !range.equals("-")) {
                        String[] bounds = range.split("\\.\\.");
                        long a = Long.parseLong(bounds[0]), b = Long.parseLong(bounds[1]);
                        if (b < a || b - a + 1 != e.number("n")) throw new IllegalArgumentException("Intervalo RECEBIDO inválido");
                        if (b != e.number("ultimoSeq")) out.append("AVISO snapshot incoerente: ")
                                .append(id).append(" p").append(e.k).append(" hb=").append(eh)
                                .append(" ultimoSeq=").append(e.number("ultimoSeq")).append(" fim_lote=").append(b).append('\n');
                        for (long seq = a; seq <= b; seq++) {
                            if (!measured.add(e.k + ":" + seq)) continue;
                            SensorRecord r = records.get(e.k).get(seq);
                            if (r == null) throw new IllegalArgumentException("Registo ausente no líder: p" + e.k + "#" + seq);
                            long written = LocalDateTime.parse(r.getTimestamp()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                            long delay = e.time - written;
                            samples.append(id).append(";p").append(e.k).append(';').append(seq)
                                    .append(';').append(e.time).append(';').append(written).append(';').append(delay).append('\n');
                            localLatency.computeIfAbsent(e.k, key -> new Stats()).add(delay);
                            latency.computeIfAbsent(e.k, key -> new Stats()).add(delay);
                        }
                    }
                } else if (e.type.equals("HB_PERDIDO")) {
                    lost++;
                } else if (e.type.equals("RECUPERACAO")) {
                    recovered++;
                } else if (e.type.equals("SUSPEITA")) {
                    long incarnation = inc[e.k];
                    boolean falseSuspicion = false;
                    for (int j = index + 1; j < events.size(); j++) {
                        Event next = events.get(j);
                        if (next.k != e.k) continue;
                        if (next.type.equals("RETOMA")) {
                            falseSuspicion = next.number("inc") == incarnation;
                            break;
                        }
                        if (next.type.equals("REINICIO")) break;
                    }
                    HB last = null;
                    for (HB h : published) {
                        if (h.k == e.k && h.inc == incarnation && h.time <= e.time
                                && (last == null || h.time > last.time)) last = h;
                    }
                    String classification = incarnation < 0 ? "INDETERMINADA" : falseSuspicion ? "FALSA" : "REAL";
                    out.append(id).append(" p").append(e.k).append(" SUSPEITA t=").append(e.time)
                            .append(" inc=").append(incarnation).append(" classificacao=").append(classification)
                            .append(" silencio_publicacao_ms=").append(last == null ? "N/A" : e.time - last.time).append('\n');
                    if (classification.equals("REAL") && failuresFile != null) {
                        long[] failure = null;
                        long nextStart = Long.MAX_VALUE;
                        for (HB h : published) if (h.k == e.k && h.inc != incarnation && h.inc > incarnation)
                            nextStart = Math.min(nextStart, h.inc);
                        for (long[] f : failures) if (f[0] == e.k && f[1] <= e.time
                                && f[1] >= incarnation && f[1] < nextStart
                                && (failure == null || f[1] > failure[1])) failure = f;
                        out.append("  deteccao_ms=").append(failure == null ? "N/A (falha não correlacionada)" : e.time - failure[1]).append('\n');
                    }
                }
            }
            for (Map.Entry<Integer, Stats> entry : localLatency.entrySet()) out.append(id)
                    .append(" p").append(entry.getKey()).append(" latencia_lote_ms[").append(entry.getValue().summary()).append("]\n");
            out.append(id).append(" HB_PERDIDO=").append(lost).append(" RECUPERACAO=").append(recovered).append('\n');
        }
        for (int k = 0; k < n; k++) out.append("p").append(k).append(" latencia_lote_ms_todos_receptores[")
                .append(latency.getOrDefault(k, new Stats()).summary()).append("]\n");
        long bytes = 0, allBytes = 0, count = 0, regBytes = 0;
        // Os logs hb não guardam o endpoint: a comparação REG explicita a hipótese.
        String address = System.getProperty("rep04.endereco", "127.0.0.1");
        for (HB h : published) {
            allBytes += h.bytes;
            if (h.count == 0) continue;
            bytes += h.bytes; count += h.count;
            for (long seq = h.last - h.count + 1; seq <= h.last; seq++) {
                SensorRecord r = records.get(h.k).get(seq);
                if (r == null) throw new IllegalArgumentException("Registo publicado ausente no histórico");
                regBytes += ("REG;" + address + ";" + (Wire.PORTO_BASE + h.k) + ";" + r.toLine())
                        .getBytes(StandardCharsets.UTF_8).length;
            }
        }
        out.append("Bytes UTF-8 da mensagem de aplicação; sem HTTP/IPFS/transporte. REG usa endereco=").append(address).append('\n');
        if (count == 0) out.append("bytes/registo LOTE=N/A REG=N/A\n");
        else out.append(String.format(Locale.ROOT,
                "bytes/registo LOTE=%.3f REG=%.3f LOTE_incluindo_vazios=%.3f registos=%d\n",
                (double) bytes / count, (double) regBytes / count, (double) allBytes / count, count));
        out.append("REAL segue a classificação observacional da ficha; um ensaio terminado antes da retoma não prova uma falha.\n");
        out.append("Latência mede receção do LOTE, exclui recuperações TCP e inclui rede/escalonamento.\n");
        Files.writeString(folder.resolve("latencias-lote.csv"), samples, StandardCharsets.UTF_8);
        out.append("Amostras individuais: latencias-lote.csv\n");
        return out.toString();
    }
}
