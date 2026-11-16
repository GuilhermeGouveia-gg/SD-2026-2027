package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class RaftCheck {
    private static final int GROUP_SIZE = 3;

    private static final Pattern NODE_FILE =
            Pattern.compile("no-p(\\d+)-n(\\d+)\\.txt");

    private static final Pattern STATE_FILE =
            Pattern.compile("estado-p(\\d+)-n(\\d+)\\.txt");

    private static final Pattern EVENT_FILE =
            Pattern.compile("eventos-p(\\d+)-n(\\d+)\\.txt");

    private static final Pattern HB_FILE =
            Pattern.compile("hb-p(\\d+)-n(\\d+)\\.txt");

    private final int n;

    private final Map<Integer, Map<Integer, List<Row>>> records =
            new TreeMap<>();

    private final Map<Integer, Map<Integer, Long>> persistentTerms =
            new TreeMap<>();

    private final Map<Integer, Map<Long, Election>> elections =
            new TreeMap<>();

    private final Map<Integer, Long> firstStarts = new TreeMap<>();

    private final List<Event> events = new ArrayList<>();
    private final List<Heartbeat> heartbeats = new ArrayList<>();
    private final List<Reading> readings = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();

    private int filesRead;
    private int monitorLines;
    private int decreasingTerms;

    private RaftCheck(int n) {
        this.n = n;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) {
            System.out.println(
                    "Uso: RaftCheck <N> <pasta> [falhas.txt]"
            );
            return;
        }

        int n = Integer.parseInt(args[0]);

        if (n < 1) {
            throw new IllegalArgumentException("N deve ser positivo");
        }

        Path folder = Path.of(args[1]).toAbsolutePath().normalize();

        if (!Files.isDirectory(folder)) {
            throw new IOException("Pasta inexistente: " + folder);
        }

        RaftCheck check = new RaftCheck(n);
        check.load(folder);

        System.out.println("RaftCheck: N=" + n);
        System.out.println("Pasta: " + folder);
        System.out.println("Ficheiros lidos: " + check.filesRead);

        check.reportElections();
        check.reportTermSafety();
        check.reportFirstLeaders();
        check.reportFinalRecords();
        check.reportLostReadings();
        check.reportTruncations();

        if (args.length == 3) {
            Path failures = Path.of(args[2]);

            if (!Files.exists(failures) && !failures.isAbsolute()) {
                failures = folder.resolve(failures);
            }

            check.reportFailures(failures);
        }

        System.out.println();
        System.out.println(
                "Linhas de monitorização lidas: " + check.monitorLines
        );

        if (!check.warnings.isEmpty()) {
            System.out.println();
            System.out.println("AVISOS:");

            for (String warning : check.warnings) {
                System.out.println("- " + warning);
            }
        }
    }

    // ---------------------------------------------------------
    // Leitura dos ficheiros
    // ---------------------------------------------------------

    private void load(Path folder) throws IOException {
        List<Path> files;

        try (var stream = Files.list(folder)) {
            files = stream
                    .filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(
                            path -> path.getFileName().toString()
                    ))
                    .collect(Collectors.toList());
        }

        for (Path file : files) {
            String name = file.getFileName().toString();
            Matcher matcher;

            matcher = NODE_FILE.matcher(name);

            if (matcher.matches()) {
                int partition = Integer.parseInt(matcher.group(1));
                int node = Integer.parseInt(matcher.group(2));

                if (validNode(partition, node, name)) {
                    loadRecords(file, partition, node);
                    filesRead++;
                }
                continue;
            }

            matcher = STATE_FILE.matcher(name);

            if (matcher.matches()) {
                int partition = Integer.parseInt(matcher.group(1));
                int node = Integer.parseInt(matcher.group(2));

                if (validNode(partition, node, name)) {
                    loadState(file, partition, node);
                    filesRead++;
                }
                continue;
            }

            matcher = EVENT_FILE.matcher(name);

            if (matcher.matches()) {
                int partition = Integer.parseInt(matcher.group(1));
                int node = Integer.parseInt(matcher.group(2));

                if (validNode(partition, node, name)) {
                    loadEvents(file, partition, node);
                    filesRead++;
                }
                continue;
            }

            matcher = HB_FILE.matcher(name);

            if (matcher.matches()) {
                int partition = Integer.parseInt(matcher.group(1));
                int node = Integer.parseInt(matcher.group(2));

                if (validNode(partition, node, name)) {
                    loadHeartbeats(file, partition, node);
                    filesRead++;
                }
                continue;
            }

            if (name.equals("produtor.txt")) {
                loadReadings(file);
                filesRead++;
            } else if (name.startsWith("monitor-")
                    && name.endsWith(".txt")) {
                monitorLines += lines(file).size();
                filesRead++;
            }
        }

        heartbeats.sort(Comparator.comparingLong(h -> h.time));
        readings.sort(Comparator.comparingLong(r -> r.time));
    }

    private boolean validNode(int partition, int node, String name) {
        if (partition < 0 || partition >= n
                || node < 1 || node > GROUP_SIZE) {
            warnings.add("Ficheiro fora da configuração: " + name);
            return false;
        }

        return true;
    }

    private static List<String> lines(Path file) throws IOException {
        return Files.readAllLines(file, StandardCharsets.UTF_8)
                .stream()
                .filter(line -> !line.isBlank())
                .collect(Collectors.toList());
    }

    private void loadRecords(
            Path file,
            int partition,
            int node
    ) throws IOException {
        List<Row> list = new ArrayList<>();
        long expected = 1;
        long previousTerm = 0;

        for (String line : lines(file)) {
            try {
                Row row = Row.parse(line);

                if (row.seq != expected) {
                    warnings.add(file.getFileName()
                            + ": esperado seq=" + expected
                            + ", encontrado=" + row.seq);
                }

                if (row.term < previousTerm) {
                    warnings.add(file.getFileName()
                            + ": termo dos registos diminuiu");
                }

                if (Math.floorMod(row.sensor.hashCode(), n)
                        != partition) {
                    warnings.add(file.getFileName()
                            + ": sensor na partição errada: "
                            + row.sensor);
                }

                list.add(row);
                expected = row.seq + 1;
                previousTerm = row.term;

            } catch (IllegalArgumentException e) {
                warnings.add(file.getFileName()
                        + ": registo inválido: " + line);
            }
        }

        records.computeIfAbsent(
                partition, k -> new TreeMap<>()
        ).put(node, list);
    }

    private void loadState(
            Path file,
            int partition,
            int node
    ) throws IOException {
        try {
            String[] fields = Files.readString(
                    file, StandardCharsets.UTF_8
            ).trim().split(";", -1);

            if (fields.length != 2) {
                throw new IllegalArgumentException();
            }

            long term = Long.parseLong(fields[0]);
            int votedFor = Integer.parseInt(fields[1]);

            if (term < 0 || votedFor < 0 || votedFor > GROUP_SIZE) {
                throw new IllegalArgumentException();
            }

            persistentTerms.computeIfAbsent(
                    partition, k -> new TreeMap<>()
            ).put(node, term);

        } catch (IllegalArgumentException e) {
            warnings.add("Estado inválido: " + file.getFileName());
        }
    }

    private void loadEvents(
            Path file,
            int partition,
            int node
    ) throws IOException {
        for (String line : lines(file)) {
            try {
                int separator = line.indexOf(';');

                if (separator < 0) {
                    throw new IllegalArgumentException();
                }

                long time = Long.parseLong(
                        line.substring(0, separator)
                );

                String body = line.substring(separator + 1);
                String type = body.split(";", 2)[0];

                Event event = new Event(
                        time, partition, node, type, body
                );

                events.add(event);

                if (type.equals("ARRANQUE")
                        || type.equals("START")) {
                    firstStarts.merge(partition, time, Math::min);
                }

                long term = currentTerm(event);

                if (term < 0) {
                    continue;
                }

                if (type.equals("CANDIDATO")) {
                    Election election = election(partition, term);

                    election.candidates.add(node);
                    election.firstCandidate = Math.min(
                            election.firstCandidate, time
                    );

                    // O candidato vota em si próprio.
                    election.votes.computeIfAbsent(
                            node, k -> new TreeSet<>()
                    ).add(node);
                }

                if (type.equals("VOTO")) {
                    int candidate = integerField(
                            body, "candidato", -1
                    );

                    boolean granted =
                            body.contains(";concedido")
                                    || body.contains(";concedido=true")
                                    || body.endsWith(";S");

                    if (candidate >= 1
                            && candidate <= GROUP_SIZE
                            && granted) {
                        election(partition, term).votes
                                .computeIfAbsent(
                                        candidate,
                                        k -> new TreeSet<>()
                                )
                                .add(node);
                    }
                }

                if (type.equals("LIDER")) {
                    Election election = election(partition, term);

                    // Dois logs LIDER do mesmo nó contam uma vez.
                    election.leaders.merge(node, time, Math::min);

                    Matcher voteList = Pattern.compile(
                            "votos=\\[([^\\]]*)\\]"
                    ).matcher(body);

                    if (voteList.find()) {
                        Set<Integer> votes =
                                election.votes.computeIfAbsent(
                                        node,
                                        k -> new TreeSet<>()
                                );

                        for (String value
                                : voteList.group(1).split(",")) {
                            if (!value.isBlank()) {
                                votes.add(Integer.parseInt(
                                        value.trim()
                                ));
                            }
                        }
                    }
                }

            } catch (IllegalArgumentException e) {
                warnings.add(file.getFileName()
                        + ": evento inválido: " + line);
            }
        }
    }

    private void loadHeartbeats(
            Path file,
            int partition,
            int node
    ) throws IOException {
        for (String line : lines(file)) {
            try {
                String[] fields = line.split(";", -1);

                long term;
                long time;

                if (line.contains("termo=")) {
                    // Compatibilidade com os logs antigos.
                    time = Long.parseLong(fields[0]);
                    term = longField(line, "termo", -1);
                } else {
                    // Formato pedido:
                    // termo;hb;instante;ultimoSeq;ultimoTermo;n;bytes
                    if (fields.length != 7) {
                        throw new IllegalArgumentException();
                    }

                    term = Long.parseLong(fields[0]);
                    time = Long.parseLong(fields[2]);
                }

                if (term < 1 || time < 0) {
                    throw new IllegalArgumentException();
                }

                heartbeats.add(
                        new Heartbeat(time, partition, node, term)
                );

            } catch (IllegalArgumentException e) {
                warnings.add(file.getFileName()
                        + ": heartbeat inválido: " + line);
            }
        }
    }

    private void loadReadings(Path file) throws IOException {
        for (String line : lines(file)) {
            try {
                // Aceita também os campos adicionais dos logs antigos.
                String[] fields = line.split(";", -1);

                if (fields.length < 4 || !fields[1].startsWith("p")) {
                    throw new IllegalArgumentException();
                }

                long time = Long.parseLong(fields[0]);
                int partition =
                        Integer.parseInt(fields[1].substring(1));

                String sensor = fields[2];
                double temperature = Double.parseDouble(fields[3]);

                if (partition < 0 || partition >= n
                        || sensor.isBlank()
                        || !Double.isFinite(temperature)) {
                    throw new IllegalArgumentException();
                }

                readings.add(new Reading(
                        time, partition, sensor, temperature
                ));

            } catch (IllegalArgumentException e) {
                warnings.add(
                        "produtor.txt: linha inválida: " + line
                );
            }
        }
    }

    private Election election(int partition, long term) {
        return elections
                .computeIfAbsent(
                        partition, k -> new TreeMap<>()
                )
                .computeIfAbsent(
                        term, k -> new Election()
                );
    }

    // ---------------------------------------------------------
    // Eleições
    // ---------------------------------------------------------

    private void reportElections() {
        System.out.println();
        System.out.println("ELEIÇÕES");

        int unsafeTerms = 0;

        for (int partition = 0; partition < n; partition++) {
            System.out.println("p" + partition + ":");

            Map<Long, Election> terms = elections.get(partition);

            if (terms == null || terms.isEmpty()) {
                System.out.println("  Sem eleições registadas.");
                continue;
            }

            for (Map.Entry<Long, Election> entry
                    : terms.entrySet()) {
                long term = entry.getKey();
                Election election = entry.getValue();

                System.out.println(
                        "  Termo " + term
                                + ": candidatos="
                                + election.candidates
                );

                for (Map.Entry<Integer, Set<Integer>> votes
                        : election.votes.entrySet()) {
                    System.out.println(
                            "    Candidato n" + votes.getKey()
                                    + ": votos concedidos por "
                                    + votes.getValue()
                    );
                }

                if (election.leaders.isEmpty()) {
                    System.out.println(
                            "    Sem líder observado."
                    );

                    if (election.candidates.size() > 1) {
                        System.out.println(
                                "    Possível voto dividido; "
                                        + "confirmar recusas e prazos."
                        );
                    }

                    continue;
                }

                if (election.leaders.size() > 1) {
                    unsafeTerms++;
                    System.out.println(
                            "    ERRO: mais de um líder no termo."
                    );
                }

                for (Map.Entry<Integer, Long> leader
                        : election.leaders.entrySet()) {
                    System.out.println(
                            "    Líder: n" + leader.getKey()
                    );

                    if (election.firstCandidate != Long.MAX_VALUE) {
                        long duration =
                                leader.getValue()
                                        - election.firstCandidate;

                        System.out.println(
                                "    Duração da eleição: "
                                        + duration + " ms"
                        );
                    }

                    Set<Integer> votes =
                            election.votes.get(leader.getKey());

                    if (votes == null
                            || votes.size() < GROUP_SIZE / 2 + 1) {
                        System.out.println(
                                "    AVISO: maioria não comprovada "
                                        + "pelos logs disponíveis."
                        );
                    }
                }
            }
        }

        System.out.println(
                "Termos com mais de um líder observado: "
                        + unsafeTerms
        );
    }

    private void reportTermSafety() {
        System.out.println();
        System.out.println("MONOTONIA DOS TERMOS");

        for (int partition = 0; partition < n; partition++) {
            for (int node = 1; node <= GROUP_SIZE; node++) {
                long previous = -1;
                boolean found = false;
                int violations = 0;

                // A ordem original do ficheiro é preservada.
                for (Event event : events) {
                    if (event.partition != partition
                            || event.node != node) {
                        continue;
                    }

                    long current = currentTerm(event);

                    if (current < 0) {
                        continue;
                    }

                    found = true;

                    if (current < previous) {
                        violations++;
                        decreasingTerms++;

                        System.out.println(
                                "ERRO p" + partition + "/n" + node
                                        + ": termo " + previous
                                        + " -> " + current
                                        + " em " + event.time
                        );
                    }

                    previous = current;
                }

                Long saved = persistentTerms
                        .getOrDefault(
                                partition, new TreeMap<>()
                        )
                        .get(node);

                if (saved != null
                        && previous >= 0
                        && saved < previous) {
                    violations++;
                    decreasingTerms++;

                    System.out.println(
                            "ERRO p" + partition + "/n" + node
                                    + ": estado persistente="
                                    + saved
                                    + ", último termo observado="
                                    + previous
                    );
                }

                System.out.println(
                        "p" + partition + "/n" + node
                                + ": "
                                + (!found
                                ? "sem histórico suficiente"
                                : violations == 0
                                ? "sem diminuições observadas"
                                : violations + " violações")
                                + "; termo persistente="
                                + (saved == null ? "ausente" : saved)
                );
            }
        }

        System.out.println(
                "Total de diminuições/inconsistências: "
                        + decreasingTerms
        );
    }

    private void reportFirstLeaders() {
        System.out.println();
        System.out.println("TEMPO ATÉ AO PRIMEIRO LÍDER");

        for (int partition = 0; partition < n; partition++) {
            long firstLeader = Long.MAX_VALUE;

            Map<Long, Election> terms = elections.get(partition);

            if (terms != null) {
                for (Election election : terms.values()) {
                    for (long time : election.leaders.values()) {
                        firstLeader = Math.min(firstLeader, time);
                    }
                }
            }

            Long start = firstStarts.get(partition);

            if (start == null || firstLeader == Long.MAX_VALUE) {
                System.out.println(
                        "p" + partition + ": não determinável"
                );
            } else {
                System.out.println(
                        "p" + partition + ": "
                                + (firstLeader - start) + " ms"
                );
            }
        }
    }

    // ---------------------------------------------------------
    // Coincidência dos históricos
    // ---------------------------------------------------------

    private List<Row> nodeRecords(int partition, int node) {
        return records
                .getOrDefault(partition, new TreeMap<>())
                .getOrDefault(node, new ArrayList<>());
    }

    private void reportFinalRecords() {
        System.out.println();
        System.out.println("COINCIDÊNCIA FINAL");

        for (int partition = 0; partition < n; partition++) {
            List<Map<Long, String>> indexes = new ArrayList<>();
            long minLast = Long.MAX_VALUE;
            long maxLast = 0;

            System.out.println("p" + partition + ":");

            for (int node = 1; node <= GROUP_SIZE; node++) {
                List<Row> rows = nodeRecords(partition, node);
                Map<Long, String> index = new TreeMap<>();
                long last = 0;

                for (Row row : rows) {
                    if (index.put(row.seq, row.line) != null) {
                        warnings.add(
                                "p" + partition + "/n" + node
                                        + ": seq repetido "
                                        + row.seq
                        );
                    }

                    last = Math.max(last, row.seq);
                }

                indexes.add(index);
                minLast = Math.min(minLast, last);
                maxLast = Math.max(maxLast, last);

                boolean exists = records
                        .getOrDefault(partition, new TreeMap<>())
                        .containsKey(node);

                System.out.println(
                        "  n" + node
                                + ": registos=" + rows.size()
                                + "; ultimoSeq=" + last
                                + (exists ? "" : "; ficheiro ausente")
                );
            }

            Set<Long> observed = new TreeSet<>();

            for (Map<Long, String> index : indexes) {
                for (long seq : index.keySet()) {
                    if (seq <= minLast) {
                        observed.add(seq);
                    }
                }
            }

            List<Long> differences = new ArrayList<>();

            for (long seq : observed) {
                String reference = indexes.get(0).get(seq);

                for (int i = 1; i < indexes.size(); i++) {
                    String other = indexes.get(i).get(seq);

                    if (reference == null
                            || !reference.equals(other)) {
                        differences.add(seq);
                        break;
                    }
                }
            }

            System.out.println(
                    "  Intervalo comum: 1.." + minLast
            );

            System.out.println(
                    "  Seq divergentes no intervalo comum: "
                            + differences
            );

            if (minLast != maxLast) {
                System.out.println(
                        "  Os nós têm comprimentos diferentes."
                );
            }

            if (minLast == 0) {
                System.out.println(
                        "  Sem prefixo comum não vazio "
                                + "para comparar."
                );
            } else if (differences.isEmpty()
                    && minLast == maxLast) {
                System.out.println(
                        "  Históricos coincidentes "
                                + "nos registos observados."
                );
            }
        }
    }

    // ---------------------------------------------------------
    // Leituras perdidas
    // ---------------------------------------------------------

    private void reportLostReadings() {
        System.out.println();
        System.out.println("LEITURAS DO PRODUTOR");

        if (readings.isEmpty()) {
            System.out.println(
                    "Sem leituras em produtor.txt."
            );
            return;
        }

        // União dos registos finais: uma cópia por conteúdo,
        // evitando contar a mesma leitura em três réplicas.
        Set<String> uniqueRecords = new HashSet<>();
        Map<String, Integer> available = new HashMap<>();

        for (Map.Entry<Integer, Map<Integer, List<Row>>> partition
                : records.entrySet()) {
            for (List<Row> rows : partition.getValue().values()) {
                for (Row row : rows) {
                    String identity =
                            partition.getKey() + ";" + row.line;

                    if (uniqueRecords.add(identity)) {
                        String key = readingKey(
                                partition.getKey(),
                                row.sensor,
                                row.temperature
                        );

                        available.merge(key, 1, Integer::sum);
                    }
                }
            }
        }

        Map<String, Integer> emittedCounts = new HashMap<>();

        for (Reading reading : readings) {
            emittedCounts.merge(reading.key(), 1, Integer::sum);
        }

        boolean repeated = emittedCounts.values()
                .stream()
                .anyMatch(count -> count > 1);

        int missing = 0;

        for (Reading reading : readings) {
            int count = available.getOrDefault(
                    reading.key(), 0
            );

            if (count > 0) {
                available.put(reading.key(), count - 1);
            } else {
                missing++;

                System.out.println(
                        "Sem correspondência final: "
                                + reading.time
                                + ";p" + reading.partition
                                + ";" + reading.sensor
                                + ";" + reading.temperature
                );
            }
        }

        System.out.println(
                "Leituras enviadas: " + readings.size()
        );

        System.out.println(
                "Leituras sem correspondência final: " + missing
        );

        if (repeated) {
            System.out.println(
                    "AVISO: existem leituras com o mesmo sensor "
                            + "e temperatura. Sem identificador único, "
                            + "a associação a cada envio é ambígua."
            );
        }

        System.out.println(
                "A comparação usa partição, sensor e temperatura; "
                        + "não constitui confirmação de commit."
        );
    }

    // ---------------------------------------------------------
    // Truncagens
    // ---------------------------------------------------------

    private void reportTruncations() {
        System.out.println();
        System.out.println("TRUNCAGENS");

        long total = 0;
        int count = 0;

        for (Event event : events) {
            if (!event.type.equals("TRUNCADO")) {
                continue;
            }

            count++;

            long discarded = longField(
                    event.body, "descartados", -1
            );

            if (discarded >= 0) {
                total += discarded;
            }

            System.out.println(
                    event.time + ";n" + event.node
                            + ";" + event.body
            );
        }

        System.out.println(
                "Eventos TRUNCADO: " + count
        );

        System.out.println(
                "Registos descartados indicados nos logs: " + total
        );

        if (count > 0) {
            System.out.println(
                    "Registos truncados que reaparecem com outro seq: "
                            + "não determinável apenas com "
                            + "de/descartados/termos."
            );

            System.out.println(
                    "Para essa contagem é necessário conservar "
                            + "o conteúdo dos registos descartados."
            );
        }
    }

    // ---------------------------------------------------------
    // Falhas e indisponibilidade
    // ---------------------------------------------------------

    private void reportFailures(Path file) throws IOException {
        System.out.println();
        System.out.println("FALHAS E INDISPONIBILIDADE");

        if (!Files.exists(file)) {
            System.out.println(
                    "Ficheiro de falhas inexistente: " + file
            );
            return;
        }

        for (String line : lines(file)) {
            try {
                String[] fields = line.trim().split(";", -1);

                if (fields.length != 3
                        || !fields[0].startsWith("p")
                        || !fields[1].startsWith("n")) {
                    throw new IllegalArgumentException();
                }

                int partition =
                        Integer.parseInt(fields[0].substring(1));

                int node =
                        Integer.parseInt(fields[1].substring(1));

                long failureTime = Long.parseLong(fields[2]);

                long oldTerm = -1;

                for (Heartbeat heartbeat : heartbeats) {
                    if (heartbeat.partition == partition
                            && heartbeat.node == node
                            && heartbeat.time <= failureTime) {
                        oldTerm = Math.max(
                                oldTerm, heartbeat.term
                        );
                    }
                }

                if (oldTerm < 0) {
                    Map<Long, Election> terms =
                            elections.get(partition);

                    if (terms != null) {
                        for (Map.Entry<Long, Election> entry
                                : terms.entrySet()) {
                            Long leaderTime =
                                    entry.getValue()
                                            .leaders.get(node);

                            if (leaderTime != null
                                    && leaderTime <= failureTime) {
                                oldTerm = Math.max(
                                        oldTerm, entry.getKey()
                                );
                            }
                        }
                    }
                }

                System.out.println(
                        "Falha: p" + partition
                                + "/n" + node
                                + " em " + failureTime
                );

                if (oldTerm < 0) {
                    System.out.println(
                            "  Termo do líder falhado "
                                    + "não determinável."
                    );
                    continue;
                }

                Heartbeat next = null;

                for (Heartbeat heartbeat : heartbeats) {
                    if (heartbeat.partition == partition
                            && heartbeat.time >= failureTime
                            && heartbeat.term > oldTerm) {
                        next = heartbeat;
                        break;
                    }
                }

                if (next == null) {
                    System.out.println(
                            "  Sem LOTE posterior "
                                    + "de termo superior observado."
                    );
                    continue;
                }

                long unavailable = next.time - failureTime;

                System.out.println(
                        "  Novo líder: n" + next.node
                                + "; termo=" + next.term
                );

                System.out.println(
                        "  Indisponibilidade: "
                                + unavailable + " ms"
                );

                int sentDuringFailure = 0;

                for (Reading reading : readings) {
                    if (reading.partition == partition
                            && reading.time >= failureTime
                            && reading.time < next.time) {
                        sentDuringFailure++;

                        System.out.println(
                                "  Enviada nesse intervalo: "
                                        + reading.time
                                        + ";" + reading.sensor
                                        + ";" + reading.temperature
                        );
                    }
                }

                System.out.println(
                        "  Leituras enviadas durante "
                                + "a indisponibilidade: "
                                + sentDuringFailure
                );

            } catch (IllegalArgumentException e) {
                warnings.add(
                        "Linha de falha inválida: " + line
                );
            }
        }
    }

    // ---------------------------------------------------------
    // Auxiliares
    // ---------------------------------------------------------

    private static long currentTerm(Event event) {
        switch (event.type) {
            case "ARRANQUE":
            case "CANDIDATO":
            case "VOTO":
            case "LIDER":
            case "NOVO_LIDER":
            case "DEMISSAO":
            case "RECEBIDO":
                return longField(event.body, "termo", -1);

            case "TERMO":
                Matcher matcher = Pattern.compile(
                        "(\\d+)\\s*->\\s*(\\d+)"
                ).matcher(event.body);

                return matcher.find()
                        ? Long.parseLong(matcher.group(2))
                        : longField(event.body, "termo", -1);

            default:
                // Um IGNORADO pode mencionar um termo antigo;
                // esse termo não é o termo atual do nó.
                return -1;
        }
    }

    private static long longField(
            String text,
            String field,
            long fallback
    ) {
        Matcher matcher = Pattern.compile(
                "(?:^|;)" + Pattern.quote(field)
                        + "=(-?\\d+)(?:;|$)"
        ).matcher(text);

        return matcher.find()
                ? Long.parseLong(matcher.group(1))
                : fallback;
    }

    private static int integerField(
            String text,
            String field,
            int fallback
    ) {
        Matcher matcher = Pattern.compile(
                "(?:^|;)" + Pattern.quote(field)
                        + "=n?(\\d+)(?:;|$)"
        ).matcher(text);

        return matcher.find()
                ? Integer.parseInt(matcher.group(1))
                : fallback;
    }

    private static String readingKey(
            int partition,
            String sensor,
            double temperature
    ) {
        return partition + ";" + sensor + ";"
                + Double.toHexString(temperature);
    }

    private static final class Election {
        final Set<Integer> candidates = new TreeSet<>();

        final Map<Integer, Set<Integer>> votes =
                new TreeMap<>();

        final Map<Integer, Long> leaders =
                new TreeMap<>();

        long firstCandidate = Long.MAX_VALUE;
    }

    private static final class Event {
        final long time;
        final int partition;
        final int node;
        final String type;
        final String body;

        Event(
                long time,
                int partition,
                int node,
                String type,
                String body
        ) {
            this.time = time;
            this.partition = partition;
            this.node = node;
            this.type = type;
            this.body = body;
        }
    }

    private static final class Heartbeat {
        final long time;
        final int partition;
        final int node;
        final long term;

        Heartbeat(
                long time,
                int partition,
                int node,
                long term
        ) {
            this.time = time;
            this.partition = partition;
            this.node = node;
            this.term = term;
        }
    }

    private static final class Reading {
        final long time;
        final int partition;
        final String sensor;
        final double temperature;

        Reading(
                long time,
                int partition,
                String sensor,
                double temperature
        ) {
            this.time = time;
            this.partition = partition;
            this.sensor = sensor;
            this.temperature = temperature;
        }

        String key() {
            return readingKey(
                    partition, sensor, temperature
            );
        }
    }

    private static final class Row {
        final long seq;
        final long term;
        final String sensor;
        final double temperature;
        final String line;

        Row(
                long seq,
                long term,
                String sensor,
                double temperature,
                String line
        ) {
            this.seq = seq;
            this.term = term;
            this.sensor = sensor;
            this.temperature = temperature;
            this.line = line;
        }

        static Row parse(String line) {
            String[] fields = line.trim().split(";", -1);

            if (fields.length != 7) {
                throw new IllegalArgumentException();
            }

            long seq = Long.parseLong(fields[0]);
            long term = Long.parseLong(fields[1]);
            String sensor = fields[2];
            double temperature =
                    Double.parseDouble(fields[3]);

            if (seq < 1 || term < 1
                    || sensor.isBlank()
                    || !Double.isFinite(temperature)) {
                throw new IllegalArgumentException();
            }

            return new Row(
                    seq,
                    term,
                    sensor,
                    temperature,
                    line.trim()
            );
        }
    }
}