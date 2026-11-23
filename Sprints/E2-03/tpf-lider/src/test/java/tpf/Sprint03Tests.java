package tpf;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Runner offline do Sprint E2-03: JDK 17, sem JUnit ou rede.
 * Executar via mvn test compile ou javac + java.
 */
public final class Sprint03Tests {
    private static int checks;

    public static void main(String[] args) throws Exception {
        placement();
        reservasConcorrentes();
        heartbeats();
        catalogo();
        ed25519();
        quotasMinorias();
        System.out.println("OK: " + checks + " verificacoes E2-03");
    }

    private static void placement() throws Exception {
        PlacementResults r1 = distribuir(1.0);
        PlacementResults r0 = distribuir(0.0);
        PlacementResults r2 = distribuir(2.0);
        check(r1.counts.get("r8") > r1.counts.get("r6"), "A replica de 8MB lidera em alfa=1");
        check(r1.counts.get("r2") < r1.counts.get("r4"), "A replica de 2MB fica abaixo da de 4MB");
        int r0Max = Collections.max(r0.counts.values());
        int r0Min = Collections.min(r0.counts.values());
        check(r0Max - r0Min < 1500, "Alfa=0 distribui uniformemente sem peso de espaco livre");
        check(r2.counts.get("r8") > r2.counts.get("r6"), "Alfa=2 favorece replicas maiores");
        check(r1.total == 10_000, "Placement executado 10000 vezes");
        check(distribuir(1.0).counts.equals(r1.counts), "Placement deterministico");
    }

    private static void reservasConcorrentes() throws Exception {
        ReplicaRegistry registry = new ReplicaRegistry();
        replica(registry, "r1", 100_000, 0);
        replica(registry, "r2", 100_000, 0);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<List<String>>> tarefas = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                String chunk = "chunk-" + i;
                tarefas.add(pool.submit(() ->
                        registry.reserveAndChoose(chunk, 10_000, 1, Set.of())));
            }
            for (Future<List<String>> tarefa : tarefas) tarefa.get(5, TimeUnit.SECONDS);
            for (ReplicaInfo replica : registry.todas()) {
                check(replica.reservados <= replica.quota, "Reserva nao ultrapassa quota");
                check(replica.livre() >= 0, "Livre nunca negativo");
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private static void heartbeats() throws Exception {
        ReplicaRegistry registry = new ReplicaRegistry();
        replica(registry, "r1", 100, 0);
        long agora = System.currentTimeMillis();
        registry.onHeartbeat("r1", 10, 100, agora);
        check(registry.info("r1").estado == ReplicaEstado.VIVA, "HB coloca replica VIVA");
        registry.atualizarEstados(agora + Wire.SUSPEITA_MS);
        check(registry.info("r1").estado == ReplicaEstado.SUSPEITA, "SUSPEITA apos limite");
        registry.atualizarEstados(agora + Wire.MORTA_MS);
        check(registry.info("r1").estado == ReplicaEstado.MORTA, "MORTA apos limite");
        registry.onHeartbeat("r1", 20, 100, agora + Wire.MORTA_MS + 1);
        check(registry.info("r1").estado == ReplicaEstado.VIVA, "HB novo recupera VIVA");
        check(registry.info("r1").livre() == 80, "Livre atualiza usados");
    }

    private static void catalogo() throws Exception {
        Path pasta = Files.createTempDirectory("tpf-sprint3-catalog");
        Path file = pasta.resolve("catalogo.txt");
        Catalog catalog = new Catalog(file);
        catalog.adicionar("aaaa", 1, 2, 1024);
        catalog.confirmar("aaaa", "r1");
        check(catalogCopiasEstado(catalog, "aaaa").equals("SELADO"), "Um ACK fica SELADO");
        catalog.confirmar("aaaa", "r2");
        check(catalogCopiasEstado(catalog, "aaaa").equals("DURAVEL"), "Maioria torna DURAVEL");
        catalog.confirmar("aaaa", "r3");
        catalog.gravarAtomico();
        check(Files.exists(file), "Catalogo escrito");
        Catalog reload = new Catalog(file);
        check(reload.copias("aaaa").equals(Set.of("r1", "r2", "r3")),
                "Catalogo sobrevive ao reinicio");
    }

    private static void ed25519() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        KeyPair correta = generator.generateKeyPair();
        KeyPair errada = generator.generateKeyPair();
        String nonce = Base64.getEncoder().encodeToString(new byte[]{1, 2, 3});
        byte[] texto = ("tpf-registo|" + "r1|" + nonce).getBytes(StandardCharsets.UTF_8);
        Signature assinatura = Signature.getInstance("Ed25519");
        assinatura.initSign(correta.getPrivate());
        assinatura.update(texto);
        byte[] prova = assinatura.sign();

        PubSubHub hub = new PubSubHub(
                new java.net.InetSocketAddress("127.0.0.1", 0),
                Files.createTempDirectory("tpf-sprint3-hub"), null);
        check(hub.verificarProva(correta.getPublic(), "r1", nonce,
                        Base64.getEncoder().encodeToString(prova)),
                "PROVA correta aceita");
        check(!hub.verificarProva(errada.getPublic(), "r1", nonce,
                        Base64.getEncoder().encodeToString(prova)),
                "Outra chave recusada");
        check(!hub.verificarProva(correta.getPublic(), "r2", nonce,
                        Base64.getEncoder().encodeToString(prova)),
                "Outro id recusado");
    }

    private static void quotasMinorias() throws Exception {
        ReplicaRegistry registry = new ReplicaRegistry();
        replica(registry, "r1", 100_000, 0);
        replica(registry, "r2", 100_000, 0);
        replica(registry, "r3", 100_000, 0);
        List<String> escolhidas = registry.reserveAndChoose("c1", 1000, 3, Set.of());
        check(escolhidas.size() == 3, "Tres replicas vivas recebem tres copias");
        for (String id : escolhidas) registry.libertar(id, 1000);

        ReplicaRegistry pequena = new ReplicaRegistry();
        replica(pequena, "grande", 1_000_000, 0);
        replica(pequena, "pequena", 1_000, 0);
        check(!pequena.reserveAndChoose("c2", 100_000, 1, Set.of()).contains("pequena"),
                "Replica com margem insuficiente nao e escolhida");

        Catalog dois = new Catalog(Files.createTempDirectory("tpf-sprint3-minoria")
                .resolve("catalogo.txt"));
        dois.adicionar("c2", 1, 1, 100);
        dois.confirmar("c2", "r1");
        dois.confirmar("c2", "r2");
        check(catalogCopiasEstado(dois, "c2").equals("DURAVEL"),
                "Dois ACK tornam DURAVEL com W_MIN=2");
        check(dois.copias("c2").size() == 2, "Dois de tres fica subreplicado");

        Catalog um = new Catalog(Files.createTempDirectory("tpf-sprint3-um")
                .resolve("catalogo.txt"));
        um.adicionar("c3", 2, 2, 100);
        um.confirmar("c3", "r1");
        check(catalogCopiasEstado(um, "c3").equals("SELADO"),
                "Um ACK nao torna DURAVEL");
    }

    private static String catalogCopiasEstado(Catalog catalog, String chunk) {
        for (String linha : catalog.linhasEstado()) {
            if (linha.startsWith(chunk + ";")) return linha.split(";")[1];
        }
        throw new AssertionError("Chunk ausente");
    }

    private static void replica(ReplicaRegistry registry, String id,
                                long quota, long usados) {
        registry.onHeartbeat(id, usados, quota, System.currentTimeMillis());
    }

    private static PlacementResults distribuir(double alfa) throws Exception {
        Map<String, Integer> counts = new LinkedHashMap<>();
        counts.put("r8", 0);
        counts.put("r6", 0);
        counts.put("r4", 0);
        counts.put("r2", 0);
        List<ReplicaInfo> replicas = new ArrayList<>();
        replicas.add(info("r8", 8_000_000));
        replicas.add(info("r6", 6_000_000));
        replicas.add(info("r4", 4_000_000));
        replicas.add(info("r2", 2_000_000));
        for (int i = 0; i < 10_000; i++) {
            List<String> chosen = Placement.choose("dist-" + i, 1024, 1,
                    replicas, Set.of(), alfa);
            if (!chosen.isEmpty()) counts.compute(chosen.get(0),
                    (k, v) -> v == null ? 1 : v + 1);
        }
        int total = counts.values().stream().mapToInt(v -> v).sum();
        return new PlacementResults(counts, total);
    }

    private static ReplicaInfo info(String id, long quota) {
        ReplicaInfo replica = new ReplicaInfo(id);
        replica.quota = quota;
        replica.usados = 0;
        replica.estado = ReplicaEstado.VIVA;
        return replica;
    }

    private record PlacementResults(Map<String, Integer> counts, int total) { }

    private static void check(boolean condition, String name) {
        if (!condition) throw new AssertionError(name);
        checks++;
    }
}
