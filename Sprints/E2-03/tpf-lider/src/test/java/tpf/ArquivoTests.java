package tpf;

import tpf.ChunkCrypto.Sealed;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;

/** Testes executáveis só com JDK, sem token Discord, JUnit ou bibliotecas de cifra externas. */
public final class ArquivoTests {
    private static int checks;
    private static final boolean SEM_ACL = Boolean.getBoolean("tpf.tests.semAcl");

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("queda")) {
            LocalArchive a = arquivo(Path.of(args[1]));
            a.registar("123", "456", "Antes da queda abrupta");
            // Sem close/shutdown hook: imita o processo terminado antes da selagem.
            Runtime.getRuntime().halt(0);
        }
        Path base = Files.createTempDirectory("tpf-e2-02-tests-");
        System.out.println("Dados isolados de teste: " + base);
        cifra();
        formato();
        if (SEM_ACL) System.out.println("NAO VALIDADO: permissoes da chave (teste separado fora do sandbox).");
        else chave(base.resolve("chave"));
        tempo(base.resolve("tempo"));
        rajada(base.resolve("rajada"));
        concorrencia(base.resolve("concorrencia"));
        queda(base.resolve("queda"));
        tpf.Sprint03Tests.main(args);
        System.out.println("OK: " + checks + " verificacoes; cifra, padding, tempo, tamanho, concorrencia e reinicio.");
    }

    private static void cifra() throws Exception {
        // RFC 5869, caso 1: confirmar Extract+Expand com um vetor independente.
        byte[] ikm = new byte[22];
        Arrays.fill(ikm, (byte) 0x0b);
        byte[] hkdf = ChunkCrypto.hkdf(ikm, HexFormat.of().parseHex("000102030405060708090a0b0c"),
                HexFormat.of().parseHex("f0f1f2f3f4f5f6f7f8f9"));
        check(HexFormat.of().formatHex(hkdf).equals("3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf"), "HKDF RFC5869");
        byte[] mk = new byte[32];
        new SecureRandom().nextBytes(mk);
        ChunkCrypto c = new ChunkCrypto(mk);
        byte[] claro = "Conteúdo confidencial çã 🚀".getBytes(StandardCharsets.UTF_8);
        Sealed s = c.seal(claro);
        check(Arrays.equals(c.open(s), claro), "Seal/open");
        check(!s.id.equals(c.seal(claro).id), "Aleatoriedade por chunk");
        byte[] alterado = s.ciphertext.clone();
        alterado[0] ^= 1;
        falhaCrypto(() -> c.open(new Sealed(s.id, 1, s.salt, s.nonce, alterado)), "Hash adulterado");
        falhaCrypto(() -> c.open(new Sealed(ChunkCrypto.sha256Hex(alterado), 1, s.salt, s.nonce, alterado)), "GCM mesmo com hash recalculado");
        byte[] salt = s.salt.clone(); salt[0] ^= 1;
        falhaCrypto(() -> c.open(new Sealed(s.id, 1, salt, s.nonce, s.ciphertext)), "Salt adulterado");
        byte[] nonce = s.nonce.clone(); nonce[0] ^= 1;
        falhaCrypto(() -> c.open(new Sealed(s.id, 1, s.salt, nonce, s.ciphertext)), "Nonce adulterado");
        falhaCrypto(() -> c.open(new Sealed(s.id, 2, s.salt, s.nonce, s.ciphertext)), "Versao adulterada");
        byte[] outra = mk.clone(); outra[0] ^= 1;
        falhaCrypto(() -> new ChunkCrypto(outra).open(s), "Outra master key");
        // Hash tem precedência sobre a versão, conforme a ordem pedida no enunciado.
        try { c.open(new Sealed("0".repeat(64), 2, s.salt, s.nonce, s.ciphertext)); throw new AssertionError(); }
        catch (GeneralSecurityException e) { check(e.getMessage().contains("Hash"), "Ordem de validacao"); }
    }

    private static void formato() throws Exception {
        Registo r = new Registo(1, 123456, "123", "456", "Olá; mundo\nsegunda linha 🚀");
        check(r.equals(Registo.lerLinha(r.linha())), "Log Base64 e UTF-8");
        Chunk chunk = new Chunk(List.of(r));
        for (int pad : new int[]{1, 16, 4096}) {
            byte[] bytes = chunk.serializar(pad);
            check(bytes.length % pad == 0, "Multiplo do padding");
            check(Chunk.ler(bytes).equals(chunk), "Leitura inversa do chunk");
        }
        byte[] bytes = chunk.serializar(4096);
        bytes[bytes.length - 1] = 1;
        falhaIo(() -> Chunk.ler(bytes), "Padding invalido");
        byte[] truncado = Arrays.copyOf(chunk.serializar(1), 12);
        falhaIo(() -> Chunk.ler(truncado), "Chunk truncado");
    }

    private static void chave(Path pasta) throws Exception {
        Files.createDirectories(pasta);
        Path path = pasta.resolve("master.key");
        byte[] primeira = MasterKey.carregar(path);
        check(primeira.length == 32, "Chave com 32 bytes");
        check(Arrays.equals(primeira, MasterKey.carregar(path)), "Chave conservada no reinicio");
        PosixFileAttributeView posix = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if (posix != null) check(posix.readAttributes().permissions().equals(Set.of(PosixFilePermission.OWNER_READ)), "Permissoes 0400");
        else {
            AclFileAttributeView acl = Files.getFileAttributeView(path, AclFileAttributeView.class);
            check(acl.getAcl().size() == 1 && acl.getAcl().get(0).principal().equals(acl.getOwner())
                    && !acl.getAcl().get(0).permissions().contains(AclEntryPermission.WRITE_DATA), "ACL so leitura do dono");
        }
        Path invalida = pasta.resolve("invalida.key");
        Files.write(invalida, new byte[31]);
        falhaIo(() -> MasterKey.carregar(invalida), "Recusar chave com 31 bytes");
    }

    private static void tempo(Path pasta) throws Exception {
        try (LocalArchive a = arquivo(pasta)) {
            long inicio = System.nanoTime();
            // Um segundo escritor deve ser recusado sem bloquear o primeiro.
            try (LocalArchive segundo = arquivo(pasta)) { throw new AssertionError("Dois escritores no mesmo arquivo"); }
            catch (java.nio.channels.OverlappingFileLockException | IOException esperado) { checks++; }
            check(a.registar("123", "456", "Um unico registo") == 1, "Primeiro LSN");
            check(Files.readAllLines(pasta.resolve("registos.log")).size() == 1, "Log presente antes de devolver LSN");
            // Esperar pela publicação do par completo, sem depender de um sleep exato.
            long limite = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
            while (ficheiros(pasta, ".hdr").isEmpty() && System.nanoTime() < limite) Thread.sleep(20);
            long decorrido = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - inicio);
            check(ficheiros(pasta, ".hdr").size() == 1, "Selagem por tempo");
            check(decorrido >= Wire.SELAGEM_MS - 100 && decorrido < Wire.SELAGEM_MS + 6000, "Prazo desde primeiro registo");
            System.out.println("Selagem por tempo: " + decorrido + " ms (alvo " + Wire.SELAGEM_MS + ")");
        }
        verificarChunks(pasta, 1);
        try (LocalArchive a = arquivo(pasta)) {
            check(a.registar("123", "456", "Depois do reinicio") == 2, "Reinicio sem repetir LSN");
        }
        verificarChunks(pasta, 2);
        // A leitura no arranque deve detetar adulteração de um chunk persistido.
        Path bin = ficheiros(pasta, ".bin").get(0);
        byte[] adulterado = Files.readAllBytes(bin);
        adulterado[0] ^= 1;
        Files.write(bin, adulterado);
        falhaCrypto(() -> { try (LocalArchive a = arquivo(pasta)) { } }, "Arranque com chunk adulterado");
    }

    private static void rajada(Path pasta) throws Exception {
        try (LocalArchive a = arquivo(pasta)) {
            for (int i = 0; i < 500; i++) a.registar("123", "456", "Rajada " + i + " " + "dados-sinteticos ".repeat(20));
            check(ficheiros(pasta, ".hdr").size() >= 2, "Rajada sela varios chunks por tamanho");
        }
        verificarChunks(pasta, 500);
        System.out.println("Rajada de 500: " + ficheiros(pasta, ".bin").size() + " chunks");
    }

    private static void concorrencia(Path pasta) throws Exception {
        try (LocalArchive a = arquivo(pasta)) {
            ExecutorService pool = Executors.newFixedThreadPool(4);
            try {
                List<Future<?>> tarefas = new ArrayList<>();
                for (int thread = 0; thread < 4; thread++) tarefas.add(pool.submit(() -> {
                    for (int i = 0; i < 100; i++) {
                        try { a.registar("123", "456", "Concorrente " + i); }
                        catch (IOException e) { throw new UncheckedIOException(e); }
                    }
                }));
                for (Future<?> f : tarefas) f.get(30, TimeUnit.SECONDS);
            } finally { pool.shutdownNow(); }
        }
        List<String> linhas = Files.readAllLines(pasta.resolve("registos.log"));
        check(linhas.size() == 400, "400 escritas concorrentes");
        for (int i = 0; i < linhas.size(); i++) check(Registo.lerLinha(linhas.get(i)).lsn() == i + 1, "LSN contiguo");
        verificarChunks(pasta, 400);
    }

    private static void queda(Path pasta) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process child = new ProcessBuilder(java, "-Dtpf.tests.semAcl=" + SEM_ACL, "-cp", System.getProperty("java.class.path"),
                ArquivoTests.class.getName(), "queda", pasta.toString()).inheritIO().start();
        check(child.waitFor(20, TimeUnit.SECONDS) && child.exitValue() == 0, "Queda abrupta do processo filho");
        check(ficheiros(pasta, ".bin").isEmpty(), "Queda ocorreu antes de selar");
        Files.writeString(pasta.resolve("registos.log"), "2;cauda-parcial", StandardOpenOption.APPEND);
        try (LocalArchive a = arquivo(pasta)) {
            check(a.registar("123", "456", "Depois da queda") == 2, "Recuperar prefixo e continuar LSN");
        }
        verificarChunks(pasta, 2);
    }

    private static LocalArchive arquivo(Path pasta) throws Exception {
        if (!SEM_ACL) return new LocalArchive(pasta);
        // Apenas o runner de testes usa esta chave de fixture e o construtor de pacote.
        // LeaderBot usa sempre o construtor público com proteção obrigatória da chave.
        Files.createDirectories(pasta);
        Path key = pasta.resolve("master.key");
        if (Files.notExists(key)) {
            byte[] mk = new byte[32];
            new SecureRandom().nextBytes(mk);
            Files.write(key, mk, StandardOpenOption.CREATE_NEW);
        }
        return new LocalArchive(pasta, new ChunkCrypto(Files.readAllBytes(key)));
    }

    private static List<Path> ficheiros(Path pasta, String extensao) throws IOException {
        try (var paths = Files.list(pasta.resolve("selados"))) {
            return paths.filter(p -> p.toString().endsWith(extensao)).toList();
        }
    }

    private static void verificarChunks(Path pasta, int total) throws Exception {
        ChunkCrypto crypto = new ChunkCrypto(Files.readAllBytes(pasta.resolve("master.key")));
        TreeMap<Long, Registo> recuperados = new TreeMap<>();
        for (Path bin : ficheiros(pasta, ".bin")) {
            String id = bin.getFileName().toString().replaceFirst("\\.bin$", "");
            byte[] ciphertext = Files.readAllBytes(bin);
            check(ChunkCrypto.sha256Hex(ciphertext).equals(id), "Nome SHA-256");
            // A tag GCM acrescenta 16 bytes ao plaintext que recebeu padding.
            check((ciphertext.length - 16) % Wire.PAD_BYTES == 0, "Padding antes da tag GCM");
            Chunk chunk = LocalArchive.abrir(pasta, id, crypto);
            for (Registo r : chunk.registos()) check(recuperados.put(r.lsn(), r) == null, "LSN unico nos chunks");
            check(!new String(ciphertext, StandardCharsets.ISO_8859_1).contains("dados-sinteticos"), "Sem texto da rajada em claro");
        }
        check(recuperados.size() == total, "Todos os registos recuperados");
        for (String linha : Files.readAllLines(pasta.resolve("registos.log"))) {
            Registo r = Registo.lerLinha(linha);
            check(r.equals(recuperados.get(r.lsn())), "Chunk igual ao log");
        }
        List<String> eventos = Files.readAllLines(pasta.resolve("eventos-lider.txt"));
        check(eventos.stream().anyMatch(s -> s.contains(";REGISTO;")), "Evento REGISTO");
        check(eventos.stream().anyMatch(s -> s.contains(";SELADO;")), "Evento SELADO");
    }

    private static void check(boolean condicao, String nome) {
        if (!condicao) throw new AssertionError(nome);
        checks++;
    }
    @FunctionalInterface private interface Acao { void run() throws Exception; }
    private static void falhaCrypto(Acao acao, String nome) throws Exception {
        try { acao.run(); throw new AssertionError(nome); }
        catch (GeneralSecurityException e) { checks++; }
    }
    private static void falhaIo(Acao acao, String nome) throws Exception {
        try { acao.run(); throw new AssertionError(nome); }
        catch (IOException e) { checks++; }
    }
}
