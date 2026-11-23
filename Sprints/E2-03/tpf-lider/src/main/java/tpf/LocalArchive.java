package tpf;

import tpf.ChunkCrypto.Sealed;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.concurrent.*;

/** Caminho de escrita partilhado pelo Discord, rajada e temporizador. */
public final class LocalArchive implements AutoCloseable {
    public interface ChunkSeladoListener {
        void chunkSelado(String chunkId, long lsnDe, long lsnAte, long tamanho);
    }

    private final Path pasta;
    private final ChunkCrypto crypto;
    private final FileChannel log;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private final List<Registo> aberto = new ArrayList<>();
    private ScheduledFuture<?> prazo;
    private int bytesAbertos = 8;
    private long geracaoChunk;
    private long ultimoLsn;
    private IOException falha;
    private boolean fechado;
    private volatile ChunkSeladoListener listener;

    public LocalArchive(Path pasta) throws IOException, GeneralSecurityException {
        this(pasta, null);
    }

    public void definirListener(ChunkSeladoListener listener) {
        this.listener = listener;
    }

    /** Injeção da cifra para testes de persistência sem alterar permissões do SO. */
    LocalArchive(Path pasta, ChunkCrypto cifraDeTeste) throws IOException, GeneralSecurityException {
        this.pasta = pasta.toAbsolutePath();
        Files.createDirectories(this.pasta);
        Path logPath = this.pasta.resolve("registos.log");
        // Nunca criar outra chave para dados que já foram cifrados.
        if (Files.notExists(this.pasta.resolve("master.key"))
                && ((Files.exists(logPath) && Files.size(logPath) > 0) || temChunks()))
            throw new IOException("Existem dados mas falta master.key; repõe a chave original");
        crypto = cifraDeTeste != null ? cifraDeTeste
                : new ChunkCrypto(MasterKey.carregar(this.pasta.resolve("master.key")));
        Files.createDirectories(this.pasta.resolve("selados"));
        log = FileChannel.open(logPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        // Separar o lock: no Windows um lock no próprio log impede leituras de inspeção.
        lockChannel = FileChannel.open(this.pasta.resolve("arquivo.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock adquirido;
        try {
            adquirido = lockChannel.tryLock();
            if (adquirido == null) throw new IOException("Outro líder está a usar este log");
        } catch (IOException | RuntimeException e) {
            log.close();
            lockChannel.close();
            timer.shutdownNow();
            throw e;
        }
        lock = adquirido;
        try {
            List<Registo> registos = lerLog();
            // O log permite recuperar o chunk que estava na memória numa queda.
            Set<Long> selados = lerSelados(registos);
            synchronized (this) {
                for (Registo r : registos) if (!selados.contains(r.lsn())) juntar(r);
            }
        } catch (IOException | GeneralSecurityException | RuntimeException e) {
            timer.shutdownNow();
            lock.release();
            lockChannel.close();
            log.close();
            throw e;
        }
    }

    private boolean temChunks() throws IOException {
        if (Files.notExists(pasta.resolve("selados"))) return false;
        try (var caminhos = Files.list(pasta.resolve("selados"))) {
            return caminhos.anyMatch(p -> p.toString().endsWith(".bin") || p.toString().endsWith(".hdr"));
        }
    }

    /** O mesmo monitor protege o LSN, o append e a mudança de chunk. */
    public synchronized long registar(String autor, String canal, String texto) throws IOException {
        if (fechado) throw new IOException("Arquivo fechado");
        if (falha != null) throw new IOException("Arquivo suspenso após erro; reinicia depois de o corrigir", falha);
        Registo r = new Registo(Math.addExact(ultimoLsn, 1), System.currentTimeMillis(), autor, canal, texto);
        try {
            log.position(log.size());
            escrever(log, (r.linha() + "\n").getBytes(StandardCharsets.UTF_8));
            log.force(true); // A resposta com LSN só pode acontecer depois desta barreira.
            ultimoLsn = r.lsn();
            evento("REGISTO", "lsn=" + r.lsn());
            juntar(r);
            return r.lsn();
        } catch (IOException e) {
            // Não reutilizar um LSN quando não sabemos quanto chegou ao disco.
            falha = e;
            throw e;
        }
    }

    private void juntar(Registo r) throws IOException {
        bytesAbertos = Math.addExact(bytesAbertos, Chunk.tamanhoRegisto(r));
        aberto.add(r);
        if (aberto.size() == 1) {
            // Um prazo desde o primeiro registo, sem o adiar a cada append.
            long geracao = ++geracaoChunk;
            prazo = timer.schedule(() -> selarPorTempo(geracao), Wire.SELAGEM_MS, TimeUnit.MILLISECONDS);
        }
        if (bytesAbertos >= Wire.CHUNK_MAX_BYTES) selar();
    }

    private synchronized void selarPorTempo(long geracao) {
        // Um timer antigo já em execução pode ter ficado à espera do monitor.
        // Não pode selar antecipadamente um chunk novo entretanto aberto.
        if (fechado || falha != null || geracao != geracaoChunk) return;
        try { selar(); }
        catch (IOException e) {
            falha = e;
            System.err.println("ERRO na selagem; registos mantidos no log: " + e.getMessage());
        }
    }

    private void selar() throws IOException {
        if (aberto.isEmpty()) return;
        try {
            byte[] plaintext = new Chunk(aberto).serializar(Wire.PAD_BYTES);
            Sealed s = crypto.seal(plaintext);
            Path destino = pasta.resolve("selados");
            gravarAtomico(destino.resolve(s.id + ".bin"), s.ciphertext);
            String hdr = s.keyVersion + ";" + Base64.getEncoder().encodeToString(s.salt)
                    + ";" + Base64.getEncoder().encodeToString(s.nonce) + "\n";
            gravarAtomico(destino.resolve(s.id + ".hdr"), hdr.getBytes(StandardCharsets.UTF_8));
            evento("SELADO", "chunkId=" + s.id + ";lsnDe=" + aberto.get(0).lsn()
                    + ";lsnAte=" + aberto.get(aberto.size() - 1).lsn()
                    + ";tamanho=" + s.ciphertext.length + ";paddedBytes=" + plaintext.length);
            if (listener != null) {
                    listener.chunkSelado(s.id, aberto.get(0).lsn(),
                            aberto.get(aberto.size() - 1).lsn(), s.ciphertext.length);
            }
            aberto.clear();
            bytesAbertos = 8;
            if (prazo != null) prazo.cancel(false);
            prazo = null;
        } catch (GeneralSecurityException e) { throw new IOException("Falha na cifra do chunk", e); }
    }

    private List<Registo> lerLog() throws IOException {
        byte[] bytes = Files.readAllBytes(pasta.resolve("registos.log"));
        int fim = bytes.length;
        // Uma escrita interrompida pode deixar uma última linha incompleta.
        while (fim > 0 && bytes[fim - 1] != '\n') fim--;
        if (fim != bytes.length) {
            log.truncate(fim);
            log.force(true);
            evento("ERRO", "Cauda incompleta removida;bytes=" + (bytes.length - fim));
        }
        List<Registo> registos = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new StringReader(new String(bytes, 0, fim, StandardCharsets.UTF_8)))) {
            String linha;
            while ((linha = reader.readLine()) != null) {
                Registo r = Registo.lerLinha(linha);
                if (r.lsn() != ultimoLsn + 1) throw new IOException("LSN fora de sequência no log");
                ultimoLsn = r.lsn();
                registos.add(r);
            }
        } catch (IllegalArgumentException e) { throw new IOException("Log inválido; não foi substituído", e); }
        return registos;
    }

    private Set<Long> lerSelados(List<Registo> logRegistos) throws IOException, GeneralSecurityException {
        Map<Long, Registo> referencia = new HashMap<>();
        for (Registo r : logRegistos) referencia.put(r.lsn(), r);
        Set<Long> ids = new HashSet<>();
        try (var paths = Files.list(pasta.resolve("selados"))) {
            for (Path bin : paths.filter(p -> p.toString().endsWith(".bin")).toList()) {
                String id = bin.getFileName().toString().replaceFirst("\\.bin$", "");
                if (Files.notExists(bin.resolveSibling(id + ".hdr"))) {
                    // Queda entre os renames: não contar este par como selado.
                    evento("ERRO", "Chunk incompleto ignorado;chunkId=" + id);
                    continue;
                }
                for (Registo r : abrir(pasta, id, crypto).registos()) {
                    if (!r.equals(referencia.get(r.lsn()))) throw new IOException("Chunk difere do log;LSN=" + r.lsn());
                    if (!ids.add(r.lsn())) throw new IOException("LSN repetido nos chunks: " + r.lsn());
                }
            }
        }
        return ids;
    }

    /** Inspeção local; /recuperar continua provisório neste sprint. */
    public static Chunk abrir(Path pasta, String id, ChunkCrypto crypto) throws IOException, GeneralSecurityException {
        if (!id.matches("[0-9a-f]{64}")) throw new IOException("Identificador inválido");
        Path dir = pasta.resolve("selados");
        String[] campos = Files.readString(dir.resolve(id + ".hdr")).strip().split(";", -1);
        if (campos.length != 3) throw new IOException("Cabeçalho inválido");
        try {
            Sealed s = new Sealed(id, Integer.parseInt(campos[0]), Base64.getDecoder().decode(campos[1]),
                    Base64.getDecoder().decode(campos[2]), Files.readAllBytes(dir.resolve(id + ".bin")));
            return Chunk.ler(crypto.open(s));
        } catch (IllegalArgumentException e) { throw new IOException("Cabeçalho inválido", e); }
    }

    private void evento(String nome, String detalhe) throws IOException {
        try (FileChannel out = FileChannel.open(pasta.resolve("eventos-lider.txt"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            escrever(out, (System.currentTimeMillis() + ";" + nome + ";" + detalhe + "\n").getBytes(StandardCharsets.UTF_8));
            out.force(true);
        }
    }

    private static void gravarAtomico(Path destino, byte[] dados) throws IOException {
        Path tmp = Files.createTempFile(destino.getParent(), "chunk-", ".tmp");
        try {
            try (FileChannel out = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                escrever(out, dados);
                out.force(true);
            }
            Files.move(tmp, destino, StandardCopyOption.ATOMIC_MOVE);
        } finally { Files.deleteIfExists(tmp); }
    }

    private static void escrever(FileChannel out, byte[] dados) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(dados);
        while (buffer.hasRemaining()) out.write(buffer);
    }

    @Override public synchronized void close() throws IOException {
        if (fechado) return;
        fechado = true;
        timer.shutdownNow();
        // Numa paragem normal, selar o restante. Numa queda, o replay parte do log.
        try { if (falha == null) selar(); }
        finally { try { lock.release(); } finally { try { lockChannel.close(); } finally { log.close(); } } }
    }
}
