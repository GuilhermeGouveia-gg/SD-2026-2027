package tpf;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.*;
import java.security.spec.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public final class ReplicaService implements AutoCloseable {
    private final String id;
    private final URI uri;
    private final long quota;
    private final String token;
    private final Path pasta;
    private final Path eventos;
    private final KeyPair chaves;
    private final ScheduledExecutorService agenda = Executors.newSingleThreadScheduledExecutor();
    private volatile long usados;
    private volatile WebSocket ligacao;
    private volatile boolean fechar;
    private int backoffMs = 1000;

    private ReplicaService(String id, URI uri, long quota, String token) throws Exception {
        this.id = id;
        this.uri = uri;
        this.quota = quota;
        this.token = token;
        this.pasta = Path.of("dados-" + id);
        this.eventos = Path.of("eventos-" + id + ".txt");
        Files.createDirectories(pasta);
        this.chaves = carregarOuCriarChaves();
        this.usados = Files.list(pasta)
                .filter(p -> p.getFileName().toString().endsWith(".bin"))
                .mapToLong(p -> {
                    try { return Files.size(p); } catch (Exception e) { return 0L; }
                }).sum();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("Uso: ReplicaService <id> <url> <quotaBytes> [token]");
        }
        try (ReplicaService replica = new ReplicaService(args[0], URI.create(args[1]),
                Long.parseLong(args[2]), args.length == 4 ? args[3] : null)) {
            replica.executar();
            CountDownLatch esperar = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> replica.fechar = true));
            esperar.await();
        }
    }

    private void executar() {
        agenda.execute(this::ligar);
    }

    private void ligar() {
        if (fechar) return;
        HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(uri, new WebSocket.Listener() {
            @Override
            public void onOpen(WebSocket socket) {
                ligacao = socket;
                backoffMs = 1000;
                if (token == null) socket.sendText("OLA;" + id, true);
                else {
                    String publica = Base64.getEncoder().encodeToString(chaves.getPublic().getEncoded());
                    socket.sendText("OLA;" + id + ";" + publica + ";" + token, true);
                }
                agenda.scheduleAtFixedRate(() -> publicar(
                        "HB;" + id + ";" + usados + ";" + quota),
                        2000, 2000, TimeUnit.MILLISECONDS);
                socket.request(1);
            }

            @Override
            public CompletionStage<?> onText(WebSocket socket, CharSequence dados, boolean ultimo) {
                processar(socket, dados.toString());
                socket.request(1);
                return null;
            }

            @Override
            public CompletionStage<?> onClose(WebSocket socket, int statusCode, String reason) {
                ligacao = null;
                evento("DESLIGADO " + statusCode + " " + reason);
                religar();
                return null;
            }

            @Override
            public void onError(WebSocket socket, Throwable error) {
                ligacao = null;
                evento("ERRO " + error.getMessage());
                religar();
            }
        });
    }

    private void processar(WebSocket socket, String mensagem) {
        String[] campos = mensagem.split(";", -1);
        try {
            if ("DESAFIO".equals(campos[0]) && campos.length == 2) {
                Signature signature = Signature.getInstance("Ed25519");
                signature.initSign(chaves.getPrivate());
                signature.update(("tpf-registo|" + id + "|" + campos[1]).getBytes(StandardCharsets.UTF_8));
                String prova = Base64.getEncoder().encodeToString(signature.sign());
                socket.sendText("PROVA;" + id + ";" + prova, true);
                socket.sendText("SUB;tpf-r-" + id, true);
                socket.sendText("SUB;tpf-ctl", true);
                String ids = String.join(",", Files.list(pasta)
                        .map(p -> p.getFileName().toString())
                        .filter(n -> n.endsWith(".bin"))
                        .map(n -> n.substring(0, n.length() - 4))
                        .sorted()
                        .toList());
                socket.sendText("PUB;tpf-lider;INVENTARIO;" + id + ";" + ids, true);
                evento("LIGADO");
            } else if ("DESLIGADO".equals(campos[0])) {
                socket.sendClose(1000, "");
            } else if ("STORE".equals(campos[0]) && campos.length == 4) {
                receberStore(socket, campos);
            }
        } catch (GeneralSecurityException | IllegalArgumentException | IOException e) {
            evento("ERRO " + e.getMessage());
        }
    }

    private void receberStore(WebSocket socket, String[] campos) {
        String chunkId = campos[1];
        try {
            byte[] hdr = Base64.getDecoder().decode(campos[2]);
            byte[] bin = Base64.getDecoder().decode(campos[3]);
            String tamanho = String.valueOf(bin.length);
            Path binDestino = pasta.resolve(chunkId + ".bin");
            Path hdrDestino = pasta.resolve(chunkId + ".hdr");

            if (Files.exists(binDestino) && Files.exists(hdrDestino)) {
                String existente = sha256(Files.readAllBytes(binDestino));
                if (chunkId.equals(existente)) {
                    publicarAck(socket, chunkId);
                    evento("STORE " + chunkId + " " + tamanho + " EXISTENTE");
                    return;
                }
            }
            if (usados + bin.length > quota) {
                evento("QUOTA " + chunkId + " " + tamanho);
                return;
            }
            String hash = sha256(bin);
            if (!chunkId.equals(hash)) {
                evento("ERRO hash invalido " + chunkId);
                return;
            }
            gravarAtomico(binDestino, bin);
            gravarAtomico(hdrDestino, hdr);
            usados += bin.length;
            evento("STORE " + chunkId + " " + tamanho);
            publicarAck(socket, chunkId);
        } catch (Exception e) {
            evento("ERRO " + chunkId + " " + e.getMessage());
        }
    }

    private void publicarAck(WebSocket socket, String chunkId) {
        socket.sendText("PUB;tpf-lider;ACK;" + id + ";" + chunkId + ";" + chunkId, true);
    }

    private void publicar(String mensagem) {
        WebSocket socket = ligacao;
        if (socket != null && socket.isOutputClosed() == false) {
            socket.sendText("PUB;tpf-lider;" + mensagem, true);
        }
    }

    private void religar() {
        if (fechar) return;
        evento("RELIGAR " + backoffMs);
        agenda.schedule(this::ligar, backoffMs, TimeUnit.MILLISECONDS);
        backoffMs = Math.min(backoffMs * 2, 30000);
    }

    private void evento(String detalhe) {
        try {
            Files.writeString(eventos,
                    Instant.now().toEpochMilli() + ";" + detalhe + "\n",
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("ERRO evento: " + e.getMessage());
        }
    }

    private KeyPair carregarOuCriarChaves() throws Exception {
        Path privada = pasta.resolve("privada.pk8");
        Path publica = pasta.resolve("publica.x509");
        if (Files.exists(privada) && Files.exists(publica)) {
            PrivateKey privateKey = KeyFactory.getInstance("Ed25519")
                    .generatePrivate(new PKCS8EncodedKeySpec(Files.readAllBytes(privada)));
            PublicKey publicKey = KeyFactory.getInstance("Ed25519")
                    .generatePublic(new X509EncodedKeySpec(Files.readAllBytes(publica)));
            return new KeyPair(publicKey, privateKey);
        }
        KeyPairGenerator gerador = KeyPairGenerator.getInstance("Ed25519");
        KeyPair par = gerador.generateKeyPair();
        gravarChave(privada, par.getPrivate().getEncoded(), true);
        gravarChave(publica, par.getPublic().getEncoded(), false);
        return par;
    }

    private void gravarChave(Path destino, byte[] dados, boolean privada) throws IOException {
        Files.write(destino, dados, StandardOpenOption.CREATE_NEW, StandardOpenOption.TRUNCATE_EXISTING);
        if (System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("nix")
                || System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("nux")) {
            Set<PosixFilePermission> permissoes = privada
                    ? PosixFilePermissions.fromString("rw-------")
                    : PosixFilePermissions.fromString("rw-r--r--");
            Files.setPosixFilePermissions(destino, permissoes);
        }
    }

    private void gravarAtomico(Path destino, byte[] dados) throws IOException {
        Path temporario = destino.resolveSibling(destino.getFileName() + ".tmp");
        Files.write(temporario, dados,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        try (FileChannel canal = FileChannel.open(temporario, StandardOpenOption.WRITE)) {
            canal.force(true);
        }
        try {
            Files.move(temporario, destino,
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("Move atomico nao suportado", e);
        }
    }

    private static String sha256(byte[] dados) throws GeneralSecurityException {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(dados));
    }

    @Override
    public void close() {
        fechar = true;
        agenda.shutdownNow();
        WebSocket socket = ligacao;
        if (socket != null) socket.sendClose(1001, "encerrada");
    }
}
