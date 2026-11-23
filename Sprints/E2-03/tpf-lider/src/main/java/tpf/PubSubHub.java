package tpf;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

/**
 * Hub pub/sub local das replicas. Nao recebe a chave mestre nem dados em claro.
 */
public final class PubSubHub extends WebSocketServer {
    public interface MensagemLider {
        void receber(String replicaId, String mensagem);
    }

    enum Estado { NOVA, DESAFIADA, AUTENTICADA }

    private static final class Ligacao {
        Estado estado = Estado.NOVA;
        String id;
        PublicKey chavePublica;
        byte[] nonce;
        Token token;
        boolean novoRegisto;
        final Set<String> subscricoes = new HashSet<>();
    }

    private static final class Token {
        final Instant validoAte;
        boolean usado;
        Token(Instant validoAte) { this.validoAte = validoAte; }
    }

    private final Path pastaDados;
    private final Path ficheiroReplicas;
    private final Path ficheiroEventos;
    private final MensagemLider mensagemLider;
    private final Map<WebSocket, Ligacao> ligacoes = new ConcurrentHashMap<>();
    private final Map<String, Set<WebSocket>> subscricoes = new ConcurrentHashMap<>();
    private final Map<String, Token> tokens = new ConcurrentHashMap<>();
    private final Map<String, PublicKey> replicas = new ConcurrentHashMap<>();
    private final SecureRandom random = new SecureRandom();

    public PubSubHub(InetSocketAddress endereco, Path pastaDados, MensagemLider mensagemLider) throws IOException {
        super(endereco);
        this.pastaDados = pastaDados;
        this.ficheiroReplicas = pastaDados.resolve("replicas.txt");
        this.ficheiroEventos = pastaDados.resolve("eventos-lider.txt");
        this.mensagemLider = mensagemLider;
        Files.createDirectories(pastaDados);
        carregarReplicas();
    }

    public synchronized String gerarToken() {
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        tokens.put(token, new Token(Instant.now().plusMillis(Wire.TOKEN_VALIDADE_MS)));
        return token;
    }

    public synchronized void removerReplica(String id) {
        replicas.remove(id);
        try {
            gravarAtomico(ficheiroReplicas, linhasReplicas());
        } catch (IOException e) {
            erro(null, "REMOVER " + id + " " + e.getMessage());
            return;
        }
        for (Map.Entry<WebSocket, Ligacao> entrada : ligacoes.entrySet()) {
            Ligacao ligacao = entrada.getValue();
            if (id.equals(ligacao.id)) entrada.getKey().close();
        }
    }

    public void publish(String topico, String mensagem) {
        Set<WebSocket> destino = subscricoes.get(topico);
        if (destino == null) return;
        synchronized (destino) {
            for (WebSocket ws : destino) if (ws.isOpen()) ws.send(mensagem);
        }
    }

    @Override
    public void onOpen(WebSocket ws, ClientHandshake handshake) {
        ligacoes.put(ws, new Ligacao());
    }

    @Override
    public void onClose(WebSocket ws, int code, String reason, boolean remote) {
        Ligacao ligacao = ligacoes.remove(ws);
        if (ligacao != null) {
            for (String topico : ligacao.subscricoes) {
                Set<WebSocket> ligações = subscricoes.get(topico);
                if (ligações != null) {
                    synchronized (ligações) { ligações.remove(ws); }
                }
            }
        }
    }

    @Override
    public void onError(WebSocket ws, Exception ex) {
        erro(ws, ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage());
    }

    @Override
    public void onStart() { }

    @Override
    public void onMessage(WebSocket ws, String mensagem) {
        Ligacao ligacao = ligacoes.get(ws);
        if (ligacao == null) return;
        String[] campos = mensagem.split(";", -1);

        synchronized (ligacao) {
            if ("OLA".equals(campos[0])) {
                processarOla(ws, ligacao, campos);
                return;
            }
            if ("PROVA".equals(campos[0]) && ligacao.estado == Estado.DESAFIADA) {
                processarProva(ws, ligacao, campos);
                return;
            }
            if (ligacao.estado != Estado.AUTENTICADA) {
                rejeitar(ws, "mensagem fora de ordem");
                return;
            }
            if ("SUB".equals(campos[0])) {
                processarSub(ws, ligacao, campos);
                return;
            }
            if ("PUB".equals(campos[0])) {
                processarPub(ws, ligacao, campos);
                return;
            }
            rejeitar(ws, "mensagem desconhecida");
        }
    }

    @Override
    public void onMessage(WebSocket ws, ByteBuffer mensagem) {
        rejeitar(ws, "mensagem binaria nao permitida");
    }

    private void processarOla(WebSocket ws, Ligacao ligacao, String[] campos) {
        if (ligacao.estado != Estado.NOVA) {
            rejeitar(ws, "segundo OLA");
            return;
        }
        if (campos.length == 2) {
            ligacao.id = campos[1];
            ligacao.chavePublica = replicas.get(ligacao.id);
            if (ligacao.chavePublica == null) {
                rejeitar(ws, "replica nao registada");
                return;
            }
            desafiar(ws, ligacao);
            return;
        }
        if (campos.length != 4) {
            rejeitar(ws, "OLA invalido");
            return;
        }
        ligacao.id = campos[1];
        PublicKey registada = replicas.get(ligacao.id);
        try {
            ligacao.chavePublica = KeyFactory.getInstance("Ed25519")
                    .generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(campos[2])));
        } catch (IllegalArgumentException | GeneralSecurityException e) {
            rejeitar(ws, "chave publica invalida");
            return;
        }
        if (registada != null && !Arrays.equals(registada.getEncoded(), ligacao.chavePublica.getEncoded())) {
            rejeitar(ws, "chave diferente da registada");
            return;
        }

        if (registada != null) {
            desafiar(ws, ligacao);
            return;
        }
        Token token = tokens.get(campos[3]);
        if (token == null || token.usado || Instant.now().isAfter(token.validoAte)) {
            rejeitar(ws, "token invalido");
            return;
        }
        ligacao.token = token;
        ligacao.novoRegisto = true;
        desafiar(ws, ligacao);
    }

    private void desafiar(WebSocket ws, Ligacao ligacao) {
        ligacao.nonce = new byte[32];
        random.nextBytes(ligacao.nonce);
        ligacao.estado = Estado.DESAFIADA;
        ws.send("DESAFIO;" + Base64.getEncoder().encodeToString(ligacao.nonce));
    }

    private void processarProva(WebSocket ws, Ligacao ligacao, String[] campos) {
        if (campos.length != 3 || !ligacao.id.equals(campos[1])) {
            rejeitar(ws, "PROVA invalida");
            return;
        }
        if (!verificarProva(ligacao.chavePublica, ligacao.id,
                Base64.getEncoder().encodeToString(ligacao.nonce), campos[2])) {
            rejeitar(ws, "prova invalida");
            return;
        }

        try {
            if (ligacao.novoRegisto) {
                synchronized (tokens) {
                    if (ligacao.token == null || ligacao.token.usado
                            || Instant.now().isAfter(ligacao.token.validoAte)) {
                        rejeitar(ws, "token invalido");
                        return;
                    }
                    synchronized (replicas) {
                        PublicKey registada = replicas.get(ligacao.id);
                        if (registada != null
                                && !Arrays.equals(registada.getEncoded(), ligacao.chavePublica.getEncoded())) {
                            rejeitar(ws, "chave diferente da registada");
                            return;
                        }
                        try {
                            replicas.put(ligacao.id, ligacao.chavePublica);
                            gravarAtomico(ficheiroReplicas, linhasReplicas());
                            ligacao.token.usado = true;
                        } catch (IOException e) {
                            replicas.remove(ligacao.id);
                            throw e;
                        }
                    }
                }
            }
        } catch (IOException e) {
            rejeitar(ws, "erro ao guardar replica");
            return;
        }

        ligacao.estado = Estado.AUTENTICADA;
        ligacao.nonce = null;
    }

    boolean verificarProva(PublicKey chave, String id, String nonceBase64, String assinaturaBase64) {
        try {
            Signature assinatura = Signature.getInstance("Ed25519");
            assinatura.initVerify(chave);
            String texto = "tpf-registo|" + id + "|" + nonceBase64;
            assinatura.update(texto.getBytes(StandardCharsets.UTF_8));
            return assinatura.verify(Base64.getDecoder().decode(assinaturaBase64));
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            return false;
        }
    }

    private void processarSub(WebSocket ws, Ligacao ligacao, String[] campos) {
        if (campos.length != 2) {
            rejeitar(ws, "SUB invalido");
            return;
        }
        String topico = campos[1];
        if (!("tpf-ctl".equals(topico) || ("tpf-r-" + ligacao.id).equals(topico))) {
            rejeitar(ws, "topico proibido");
            return;
        }
        ligacao.subscricoes.add(topico);
        subscricoes.computeIfAbsent(topico, k -> Collections.synchronizedSet(new HashSet<>())).add(ws);
    }

    private void processarPub(WebSocket ws, Ligacao ligacao, String[] campos) {
        if (campos.length < 3 || !"tpf-lider".equals(campos[1])) {
            rejeitar(ws, "publicacao proibida");
            return;
        }
        String mensagem = String.join(";", Arrays.copyOfRange(campos, 2, campos.length));
        if (mensagemLider != null) mensagemLider.receber(ligacao.id, mensagem);
    }

    private void rejeitar(WebSocket ws, String motivo) {
        erro(ws, motivo);
        ligacoes.remove(ws);
        ws.close();
    }

    private void erro(WebSocket ws, String motivo) {
        try {
            String id = "";
            if (ws != null) {
                Ligacao ligacao = ligacoes.get(ws);
                if (ligacao != null && ligacao.id != null) id = " " + ligacao.id;
            }
            appendEvento("ERRO" + id + " " + motivo);
        } catch (IOException e) {
            System.err.println("Nao foi possivel registar ERRO: " + e.getMessage());
        }
    }

    private void appendEvento(String detalhe) throws IOException {
        String linha = Instant.now().toEpochMilli() + ";" + detalhe + "\n";
        Files.writeString(ficheiroEventos, linha, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private synchronized void carregarReplicas() throws IOException {
        replicas.clear();
        if (!Files.exists(ficheiroReplicas)) return;
        for (String linha : Files.readAllLines(ficheiroReplicas, StandardCharsets.UTF_8)) {
            if (linha.isBlank()) continue;
            String[] campos = linha.split(";", -1);
            if (campos.length != 2) throw new IOException("Linha invalida em replicas.txt");
            try {
                PublicKey chave = KeyFactory.getInstance("Ed25519").generatePublic(
                        new X509EncodedKeySpec(Base64.getDecoder().decode(campos[1])));
                replicas.put(campos[0], chave);
            } catch (GeneralSecurityException | IllegalArgumentException e) {
                throw new IOException("Chave invalida em replicas.txt", e);
            }
        }
    }

    private synchronized List<String> linhasReplicas() {
        List<String> linhas = new ArrayList<>();
        replicas.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> linhas.add(e.getKey() + ";" +
                        Base64.getEncoder().encodeToString(e.getValue().getEncoded())));
        return linhas;
    }

    private void gravarAtomico(Path destino, List<String> linhas) throws IOException {
        Path temporario = destino.resolveSibling(destino.getFileName() + ".tmp");
        String conteudo = linhas.isEmpty() ? "" : String.join("\n", linhas) + "\n";
        Files.writeString(temporario, conteudo, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        try (FileChannel canal = FileChannel.open(temporario, StandardOpenOption.WRITE)) {
            canal.force(true);
        }
        try {
            Files.move(temporario, destino, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("Move atomico nao suportado", e);
        }
    }
}
