package tpf;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;
import net.dv8tion.jda.api.*;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.entities.channel.concrete.TextChannel;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.*;
import net.dv8tion.jda.api.requests.GatewayIntent;

/** Entrada Discord. As duas entradas reais e a rajada partilham registar(). */
public final class LeaderBot extends ListenerAdapter {
    private static final String ADMIN_ROLE = "admin-arquivo";
    private final String guildId;
    private final LocalArchive arquivo;
    private final PubSubHub hub;
    private final ReplicaRegistry replicas;
    private final Catalog catalogo;
    private final ScheduledExecutorService rajadas = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService monitores = Executors.newSingleThreadScheduledExecutor();
    private boolean rajadaAtiva;

    private LeaderBot(String guildId, LocalArchive arquivo, PubSubHub hub,
                      ReplicaRegistry replicas, Catalog catalogo) {
        this.guildId = guildId;
        this.arquivo = arquivo;
        this.hub = hub;
        this.replicas = replicas;
        this.catalogo = catalogo;
    }

    private static void processarMensagemReplica(String replicaId, String mensagem,
                                                   ReplicaRegistry replicas,
                                                   Catalog catalogo,
                                                   ReplicationCoordinator coordinator) {
        String[] c = mensagem.split(";", -1);
        if (c.length == 4 && "HB".equals(c[0])) {
            replicas.onHeartbeat(replicaId, Long.parseLong(c[2]),
                    Long.parseLong(c[3]), System.currentTimeMillis());
            return;
        }
        if (c.length == 3 && "INVENTARIO".equals(c[0])) {
            System.out.println("INVENTARIO " + replicaId + " " + c[2]);
            return;
        }
        if (c.length == 4 && "ACK".equals(c[0])) {
            String idMensagem = c[1];
            String chunkId = c[2];
            String sha = c[3];
            if (!replicaId.equals(idMensagem)) {
                System.out.println("ACK-REJEITADO " + chunkId + " " + replicaId + " ID");
                return;
            }
            if (!chunkId.equals(sha)) {
                System.out.println("ACK-REJEITADO " + chunkId + " " + replicaId + " HASH");
                return;
            }
            if (coordinator == null || !coordinator.receberAck(replicaId, chunkId, sha)) {
                System.out.println("ACK-REJEITADO " + chunkId + " " + replicaId + " SEM_STORE");
                return;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String token = obrigatoria("DISCORD_BOT_TOKEN");
        String guildId = obrigatoria("DISCORD_GUILD_ID");
        Path pastaDados = Path.of(System.getProperty("tpf.dados", "."));
        ReplicaRegistry replicas = new ReplicaRegistry();
        Catalog catalogo = new Catalog(pastaDados.resolve("catalogo.txt"));
        ReplicationCoordinator[] coordinador = new ReplicationCoordinator[1];
        PubSubHub hub = new PubSubHub(
                new InetSocketAddress(Wire.PORTO),
                pastaDados,
                (replicaId, mensagem) -> processarMensagemReplica(
                        replicaId, mensagem, replicas, catalogo, coordinador[0]));
        hub.start();
        ReplicationCoordinator coordinator = new ReplicationCoordinator(
                pastaDados, hub, replicas, catalogo);
        coordinador[0] = coordinator;
        LocalArchive arquivo = new LocalArchive(pastaDados);
        arquivo.definirListener(coordinator::selado);
        LeaderBot bot = new LeaderBot(guildId, arquivo, hub, replicas, catalogo);
        bot.monitores.scheduleAtFixedRate(() ->
                replicas.atualizarEstados(System.currentTimeMillis()),
                2000, 2000, TimeUnit.MILLISECONDS);
        JDA jda = null;
        try {
            jda = JDABuilder.createDefault(token)
                    .enableIntents(GatewayIntent.MESSAGE_CONTENT, GatewayIntent.GUILD_MESSAGES)
                    .addEventListeners(bot).build();
            JDA ligacao = jda;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                bot.rajadas.shutdownNow();
                bot.monitores.shutdownNow();
                coordinador[0].fechar();
                ligacao.shutdown();
                try { bot.hub.stop(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                try { arquivo.close(); } catch (IOException e) { System.err.println(e.getMessage()); }
            }, "fechar-arquivo"));
            jda.awaitReady();
            var guild = jda.getGuildById(guildId);
            if (guild == null) throw new IllegalStateException("Bot sem acesso à guild configurada");
            guild.updateCommands().addCommands(comandos()).complete();
            System.out.println("Bot online; comandos registados. Pasta: "
                    + Path.of(System.getProperty("tpf.dados", ".")).toAbsolutePath());
        } catch (Exception e) {
            bot.rajadas.shutdownNow();
            bot.monitores.shutdownNow();
            coordinador[0].fechar();
            if (jda != null) jda.shutdownNow();
            try { hub.stop(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            arquivo.close();
            throw e;
        }
    }

    private static String obrigatoria(String nome) {
        String valor = System.getenv(nome);
        if (valor == null || valor.isBlank()) throw new IllegalStateException("Falta a variável " + nome);
        return valor;
    }

    private static List<CommandData> comandos() {
        return List.of(
                Commands.slash("registar", "Regista um texto no log local")
                        .addOption(OptionType.STRING, "texto", "Texto a registar", true),
                Commands.slash("estado", "Estado das réplicas e do catálogo"),
                Commands.slash("config", "Configuração (provisório)")
                        .addSubcommands(new SubcommandData("r", "Fator de replicação")
                                .addOption(OptionType.INTEGER, "n", "Valor de r", true)),
                Commands.slash("recuperar", "Recuperação (provisório)")
                        .addOption(OptionType.INTEGER, "de", "LSN inicial", true)
                        .addOption(OptionType.INTEGER, "ate", "LSN final", true),
                Commands.slash("replica-token", "Gera um token de réplica"),
                Commands.slash("replica-remover", "Remove uma réplica")
                        .addOption(OptionType.STRING, "id", "ID da réplica", true),
                Commands.slash("rajada", "Injeta registos sintéticos no log local")
                        .addOption(OptionType.INTEGER, "n", "Número de registos", true)
                        .addOption(OptionType.INTEGER, "intervaloms", "Intervalo em ms", true));
    }

    private synchronized long registar(String autor, String canal, String texto) throws IOException {
        return arquivo.registar(autor, canal, texto);
    }

    private static boolean admin(Member membro) {
        return membro != null && membro.getRoles().stream().anyMatch(r -> r.getName().equalsIgnoreCase(ADMIN_ROLE));
    }

    @Override public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        if (!event.isFromGuild() || !guildId.equals(event.getGuild().getId())) {
            event.reply("Comando disponível apenas no servidor configurado.").setEphemeral(true).queue();
            return;
        }
        String nome = event.getName();
        if (List.of("config", "replica-token", "replica-remover", "rajada").contains(nome) && !admin(event.getMember())) {
            event.reply("Acesso negado: requer o papel `" + ADMIN_ROLE + "`.").setEphemeral(true).queue();
            return;
        }
        try {
            switch (nome) {
                case "registar" -> {
                    String texto = event.getOption("texto").getAsString();
                    if (texto.isBlank()) throw new IllegalArgumentException("O texto não pode estar vazio");
                    // Reconhecer a interação antes do I/O, sem confirmar ainda a escrita.
                    event.deferReply().queue(hook -> {
                        try {
                            long lsn = registar(event.getUser().getId(), event.getChannel().getId(), texto);
                            hook.editOriginal("Registo gravado localmente. LSN=" + lsn).queue();
                        } catch (IOException e) {
                            System.err.println("ERRO no registo: " + e.getMessage());
                            hook.editOriginal("Não foi possível confirmar a escrita. Verifica o log antes de repetir.").queue();
                        }
                    });
                }
                case "rajada" -> iniciarRajada(event);
                case "recuperar" -> {
                    long de = event.getOption("de").getAsLong(), ate = event.getOption("ate").getAsLong();
                    if (de < 1 || de > ate) throw new IllegalArgumentException("Intervalo inválido: exige 1 <= de <= ate");
                    event.reply("`/recuperar` recebido: de=" + de + ", ate=" + ate + " — ainda não implementado").queue();
                }
                case "config" -> {
                    long n = event.getOption("n").getAsLong();
                    if (n < 1) throw new IllegalArgumentException("r deve ser positivo");
                    event.reply("`/config r` recebido: n=" + n + " — ainda não implementado").queue();
                }
                case "replica-remover" -> {
                    String id = event.getOption("id").getAsString();
                    hub.removerReplica(id);
                    replicas.remover(id);
                    event.reply("Réplica `" + id + "` removida.").setEphemeral(true).queue();
                }
                case "replica-token" -> event.reply("Token de réplica: `" + hub.gerarToken() + "`")
                        .setEphemeral(true).queue();
                case "estado" -> {
                    replicas.atualizarEstados(System.currentTimeMillis());
                    StringBuilder resposta = new StringBuilder();
                    for (ReplicaInfo replica : replicas.todas()) {
                        resposta.append(replica.id).append(" ")
                                .append(replica.estado).append(" ")
                                .append(replica.usados).append("/").append(replica.quota)
                                .append(" chunks=");
                        resposta.append(catalogo.copiasReplicas(replica.id)).append("\n");
                    }
                    long selados = catalogo.linhasEstado().stream()
                            .filter(l -> l.contains(";SELADO;")).count();
                    long duraveis = catalogo.linhasEstado().stream()
                            .filter(l -> l.contains(";DURAVEL;")).count();
                    long sub = catalogo.linhasEstado().stream()
                            .filter(l -> {
                                String[] c = l.split(";");
                                return c.length >= 3 && Integer.parseInt(c[2].split("/")[0])
                                        < Wire.R;
                            }).count();
                    resposta.append("TOTAL SELADO=").append(selados)
                            .append(" DURAVEL=").append(duraveis)
                            .append(" subreplicados=").append(sub);
                    event.reply(resposta.toString()).queue();
                }
                default -> event.reply("Comando desconhecido").setEphemeral(true).queue();
            }
        } catch (IllegalArgumentException | NullPointerException e) {
            event.reply("Argumentos inválidos: " + e.getMessage()).setEphemeral(true).queue();
        }
    }

    private synchronized void iniciarRajada(SlashCommandInteractionEvent event) {
        long n = event.getOption("n").getAsLong(), intervalo = event.getOption("intervaloms").getAsLong();
        if (n < 1 || n > 100000 || intervalo < 0 || intervalo > 60000)
            throw new IllegalArgumentException("Usa 1..100000 registos e 0..60000 ms");
        if (rajadaAtiva) {
            event.reply("Já existe uma rajada em curso.").setEphemeral(true).queue();
            return;
        }
        rajadaAtiva = true;
        event.reply("Rajada iniciada: " + n + " registos, intervalo " + intervalo + " ms.").queue(hook -> {
            // Reagendar cada registo; não bloquear callbacks do Discord com sleeps.
            Runnable tarefa = new Runnable() {
                long feitos, primeiro, ultimo;
                @Override public void run() {
                    try {
                        String texto = "Rajada " + (feitos + 1) + "/" + n + " | " + "dados-sinteticos ".repeat(20);
                        ultimo = registar(event.getUser().getId(), event.getChannel().getId(), texto);
                        if (feitos++ == 0) primeiro = ultimo;
                        if (feitos < n) rajadas.schedule(this, intervalo, TimeUnit.MILLISECONDS);
                        else {
                            terminarRajada();
                            hook.editOriginal("Rajada concluída: " + feitos + " registos; LSN " + primeiro + ".." + ultimo).queue();
                        }
                    } catch (IOException | RuntimeException e) {
                        terminarRajada();
                        System.err.println("Rajada interrompida: " + e.getMessage());
                        hook.editOriginal("Rajada interrompida após " + feitos + " confirmações. Verifica o log.").queue();
                    }
                }
            };
            rajadas.execute(tarefa);
        }, erro -> terminarRajada());
    }

    private synchronized void terminarRajada() { rajadaAtiva = false; }

    @Override public void onMessageReceived(MessageReceivedEvent event) {
        // Ignorar bots evita que as respostas do próprio bot sejam registadas de novo.
        if (event.getAuthor().isBot() || event.isWebhookMessage() || !event.isFromGuild()
                || !guildId.equals(event.getGuild().getId())
                || !(event.getChannel() instanceof TextChannel canal)
                || !canal.getName().equalsIgnoreCase("arquivo")) return;
        try {
            long lsn = registar(event.getAuthor().getId(), canal.getId(), event.getMessage().getContentRaw());
            canal.sendMessage("Mensagem gravada localmente. LSN=" + lsn).queue();
        } catch (IOException e) {
            System.err.println("ERRO no registo do canal: " + e.getMessage());
            canal.sendMessage("Não foi possível confirmar a escrita. Verifica o log antes de repetir.").queue();
        }
    }
}
