package tpf;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.requests.GatewayIntent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.entities.channel.concrete.TextChannel;

public class LeaderBot extends ListenerAdapter {

    private static final String ADMIN_ROLE_NAME = "admin-arquivo";
    private static final String ARCHIVE_CHANNEL_NAME = "arquivo";

    public static void main(String[] args) {
        // Obter o token de variável de ambiente (critério de segurança)
        String token = System.getenv("DISCORD_BOT_TOKEN");
        if (token == null || token.isBlank()) {
            System.err.println("ERRO: A variável de ambiente DISCORD_BOT_TOKEN não está definida!");
            System.exit(1);
        }

        try {
            JDA jda = JDABuilder.createDefault(token)
                    .enableIntents(
                            GatewayIntent.MESSAGE_CONTENT, // Necessário para ler mensagens
                            GatewayIntent.GUILD_MESSAGES,
                            GatewayIntent.GUILD_MEMBERS
                    )
                    .addEventListeners(new LeaderBot())
                    .build();

            jda.awaitReady();
            System.out.println("Bot online com sucesso!");

            String guildId = System.getenv("DISCORD_GUILD_ID");
            if (guildId != null && !guildId.isBlank()) {
                var guild = jda.getGuildById(guildId);
                if (guild != null) {
                    guild.updateCommands().addCommands(
                            // 1. /registar
                            Commands.slash("registar", "Regista um texto")
                                    .addOption(OptionType.STRING, "texto", "Texto a registar", true),

                            // 2. /estado
                            Commands.slash("estado", "Mostra o estado atual do líder"),

                            // 3. /config r <n> (Usa SubcommandData)
                            Commands.slash("config", "Configura um parâmetro")
                                    .addSubcommands(
                                            new SubcommandData("r", "Configura o parâmetro r")
                                                    .addOption(OptionType.INTEGER, "n", "Valor de r", true)
                                    ),

                            // 4. /recuperar <de> <ate>
                            Commands.slash("recuperar", "Recupera dados entre intervalos")
                                    .addOption(OptionType.INTEGER, "de", "Índice inicial", true)
                                    .addOption(OptionType.INTEGER, "ate", "Índice final", true),

                            // 5. /replica-token
                            Commands.slash("replica-token", "Gera ou gere token de réplica"),

                            // 6. /replica-remover <id> (Mantido como String para aceitar IDs textuais ou numéricos)
                            Commands.slash("replica-remover", "Remove uma réplica pelo ID")
                                    .addOption(OptionType.INTEGER, "id", "ID da réplica", true),

                            // 7. /rajada <n> <intervaloms> (Nome em minúsculas exigido pelo Discord)
                            Commands.slash("rajada", "Envia rajada de mensagens")
                                    .addOption(OptionType.INTEGER, "n", "Número de mensagens", true)
                                    .addOption(OptionType.INTEGER, "intervaloms", "Intervalo em ms", true)
                    ).queue();
                    System.out.println("Comandos slash registados na guild com sucesso.");
                }
            }

        }catch (Exception e) {
            System.err.println("A ligação falhou!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        String commandName = event.getName();

        boolean isAdminCommand = commandName.equals("config") ||
                commandName.equals("replica-token") ||
                commandName.equals("replica-remover") ||
                commandName.equals("rajada");

        boolean hasAdminRole = event.getMember() != null &&
                event.getMember().getRoles().stream()
                        .anyMatch(r -> r.getName().equalsIgnoreCase(ADMIN_ROLE_NAME));

        if (isAdminCommand && !hasAdminRole) {
            event.reply("Acesso negado: Requer o papel `" + ADMIN_ROLE_NAME + "`.").queue();
            return;
        }

        switch(commandName){
            case "recuperar":
            {
                if(event.getOption("de") == null || event.getOption("ate") == null)
                {
                    event.reply("Erro: Falta um ou mais argumentos no comando").queue();
                    return;
                }
                long de, ate;
                try{
                    de = event.getOption("de").getAsLong();
                    ate = event.getOption("ate").getAsLong();
                } catch (Exception e) {
                    event.reply("Erro: Falha nos argumentos").queue();
                    return;
                }

                if (de > ate) {
                    event.reply("Erro: O argumento `de` não pode ser superior a `ate` (`" + de + " > " + ate + "`).").queue();
                    return;
                }
                event.reply("`/recuperar` recebido: de=" + de + ", ate=" + ate + " — ainda não implementado").queue();
                break;
            }
            case "config":
            {
                String subcommandName = event.getSubcommandName();
                if ("r".equals(subcommandName)) {
                    if(event.getOption("n") == null)
                    {
                        event.reply("Erro: Argumento n em falta").queue();
                        return;
                    }
                    long n;
                    try{
                        n = event.getOption("n").getAsLong();
                    } catch (Exception e) {
                        event.reply("Erro: Falha nos argumentos").queue();
                        return;
                    }
                    event.reply("`/config r` recebido: n ="+ n + " Ainda não implementado").queue();
                    return;
                }
                return;
            }
            case "replica-token":
            {
                event.reply("`/replica-token` recebido. Ainda não implementado").queue();
                return;
            }
            case "replica-remover":
            {
                if(event.getOption("id") == null)
                {
                    event.reply("Erro: Falta o argumento <id> no comando").queue();
                    return;
                }
                long id;
                try{
                    id = event.getOption("id").getAsLong();
                } catch (Exception e) {
                    event.reply("Erro: Falha no argumento").queue();
                    return;
                }
                event.reply("`/replica-remover <id>` recebido (id=" + id + "). Ainda não implementado").queue();
                break;
            }
            case "rajada":
            {
                if(event.getOption("n") == null || event.getOption("intervaloms") == null)
                {
                    event.reply("Erro: Faltam argumentos no comando").queue();
                    return;
                }
                long n, intervaloms;
                try{
                    n = event.getOption("n").getAsLong();
                    intervaloms = event.getOption("intervaloms").getAsLong();
                } catch (Exception e) {
                    event.reply("Erro: Falha no(s) argumentos(s)").queue();
                    return;
                }
                event.reply("`/rajada <n> <intervaloMs>` recebido (n=" + n + " ; intervaloMs=" + intervaloms + "). Ainda não implementado").queue();
                break;
            }
            case "registar":
            {
                if(event.getOption("texto") == null)
                {
                    event.reply("Erro: Falta o argumento <texto>").queue();
                    return;
                }
                String texto;
                try {
                    texto = event.getOption("texto").getAsString();
                }catch (Exception e) {
                    event.reply("Erro: Falha no argumento").queue();
                    return;
                }
                event.reply("`/registar <texto>` recebido (texto=" + texto + "). Ainda não implementado").queue();
                break;
            }
            case "estado":
            {
                event.reply("`/estado` recebido. Ainda não implementado").queue();
                return;
            }
            default:
            {
                event.reply("Erro: Comando desconhecido").queue();
                return;
            }
        }

    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        // Ignorar mensagens de bots (incluindo o próprio)
        if (event.getAuthor().isBot()) {
            return;
        }

        // Verificar se a mensagem é num canal de texto e se o nome do canal é #arquivo
        if (event.isFromGuild() && event.getGuild().getId().equals(System.getenv("DISCORD_GUILD_ID")) && event.getChannel() instanceof TextChannel channel) {
            if (channel.getName().equalsIgnoreCase(ARCHIVE_CHANNEL_NAME)) {
                String conteudo = event.getMessage().getContentDisplay();
                String autor = event.getAuthor().getName();

                System.out.println("[ARQUIVO] Mensagem detetada de " + autor + ": " + conteudo);

                // Resposta provisória no mesmo canal
                event.getChannel().sendMessage("Mensagem registada do canal `#arquivo` (por " + autor + "): `" + conteudo + "`").queue();
            }
        }
    }

}

