## Requisitos
Java 26 ou superior, IDE IntelliJ IDEA, código atualizado com o 
repositório Git.
## Configuração no IntelliJ
Em Run -> Edit Configurations, criar um novo perfil, nomeando-o e 
selecionando a classe LeaderBot e o diretório tpf-lider. Em 
'Environment Variables', acrescentar as variáveis de ambiente 
DISCORD_BOT_TOKEN e DISCORD_GUILD_ID com os respetivos valores e 
sem espaços no meio. Carregar em Apply, depois em OK.
## Execução
Na barra superior, aparecerá o nome da configuração e o respetivo botão 
Run. Tendo sido o perfil bem configurado, ao carregar em Run, 
aparecerão duas mensagens de sucesso no terminal e o bot Discord 
aparecerá online e a responder a comandos.
## Segurança
As variáveis de ambiente DISCORD_BOT_TOKEN e DISCORD_GUILD_ID são obtidas
e configuradas localmente. Não devem ser incluídas no Git.