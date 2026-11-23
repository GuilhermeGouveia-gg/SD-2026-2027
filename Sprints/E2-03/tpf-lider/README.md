# Sprint E2-02 — escrita local do líder

Implementação da [issue #21](https://github.com/cunhaestgv/SD-2026-2027/issues/21),
no enquadramento do [trabalho #19](https://github.com/cunhaestgv/SD-2026-2027/issues/19).
O sprint E2-01 mantém-se na pasta anterior. Neste sprint não existem réplicas,
catálogo, manifesto nem recuperação remota. `/estado`, `/config`, `/recuperar`,
`/replica-token` e `/replica-remover` mantêm respostas provisórias.

## Abrir e executar no IntelliJ

1. Abrir **o `pom.xml` desta pasta** como projeto Maven. Usar JDK 17 ou superior
   no Project SDK e na configuração Maven. O código foi compilado com JDK 25,
   usando `--release 17`.
2. Na configuração Run, selecionar a classe `tpf.LeaderBot`, o módulo
   `tpf-lider` e esta pasta como **Working directory**.
3. Em Environment variables definir `DISCORD_BOT_TOKEN` e `DISCORD_GUILD_ID`
   com os valores já usados no E2-01. Não os escrever no código ou no Git.
4. No servidor Discord devem existir o canal de texto `arquivo` e o papel
   `admin-arquivo`. Atribuir esse papel à conta que vai usar `/rajada`.
5. A aplicação Discord precisa do **Message Content Intent** ativo. O convite
   do bot deve incluir os scopes `bot` e `applications.commands`, com acesso
   a ver o canal e enviar mensagens. Não é necessário ativar Members Intent.
6. Clicar Run e aguardar `Bot online; comandos registados`. O registo dos
   comandos só é anunciado depois de o Discord confirmar a operação.

Só executar uma instância do bot por pasta de dados. `arquivo.lock` evita dois
escritores simultâneos; o ficheiro pode continuar a existir depois de fechar
o bot, porque o que conta é o lock mantido pelo processo.

Opcionalmente definir nas **VM options**:

```text
-Dtpf.chunkMax=65536 -Dtpf.pad=4096 -Dtpf.selagem=5000
```

`-Dtpf.dados=<pasta>` escolhe outra pasta de dados. Sem essa propriedade, usa-se
o Working directory. Todos os valores numéricos têm de ser positivos.

## Primeiro ensaio no Discord

1. Enviar `/registar texto:Olá mundo`. A resposta indica o LSN, depois do
   `FileChannel.force(true)` no log local.
2. Escrever uma mensagem normal em `#arquivo`. Deve receber o LSN seguinte.
   Bots, webhooks, outros servidores e outros canais são ignorados.
3. Esperar cerca de cinco segundos. A pasta `selados` deve conter pares
   `<chunkId>.bin` e `<chunkId>.hdr` e o log de eventos deve conter `SELADO`.
4. Parar normalmente, voltar a arrancar e repetir `/registar`: o LSN continua.
5. Com `admin-arquivo`, executar `/rajada n:500 intervaloms:0`. O texto sintético
   tem cerca de 350 bytes para que 500 registos originem vários chunks com os
   parâmetros padrão. A conclusão mostra o intervalo dos LSN, que pode incluir
   registos reais intercalados. Só se admite uma rajada de cada vez.

A rajada aceita 1 a 100000 registos e intervalos de 0 a 60000 ms. O intervalo é
uma espera entre escritas, além do tempo de I/O. A rajada corre num executor
separado. Para rajadas que ultrapassem a validade do hook Discord, confirmar
o resultado no log, porque a atualização final da resposta pode expirar.

## Ficheiros locais e garantias

| Ficheiro | Conteúdo |
| --- | --- |
| `master.key` | 32 bytes aleatórios, somente leitura para o dono |
| `registos.log` | `lsn;instante;autor;canal;textoBase64`, UTF-8, uma linha por registo |
| `eventos-lider.txt` | `instante;evento;detalhe`, incluindo `REGISTO`, `SELADO` e erros |
| `selados/<id>.bin` | Ciphertext AES-GCM com a tag de 16 bytes no final |
| `selados/<id>.hdr` | `1;saltBase64;nonceBase64` seguido de LF |

`instante` é um `long` em milissegundos desde epoch. `autor` e `canal` são IDs
Discord. Só `texto` é convertido para Base64; Base64 **não é cifra**. O log
local permite ler o texto. Apenas os chunks em `selados` estão cifrados.

O método sincronizado de escrita atribui o LSN, acrescenta e força a linha,
regista o evento e acrescenta o registo ao chunk. A resposta só vem depois.
O mesmo monitor de `LocalArchive` protege as escritas e a selagem do timer.
O registo que atinge ou excede o limite faz parte do chunk que sela de imediato.
O limite conta os bytes da serialização antes do padding, incluindo o cabeçalho.

O timer conta cinco segundos desde o primeiro registo do chunk. Uma paragem
normal sela o restante. Depois de uma queda, o arranque lê o log, verifica os
chunks completos e volta a agrupar os registos ainda não selados. Uma cauda
sem LF é tratada como escrita incompleta e truncada; corrupção de linhas
completas, de chunks completos ou LSN fora de sequência impede o arranque.
Um `.bin` sem `.hdr`, resultante de queda entre os dois renames, é ignorado e
os registos são reconstruídos do log; esse `.bin` órfão pode permanecer no disco.

Os ficheiros de chunks são escritos com `force(true)` e renomeados atomicamente,
um de cada vez. O par `.bin`/`.hdr` não é uma transação única. Se houver erro de
I/O após a escrita do log, o arquivo suspende novas escritas até reiniciar;
consultar o log antes de repetir o pedido. Não existe deduplicação de pedidos.
No E2-02 uma confirmação significa persistência **local**, sem cópias remotas.

## Sprint E2-03 — líder, réplicas e escrita replicada

### Configuração

O líder continua a usar JDK 17+, Maven e `DISCORD_BOT_TOKEN` /
`DISCORD_GUILD_ID`. O Hub WebSocket escuta em `-Dtpf.porto=8765`.

```text
-Dtpf.chunkMax=65536 -Dtpf.pad=4096 -Dtpf.selagem=5000
-Dtpf.porto=8765 -Dtpf.periodoHb=2000 -Dtpf.suspeita=6000
-Dtpf.morta=10000 -Dtpf.tokenValidade=600000 -Dtpf.r=3
-Dtpf.wMin=2 -Dtpf.ackTimeout=4000 -Dtpf.maxRondas=3
-Dtpf.margem=65536 -Dtpf.alfa=1.0 -Dtpf.backoffMin=1000
-Dtpf.backoffMax=30000
```

Arrancar o líder com `tpf.LeaderBot`. Arrancar uma réplica:

```text
java -cp target/classes tpf.ReplicaService <id> ws://127.0.0.1:8765 <quotaBytes> [token]
```

O primeiro arranque requer o token gerado por `/replica-token`. Os arranques
seguintes usam apenas `OLA;<id>`. A réplica cria `dados-<id>/privada.pk8`,
`dados-<id>/publica.x509` e os ficheiros cifrados dessa réplica.

### Mensagens

| Direção | Mensagem |
| --- | --- |
| Réplica → líder/Hub | `OLA;<id>;<chavePublicaB64>;<token>` ou `OLA;<id>` |
| Hub → réplica | `DESAFIO;<nonceB64>` |
| Réplica → Hub | `PROVA;<id>;<assinaturaB64>` |
| Réplica → líder | `PUB;tpf-lider;HB;<id>;<usados>;<quota>` |
| Réplica → líder | `PUB;tpf-lider;INVENTARIO;<id>;<chunkId1>,...` |
| Líder → réplica | `STORE;<chunkId>;<hdrB64>;<binB64>` |
| Réplica → líder | `PUB;tpf-lider;ACK;<id>;<chunkId>;<sha256>` |
| Réplica | `SUB;tpf-r-<id>` e `SUB;tpf-ctl` |

### Sincronização

`ReplicaRegistry.reserveAndChoose` protege escolha e soma de reservas no mesmo
monitor. `Catalog` protege o catálogo em todas as alterações. Os callbacks JDA,
WebSocket, timers de selagem e timers de heartbeat/ACK são threads separadas.
Os dados partilhados são `ReplicaRegistry`, `Catalog`, o conjunto de `STORE`
pendentes e o estado da rajada. Cada alteração do catálogo é persistida
atomicamente em `catalogo.txt`.

### Respostas às questões de reflexão

1. O token autoriza o primeiro registo e limita a emissão de identidades;
   Ed25519 prova posse da chave privada. Um token intercetado pode ser usado
   uma vez até expirar, mas não permite ligar sem assinar o nonce com a chave
   correspondente à chave pública fornecida.
2. Assinar `id` e prefixo liga a prova ao protocolo e à identidade, impedindo
   a reutilização da assinatura de outro id ou contexto numa ligação nova.
3. Escolha e reserva têm de ser atómicas para que dois selamentos concorrentes
   não usem o mesmo espaço livre. Sem sincronização, ambas podem observar um
   valor livre antigo e ultrapassar a quota.
4. `W_MIN` impõe confirmação suficiente e a maioria impede declarar durabilidade
   sem alcançar uma maioria de R. Com R=5 são necessárias 3 confirmações.
   W=1 toleraria uma única cópia; exigir R aumenta latência e custo.
5. Os chunks são imutáveis e identificados pelo SHA-256; existe um único líder.
   Não há concorrência entre vários escritores num mesmo objeto, por isso a
   maioria aqui mede durabilidade/replicação, não um quórum de atualização.
6. α=1 pondera fortemente o espaço livre, α=0 elimina essa influência da
   pontuação e deixa o rendezvous hashing uniforme. α>1 favorece mais réplicas
   grandes.
7. O ACK só pode aparecer depois de `ATOMIC_MOVE` dos ficheiros, pois o líder
   trata-o como evidência de persistência de ambos os ficheiros.
8. O líder confia no id autenticado da ligação, não no texto livre do ACK,
   impedindo uma réplica de atribuir cópias a outra identidade.

## Formato binário do chunk

Todos os inteiros usam **big-endian**, como `DataOutputStream`.

| Campo | Formato |
| --- | --- |
| Magic | int de 4 bytes, `0x54504631` (`TPF1`) |
| Número de registos | int de 4 bytes |
| Por registo: LSN | long de 8 bytes |
| Por registo: instante | long de 8 bytes |
| Por registo: autor, canal, texto | Cada string: int com o número de bytes UTF-8, seguido desses bytes |
| Padding | Zeros até o tamanho total ser múltiplo de `PAD_BYTES` |

O leitor usa a contagem e os comprimentos para reconstruir os registos e
ignora o padding, verificando que contém zeros. Strings podem conter Unicode,
pontos e vírgulas e mudanças de linha no texto.

## Cifra e identificação

Para cada chunk são gerados salt de 32 bytes e nonce de 12 bytes com
`SecureRandom`. HKDF-SHA256 faz Extract com o salt e Expand com
`info="tpf-chunk-v1"` e contador `0x01`, produzindo uma DEK de 32 bytes.
O AAD é ASCII `tpf-v1`, versão da chave como int big-endian (sempre 1), salt e
nonce. AES-256-GCM usa tag de 128 bits. O nome `.bin` é o SHA-256 hexadecimal
do ciphertext **incluindo a tag**. `open` verifica hash, versão e autenticação,
por essa ordem, lançando exceção em caso de falha.

O **plaintext com padding** é múltiplo de 4096. O `.bin` tem mais 16 bytes por
causa da tag GCM: `(tamanhoBin - 16) % PAD_BYTES == 0`. O `.hdr` usa a versão
em decimal e apenas salt/nonce em Base64. Não contém LSN nem texto.

## Chave e permissões no Windows

Linux/macOS: criação com permissões 0600 e transição para 0400. Windows/NTFS:
é construída uma ACL protegida contra herança, com leitura apenas para o dono,
através do PowerShell nativo e das classes .NET de permissões. Não se usa
qualquer biblioteca de cifra externa. O caminho é passado por variável de
ambiente, não interpolado no comando.

O arranque valida o tamanho de 32 bytes e as permissões. Se o sistema impedir
a sua aplicação, **recusa arrancar**. Se já existirem dados sem `master.key`,
também recusa gerar uma substituta: é necessário repor a chave original.

O ambiente de execução assistida usado nesta sessão não aplicou alterações de
ACL. Por isso a validação de permissões e o arranque real precisam de ser
confirmados no IntelliJ/terminal normal do utilizador. O runner permite isolar
os testes funcionais dessa limitação; o bot não tem uma opção para a ignorar.

## Testes offline

No PowerShell, a partir desta pasta:

```powershell
.\testar.ps1 -Jdk 'C:\Program Files\Java\jdk-25.0.2'
```

Pode omitir `-Jdk` se `JAVA_HOME` ou `java`/`javac` no PATH apontarem para um
JDK 17+. O script não precisa de Maven, JDA, token nem rede: compila o núcleo e
o runner com `--release 17`. Os dados isolados ficam em `target/test-data`.
Para testar apenas a lógica funcional num ambiente que não permita ACL:

```powershell
.\testar.ps1 -Jdk 'C:\Program Files\Java\jdk-25.0.2' -SemAcl
```

Esse modo usa uma chave de fixture e injeção da cifra no construtor de pacote
de `LocalArchive`; não testa a segurança da chave. A execução normal de
`LeaderBot` chama sempre `MasterKey.carregar` e exige as permissões.

Os testes cobrem vetor HKDF da RFC 5869, round-trip, adulteração do ciphertext
com e sem recalcular o hash, salt, nonce, versão e outra chave, serialização,
padding, selagem por tempo/tamanho, escrita concorrente, reinício e processo
filho terminado sem `close`. Compilar **também o bot completo** no IntelliJ
ou com `mvn compile`; o runner do núcleo não valida a ligação ao Discord.

Para inspecionar um chunk local, executar a classe `tpf.InspecionarChunk`
no IntelliJ com argumentos `<pastaLider> <chunkId>`. A chave permanece local e
os registos decifrados são apresentados na consola. `/recuperar` mantém a
resposta provisória exigida neste sprint.

## Validação desta implementação

Em 23/11/2026, o bot completo compilou com `javac --release 17 -Xlint:all`,
sem avisos, usando a JDA 6.5.0 já disponível localmente. O runner com `-SemAcl`
passou 2280 verificações: selagem por tempo em 5077 ms, 500 registos em três
chunks, 400 escritas concorrentes sem saltos/repetições, recuperação após
queda abrupta e rejeição de adulterações. O teste completo, que inclui ACL,
recusou a chave porque o ambiente não aplicou a restrição de permissões.
Não foi executado um ensaio real no Discord. O `git fetch` falhou por falta
de rede; o branch foi integrado com a `master` disponível localmente.

Na primeira execução no terminal normal foi detetada uma falha adicional:
o helper voltava a marcar o proprietário para alteração depois de restringir
a chave, provocando `UnauthorizedAccessException` na passagem para leitura.
Foi corrigido para preservar o proprietário e alterar apenas a DACL. Falta
repetir o teste completo no terminal normal para confirmar esta correção.
