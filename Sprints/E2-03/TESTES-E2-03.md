# Testes do Sprint E2-03

## Testes automáticos

No ambiente com Maven e rede/dependências disponíveis:

```bash
cd Sprints/E2-03/tpf-lider
mvn test
mvn test-compile
java -cp target/test-classes:target/classes:$HOME/.m2/repository/org/java-websocket/Java-WebSocket/1.5.7/Java-WebSocket-1.5.7.jar tpf.Sprint03Tests
java -cp target/test-classes:target/classes:$HOME/.m2/repository/org/java-websocket/Java-WebSocket/1.5.7/Java-WebSocket-1.5.7.jar tpf.ArquivoTests
```

O runner automático verifica:

- placement determinístico com α 0, 1 e 2;
- distribuição aproximada em 10 000 escolhas;
- reservas concorrentes;
- transições VIVA/SUSPEITA/MORTA;
- recuperação a VIVA por heartbeat;
- catálogo persistido e carregado;
- assinatura Ed25519 válida, com outra chave e com outro id;
- quota insuficiente;
- estados DURAVEL/SELADO e subreplicação.

No Windows pode ser usado `.\testar.ps1 -SemAcl`.

## Preparação manual

1. Arrancar o líder com `tpf.LeiderBot` e JDK 17.
2. Definir `DISCORD_BOT_TOKEN`, `DISCORD_GUILD_ID` e `-Dtpf.dados=<pasta>`.
3. Confirmar que o papel `admin-arquivo` existe.
4. Para cada réplica:

```text
java -cp target/classes tpf.ReplicaService r1 ws://127.0.0.1:8765 8000000 <token-r1>
java -cp target/classes tpf.ReplicaService r2 ws://127.0.0.1:8765 8000000 <token-r2>
java -cp target/classes tpf.ReplicaService r3 ws://127.0.0.1:8765 8000000 <token-r3>
```

## Experiência 1 — autenticação

1. Gerar três tokens com `/replica-token`, usando uma conta com `admin-arquivo`.
2. Confirmar que a resposta é efémera.
3. Arrancar uma réplica com cada token.
4. Confirmar os três IDs em `replicas.txt`.
5. Reutilizar o mesmo token numa quarta réplica e confirmar que é rejeitado.
6. Alterar manualmente a hora ou aguardar mais de 10 minutos e usar o token expirado.
7. Introduzir uma chave privada diferente da enviada no `OLA` e confirmar uma `PROVA` rejeitada.

## Experiência 2 — heartbeats

1. Executar `/estado` e confirmar `VIVA`.
2. Terminar abruptamente uma réplica.
3. Após 6 segundos observar `SUSPEITA`.
4. Após 10 segundos desde o último heartbeat observar `MORTA`.
5. Relançar a réplica.
6. Confirmar `VIVA`, um novo heartbeat e `INVENTARIO` no log do líder.

## Experiência 3 — religação do líder

1. Parar o líder.
2. Observar em `eventos-r1.txt`, `eventos-r2.txt`, etc. os eventos `RELIGAR`.
3. Confirmar intervalos aproximadamente 1, 2, 4, 8, 16 e 30 segundos.
4. Reiniciar o líder.
5. Confirmar que a réplica volta a ligar sem o token e envia `INVENTARIO`.

## Experiência 4 — 50 registos

1. Manter pelo menos três réplicas `VIVA`.
2. Executar `/rajada n:50 intervaloms:0`.
3. Executar `/estado`.
4. Confirmar que todos os chunks estão `DURAVEL`, com 3 cópias.
5. Para cada `selados/<id>.bin`, confirmar o hash em cada réplica:

```bash
sha256sum dados-r1/<id>.bin
sha256sum dados-r2/<id>.bin
sha256sum dados-r3/<id>.bin
```

O nome do ficheiro e os três resultados devem ser iguais.

## Experiência 5 — distribuição α

Executar `Sprint03Tests`; para quatro réplicas com quotas 8, 2, 4 e 6 MB,
repetir `Placement.choose` 10 000 vezes com α 0, 1 e 2, sem reservas.
Com α=1 a de 8 MB deve aproximar-se de 40% e a de 2 MB de 10%.

## Experiência 6 — poucas réplicas

1. Parar duas das três réplicas.
2. Executar `/rajada n:10 intervaloms:100`.
3. Confirmar `DURAVEL` e `2/3` com duas réplicas.
4. Parar uma das duas restantes.
5. Confirmar `SELADO` e `1/3`.

## Experiência 7 — quota

1. Dar a uma réplica quota inferior ao tamanho do chunk.
2. Confirmar que não aparece como escolhida.
3. Enviar-lhe um `STORE` deliberadamente grande.
4. Confirmar `QUOTA` em `eventos-<id>.txt`.

## Experiência 8 — ACK com hash errado

1. Arrancar quatro réplicas.
2. Alterar temporariamente uma réplica para responder com um hash falso.
3. Selar um chunk.
4. Confirmar `ACK-REJEITADO <chunkId> <id> HASH`.
5. Confirmar que o chunk fica `DURAVEL` com duas cópias válidas.
6. Confirmar uma nova ronda e uma terceira cópia na quarta réplica.

## Experiência 9 — ID errado no ACK

1. Fazer uma réplica enviar:

```text
PUB;tpf-lider;ACK;outra-replicacao;<chunkId>;<sha256>
```

2. Confirmar `ACK-REJEITADO ... ID`.
3. Confirmar que não é registada cópia dessa réplica no catálogo.

## Lista de critérios

- [ ] Token novo regista apenas após prova válida.
- [ ] Token reutilizado/expirado/inválido é rejeitado.
- [ ] Mensagens fora de ordem fecham a ligação e registam `ERRO`.
- [ ] `OLA;<id>` só funciona para chave já registada.
- [ ] `/replica-token` é efémero e só acessível a `admin-arquivo`.
- [ ] Estados de heartbeat visíveis em `/estado`.
- [ ] Backoff observado entre 1 e 30 segundos.
- [ ] Reservas não ultrapassam quota antes/depois do ACK.
- [ ] Placement determinístico e distribuído.
- [ ] R cópias distintas; durabilidade exige W_MIN e maioria.
- [ ] ACK só aceite após STORE e depois dos ficheiros persistirem.
- [ ] Hash e ID autenticado validados.
- [ ] Catálogo sobrevive ao reinício.
- [ ] Réplicas não possuem texto claro, master key ou catálogo.
- [ ] README actualizado com protocolo e respostas teóricas.
