# Validação da COMINDIR04

Os testes usam apenas a biblioteca-padrão de Python e o JDK. A aplicação Java
usa apenas a biblioteca-padrão de Java. Os testes de integração exigem Kubo
real; não substituem o transporte por um servidor simulado.

Primeiro executar `compilar.ps1` na pasta da ficha: compila os quatro projetos
separadamente e cria os seus diretórios `out`. A opção `-Javac` permite indicar
o caminho de um compilador Java 11 ou superior.

## Testes de protocolo e do detetor

```powershell
javac --release 11 -encoding UTF-8 -cp rep04-replica/out -d rep04-replica/out testes/Contracts.java
```

Compila o teste de contratos com as classes da réplica. `--release 11` fixa a
compatibilidade, `-cp` procura as classes de aplicação e `-d` escolhe o destino.

```powershell
java -cp rep04-replica/out rep04.Contracts
```

Executa validação de lotes vazios/inválidos, cabeçalho sem corpo interpretado,
suspeita única, rejeição de heartbeats antigos/duplicados, perda, retoma e reinício.

## Analisadores com dados de referência

```powershell
python testes/analisadores.py --java java --output C:/resultados-comindir04/contratos
```

Cria uma pasta nova com uma linha temporal conhecida e verifica tempos exatos,
classificação de suspeitas, erro causal e rejeição de um log truncado. `--java`
escolhe o executável Java e `--output` a pasta nova para as evidências. O teste
fixa UTC apenas na sua JVM e nos seus dados artificiais, para não depender das
regras históricas de fuso horário do computador.

## Ensaios da secção 8

```powershell
python testes/integracao.py --java java --ipfs C:/caminho/kubo/ipfs.exe --output C:/resultados-comindir04/ensaios
```

Executa a sequência 8(a–e), incluindo três falhas de p1, pausas de 20 segundos,
inatividade durante um minuto e períodos de 1000, 5000 e 10000 ms. `--ipfs`
escolhe o Kubo instalado. Substituir o caminho de exemplo pelo executável real.
`--output` deve ser uma pasta nova, de preferência fora do OneDrive. A opção
opcional `--quick` faz três ensaios curtos e não substitui a bateria completa.

São criados dois repositórios Kubo isolados. São usados os portos locais
14001/14002, 15001/15002, 18081/18082 e os portos de recuperação 7000–7002;
devem estar livres. Todos os processos correm na mesma máquina. Em Windows os
processos auxiliares são criados sem janelas. A pausa utiliza a operação de
suspensão do Windows apenas sobre o processo criado pelo ensaio; em Unix usa
STOP/CONT. Não se suspendem nem terminam processos pessoais preexistentes.

As entradas na consola são automatizadas. A Figura 1 é reproduzida quanto à
ordem dos passos, persistência do registo 3 e recuperação após reinício; os
instantes absolutos não são impostos como se fosse uma execução ideal sem
atrasos. O instante da falha é anotado imediatamente antes da terminação.

Antes de executar HeartbeatCheck e CausalCheck, o ensaio para os escritores.
Compara também todos os ficheiros de réplica com os respetivos líderes.
`resultados.json` lista os ensaios concluídos; cada subpasta conserva os dados,
logs, relatórios e amostras de latência. Processos criados são encerrados mesmo
se uma verificação falhar. Não confundir erros de publicação com entrega de
todas as 300 leituras ao líder.

## Falhas deliberadas da secção 9

```powershell
python testes/falhas.py --java C:/caminho/jdk/bin/java.exe --ipfs C:/caminho/kubo/ipfs.exe --output C:/resultados-comindir04/falhas
```

Cria e compila cópias isoladas dos líderes com os quatro erros pedidos e usa
participantes com fator 1 no quinto ensaio. O caminho completo em `--java`
permite encontrar o `javac` ao lado do executável Java. Os caminhos são exemplos
a substituir. A pasta de saída tem de ser nova. Usa os mesmos portos dos testes
de integração: executar as duas baterias separadamente.

A corrida da leitura de ultimoSeq é tornada reproduzível com uma pausa de
700 ms depois de trocar a lista na variante isolada. Essa pausa e os erros
nunca são escritos no código de produção. Por omissão, todas as variantes usam
Δ=5000 ms; `--periodo 1000` permite uma passagem adicional acelerada. O fator 1
é observado durante 20 s. O fator 1 aumenta
o risco de falsa suspeita, mas não se exige que ocorra uma em todas as execuções.

Um verificador pode não detetar um erro que não deixa um registo incorreto nos
seus ficheiros de entrada. Por exemplo, o teste V[k]==seq do próprio líder pode
impedir a persistência antes de CausalCheck observar uma violação. Os relatórios
distinguem prevenção no líder, evento observado e erro devolvido pelo analisador.
