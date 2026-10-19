"""Testes de integração com processos Java, TCP e um/dois nós Kubo reais.

Usa apenas a biblioteca-padrão do Python para orquestrar os programas.
Não altera o repositório IPFS habitual. Os dados ficam em testes/execucoes/.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = Path(__file__).resolve().parents[1]
# As APIs de teste são locais e não devem passar por proxies do Windows.
LOCAL_HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def wait_for(predicate, description, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError("Tempo esgotado: " + description)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def multibase(text):
    return "u" + base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def lines(path):
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8").splitlines()


def settle(paths, seconds=1.0, timeout=30):
    """Esperar que as escritas estabilizem, sem supor entrega garantida pelo PubSub."""
    previous = None
    changed = time.monotonic()
    deadline = changed + timeout
    while time.monotonic() < deadline:
        current = tuple(path.stat().st_size if path.exists() else 0 for path in paths)
        if current != previous:
            previous, changed = current, time.monotonic()
        elif time.monotonic() - changed >= seconds:
            return
        time.sleep(0.1)
    raise AssertionError("As escritas não estabilizaram")


def jhash(text):
    value = 0
    for char in text:
        value = (31 * value + ord(char)) & 0xFFFFFFFF
    return value - 0x100000000 if value >= 0x80000000 else value


class Suite:
    def __init__(self, args):
        self.args = args
        output = Path(args.output) if getattr(args, "output", None) else BASE / "testes" / "execucoes"
        self.root = output / time.strftime("%Y%m%d-%H%M%S")
        self.root.mkdir(parents=True)
        self.processes = []
        self.logs = []
        self.metrics = []
        self.passed = []
        self.nodes = []
        print("Artefactos:", self.root, flush=True)

    def mark(self, message):
        self.passed.append(message)
        print("PASSOU:", message, flush=True)

    def spawn(self, cmd, cwd, log, env=None, stdin=False):
        output = open(log, "w", encoding="utf-8")
        self.logs.append(output)
        proc = subprocess.Popen(
            [str(x) for x in cmd], cwd=cwd, env=env,
            stdin=subprocess.PIPE if stdin else subprocess.DEVNULL,
            stdout=output, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8",
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        self.processes.append(proc)
        return proc

    @staticmethod
    def stop(proc):
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)

    def close(self):
        for proc in reversed(self.processes):
            self.stop(proc)
        for log in self.logs:
            log.close()

    def compile(self):
        javac = str(Path(self.args.java).with_name("javac.exe" if os.name == "nt" else "javac"))
        for project in ("rep02-produtor", "rep02-lider", "rep02-replica"):
            src = BASE / project / "src" / "rep02"
            subprocess.run([javac, "--release", "11", "-encoding", "UTF-8", "-Xlint:all",
                            "-d", str(BASE / project / "out"), *map(str, src.glob("*.java"))], check=True)
        self.mark("Três projetos compilados para Java 11, com -Xlint:all")

    def api(self, node, command, args=(), data=None):
        query = urllib.parse.urlencode([("arg", value) for value in args])
        request = urllib.request.Request(
            f"http://127.0.0.1:{node['api']}/api/v0/{command}?{query}",
            data=b"" if data is None else data, method="POST")
        with LOCAL_HTTP.open(request, timeout=10) as response:
            body = response.read()
        return json.loads(body) if body else {}

    def kubo(self):
        index = len(self.nodes) + 1
        repo = self.root / f"ipfs-{index}"
        env = dict(os.environ, IPFS_PATH=str(repo))
        def command(*args):
            result = subprocess.run([self.args.ipfs, *args], env=env, capture_output=True,
                                    text=True, encoding="utf-8", timeout=30)
            if result.returncode:
                raise RuntimeError(result.stderr)
            return result.stdout
        command("init")
        node = {"api": free_port(), "swarm": free_port(), "gateway": free_port()}
        command("config", "Addresses.API", f"/ip4/127.0.0.1/tcp/{node['api']}")
        command("config", "Addresses.Gateway", f"/ip4/127.0.0.1/tcp/{node['gateway']}")
        command("config", "--json", "Addresses.Swarm", json.dumps([f"/ip4/127.0.0.1/tcp/{node['swarm']}"]))
        command("config", "--json", "Bootstrap", "[]")
        command("config", "--json", "Discovery.MDNS.Enabled", "false")
        command("config", "--json", "Pubsub.Enabled", "true")
        proc = self.spawn([self.args.ipfs, "daemon", "--routing=none"], self.root,
                          self.root / f"ipfs-{index}.log", env=env)
        last_error = []
        def ready():
            if proc.poll() is not None:
                raise RuntimeError("Daemon terminou: " + (self.root / f"ipfs-{index}.log").read_text())
            try:
                node["id"] = self.api(node, "id")["ID"]
                return True
            except (OSError, urllib.error.URLError) as error:
                last_error[:] = [str(error)]
                return False
        try:
            wait_for(ready, "daemon IPFS", 30)
        except AssertionError as error:
            raise AssertionError(f"{error}: {last_error}") from error
        self.nodes.append(node)
        if index > 1:
            first = self.nodes[0]
            self.api(node, "swarm/connect", [f"/ip4/127.0.0.1/tcp/{first['swarm']}/p2p/{first['id']}"])
        return node

    def java(self, project, main, args, folder, name, props=()):
        return self.spawn([self.args.java, "-Dfile.encoding=UTF-8", "-Dstdout.encoding=UTF-8",
                           "-Dstderr.encoding=UTF-8", *props, "-cp", BASE / project / "out",
                           "rep02." + main, *map(str, args)], folder, folder / (name + ".log"))

    def replica(self, ident, partitions, node, folder, name=None, props=()):
        return self.java("rep02-replica", "PartitionReplica", [ident, partitions, node["api"]],
                         folder, name or f"replica-{ident}", props)

    def leader(self, k, n, node, folder, name=None, host="127.0.0.1"):
        return self.java("rep02-lider", "PartitionLeader",
                         [k, n, host, f"http://127.0.0.1:{node['api']}/api/v0"],
                         folder, name or f"lider-{k}")

    def subscribed(self, node, topics):
        expected = {multibase(topic) for topic in topics}
        def ready():
            actual = set(self.api(node, "pubsub/ls").get("Strings") or [])
            return expected <= actual
        wait_for(ready, "subscrições " + str(topics), 30)
        # ls enumera tópicos, não cada callback local; permitir concluir todas as subscrições.
        time.sleep(0.5)

    def send(self, node, n, commands, folder, name):
        log = folder / (name + ".log")
        output = open(log, "w", encoding="utf-8")
        try:
            proc = subprocess.Popen([self.args.java, "-Dfile.encoding=UTF-8", "-Dstdout.encoding=UTF-8",
                                     "-Dstderr.encoding=UTF-8", "-cp", str(BASE / "rep02-produtor" / "out"),
                                     "rep02.Producer", str(n), f"http://127.0.0.1:{node['api']}/api/v0"],
                                    stdin=subprocess.PIPE, cwd=folder,
                                    stdout=output, stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            self.processes.append(proc)
            wait_for(lambda: "Comandos:" in log.read_text(encoding="utf-8"), "produtor pronto", 15)
            for command in commands + ["sair"]:
                proc.stdin.write(command + "\n")
                proc.stdin.flush()
                if not command.startswith("rajada"):
                    # Leituras controladas espaçadas; as rajadas continuam sem pausas.
                    time.sleep(0.08)
            proc.stdin.close()
            proc.wait(timeout=180)
        finally:
            output.close()
        text = log.read_text(encoding="utf-8")
        assert proc.returncode == 0 and "INVÁLIDO" not in text, text[-2000:]
        if not any(command.startswith("rajada") for command in commands):
            assert "FALHOU" not in text, text[-2000:]
        return text

    def equal(self, folder, k, ids):
        expected = lines(folder / f"lider-p{k}.txt")
        return all(lines(folder / f"replica-{ident}-p{k}.txt") == expected for ident in ids)

    def request(self, k, text):
        with socket.create_connection(("127.0.0.1", 7000 + k), timeout=5) as sock:
            sock.sendall((text + "\n").encode())
            return sock.makefile(encoding="utf-8").read().splitlines()

    def publish_raw(self, node, topic, message):
        boundary = "rep02-test-boundary"
        data = (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="msg"\r\n'
                f"Content-Type: application/octet-stream\r\n\r\n{message}\r\n--{boundary}--\r\n").encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{node['api']}/api/v0/pubsub/pub?arg={multibase(topic)}", data=data, method="POST",
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
        with LOCAL_HTTP.open(request, timeout=10) as response:
            response.read()

    def deterministic(self, node):
        folder = self.root / "parte-a"
        folder.mkdir()
        replicas = {i: self.replica(i, "0", node, folder) for i in (1, 2, 3)}
        self.subscribed(node, ["rep02-p0"])
        leader = self.leader(0, 1, node, folder)
        self.subscribed(node, ["rep02-in-p0"])
        self.send(node, 1, [f"S0{i} 20" for i in range(1, 6)], folder, "cinco")
        wait_for(lambda: len(lines(folder / "lider-p0.txt")) == 5 and self.equal(folder, 0, (1, 2, 3)), "cinco iguais")
        self.mark("Parte A: cinco leituras em três réplicas, ficheiros iguais")
        assert self.request(0, "PEDIDO 2 4") == lines(folder / "lider-p0.txt")[1:4] + ["FIM"]
        assert self.request(0, "PEDIDO 4 999") == lines(folder / "lider-p0.txt")[3:] + ["FIM"]
        for invalid in ("LIXO", "PEDIDO a 4", "PEDIDO 5 2", "PEDIDO 0 3"):
            assert self.request(0, invalid)[0].startswith("ERRO ")
        self.mark("TCP: intervalos inclusivos, limite do histórico e pedidos inválidos")

        self.stop(replicas[2])
        self.send(node, 1, ["S01 21"] * 3, folder, "tres-parada")
        replicas[2] = self.replica(2, "0", node, folder, "replica-2-regresso")
        self.subscribed(node, ["rep02-p0"])
        self.send(node, 1, ["S01 22"] * 2, folder, "duas-retidas")
        time.sleep(0.5)
        assert len(lines(folder / "replica-2-p0.txt")) == 5
        self.send(node, 1, ["S01 23"] * 8, folder, "dez-retidas")
        time.sleep(0.5)
        assert len(lines(folder / "replica-2-p0.txt")) == 5
        self.send(node, 1, ["S01 24"], folder, "onze-retidas")
        wait_for(lambda: self.equal(folder, 0, (1, 2, 3)), "recuperação na 11.ª retida")
        log = (folder / "replica-2-regresso.log").read_text(encoding="utf-8")
        assert "RECUPERAÇÃO: 6..8" in log and log.count("RECUPERADO:") == 3
        self.mark("Parte A: réplica parada recupera 6..8 apenas na 11.ª mensagem retida")

        self.stop(replicas[3])
        replicas[3] = self.replica(3, "0", node, folder, "replica-3-descartar", ["-Ddescartar=20"])
        self.subscribed(node, ["rep02-p0"])
        self.send(node, 1, ["S01 25"], folder, "perda-final")
        time.sleep(0.5)
        assert len(lines(folder / "replica-3-p0.txt")) == 19
        self.send(node, 1, ["S01 25"] * 10, folder, "dez-apos-perda")
        time.sleep(0.5)
        assert len(lines(folder / "replica-3-p0.txt")) == 19
        self.send(node, 1, ["S01 25"], folder, "onze-apos-perda")
        wait_for(lambda: self.equal(folder, 0, (1, 2, 3)), "recuperar última leitura perdida")
        self.mark("Parte A: perda da última leitura fica por recuperar até chegarem 11 posteriores")

        self.stop(replicas[3])
        (folder / "replica-3-p0.txt").unlink()  # Apenas o ficheiro criado por esta execução isolada.
        replicas[3] = self.replica(3, "0", node, folder, "replica-3-nova")
        self.subscribed(node, ["rep02-p0"])
        self.send(node, 1, ["rajada 20 10"], folder, "historico")
        settle(list(folder.glob("*.txt")))
        if not self.equal(folder, 0, (1, 2, 3)):
            self.send(node, 1, ["S01 25.1"] * 11, folder, "seguimento-historico")
        wait_for(lambda: self.equal(folder, 0, (1, 2, 3)), "nova réplica recupera histórico")
        history_end = len(lines(folder / "lider-p0.txt"))
        log = (folder / "replica-3-nova.log").read_text(encoding="utf-8")
        assert "RECUPERAÇÃO: 1..31" in log
        assert all(f"RECUPERADO: {seq} |" in log for seq in range(1, 32))
        self.mark("Parte A: réplica vazia recupera o histórico inicial 1..31 numa ligação e converge")

        # Um anúncio com porto inacessível força uma falha, sem perder os retidos.
        self.stop(leader)
        leader = self.leader(0, 1, node, folder, "lider-errado", "10.255.255.1")
        self.subscribed(node, ["rep02-in-p0"])
        self.stop(replicas[2])
        self.send(node, 1, ["S01 26"], folder, "lacuna-falha")
        replicas[2] = self.replica(2, "0", node, folder, "replica-2-repetir")
        self.subscribed(node, ["rep02-p0"])
        self.send(node, 1, ["S01 27"] * 11, folder, "falha-recuperacao")
        logpath = folder / "replica-2-repetir.log"
        wait_for(lambda: "RECUPERAÇÃO FALHOU:" in logpath.read_text(encoding="utf-8"), "falha de TCP")
        assert len(lines(folder / "replica-2-p0.txt")) == history_end
        self.stop(leader)
        leader = self.leader(0, 1, node, folder, "lider-corrigido")
        self.subscribed(node, ["rep02-in-p0"])
        self.send(node, 1, ["S01 28"], folder, "repetir-pedido")
        wait_for(lambda: self.equal(folder, 0, (1, 2, 3)), "nova tentativa TCP")
        self.mark("Parte A: falha TCP mantém retidos; anúncio corrigido permite recuperar")

        for proc in [leader, *replicas.values()]:
            self.stop(proc)

    def interrupted_response(self, node):
        folder = self.root / "resposta-interrompida"
        folder.mkdir()
        port = free_port()
        records = [f"{seq};S01;20.0;2026-10-19T10:00:00" for seq in range(1, 19)]
        requests = []
        errors = []
        with socket.socket() as server:
            server.bind(("127.0.0.1", port))
            server.listen()
            server.settimeout(30)
            def serve():
                try:
                    for attempt in range(2):
                        with server.accept()[0] as client:
                            text = client.makefile(encoding="utf-8").readline().strip()
                            requests.append(text)
                            _, start, end = text.split()
                            selected = records[int(start) - 1:int(end)]
                            # Simular o líder a fechar sem FIM após duas linhas.
                            reply = selected[:2] if attempt == 0 else selected + ["FIM"]
                            client.sendall(("\n".join(reply) + "\n").encode())
                except Exception as error:
                    errors.append(str(error))
            thread = threading.Thread(target=serve, daemon=True)
            thread.start()
            replica = self.replica("trunc", "9", node, folder)
            self.subscribed(node, ["rep02-p9"])
            for record in records[6:17]:
                self.publish_raw(node, "rep02-p9", f"REG;127.0.0.1;{port};{record}")
            log = folder / "replica-trunc.log"
            wait_for(lambda: "RECUPERAÇÃO FALHOU:" in log.read_text(encoding="utf-8"), "EOF sem FIM")
            assert lines(folder / "replica-trunc-p9.txt") == records[:2]
            self.publish_raw(node, "rep02-p9", f"REG;127.0.0.1;{port};{records[17]}")
            wait_for(lambda: lines(folder / "replica-trunc-p9.txt") == records, "retomar após resposta parcial")
            thread.join(timeout=5)
            assert not errors and requests == ["PEDIDO 1 6", "PEDIDO 3 6"], (errors, requests)
            self.stop(replica)
        self.mark("Resposta TCP sem FIM mantém escritas parciais e retoma no próximo seq em falta")

    def loss_measurements(self, node1, replica_node, label):
        folder = self.root / ("perdas-" + label)
        folder.mkdir()
        reps = {1: self.replica(1, "0", node1, folder),
                2: self.replica(2, "0", replica_node, folder),
                3: self.replica(3, "0", replica_node, folder, "replica-3-perda", ["-Dperda=0.2"])}
        self.subscribed(node1, ["rep02-p0"])
        self.subscribed(replica_node, ["rep02-p0"])
        leader = self.leader(0, 1, node1, folder)
        self.subscribed(node1, ["rep02-in-p0"])
        if node1 is not replica_node:
            time.sleep(2)
        for attempt in range(1, 4):
            before = len(lines(folder / "lider-p0.txt"))
            logpath = folder / "replica-3-perda.log"
            previous = logpath.read_text(encoding="utf-8")
            result = self.send(node1, 1, ["rajada 1000 10"], folder, f"rajada-{attempt}")
            # PubSub também pode perder naturalmente mensagens, incluindo produtor -> líder.
            settle(list(folder.glob("*.txt")), seconds=2.0)
            leader_count = len(lines(folder / "lider-p0.txt"))
            current = logpath.read_text(encoding="utf-8")
            delta = current[len(previous):]
            state = re.findall(r"ESTADO p0 .*", current)[-1]
            timing = re.search(r"Primeiro envio: (\d+).*Último envio: (\d+).*Tempo de envio \(ms\): (\d+)", result)
            row = {"cenario": label, "teste": "perda-0.2", "execucao": attempt,
                   "envio_ms": int(timing[3]),
                   "ligacoes_tcp": delta.count("RECUPERAÇÃO:"),
                   "registos_tcp": delta.count("RECUPERADO:"),
                   "registos_no_lider": leader_count - before,
                   "perdas_produtor_lider": 1000 - (leader_count - before),
                   "em_falta": leader_count - len(lines(folder / "replica-3-p0.txt")),
                   "em_falta_por_replica": [leader_count - len(lines(folder / f"replica-{ident}-p0.txt"))
                                            for ident in (1, 2, 3)],
                   "retidos": int(re.search(r"Retidos: (\d+)", state)[1]),
                   "ultima_escrita_ms": int(re.search(r"Última escrita: (\d+)", state)[1]),
                   "primeira_publicacao_ms": int(timing[1])}
            row["convergencia_ms"] = (row["ultima_escrita_ms"] - row["primeira_publicacao_ms"]
                                      if row["em_falta"] == 0 else None)
            assert "RECUPERAÇÃO FALHOU:" not in delta
            self.metrics.append(row)
        for proc in [leader, *reps.values()]:
            self.stop(proc)
        self.mark("Três rajadas de 1000 com 20% de perdas simuladas: " + label)

    def partitions(self, node):
        folder = self.root / "parte-b"
        folder.mkdir()
        assignments = {1: (0, 1), 2: (1, 2), 3: (2, 0)}
        reps = [self.replica(ident, ",".join(map(str, parts)), node, folder) for ident, parts in assignments.items()]
        self.subscribed(node, [f"rep02-p{k}" for k in range(3)])
        leaders = [self.leader(k, 3, node, folder) for k in range(3)]
        self.subscribed(node, [f"rep02-in-p{k}" for k in range(3)])
        def converged():
            return all(self.equal(folder, k, [ident for ident, parts in assignments.items() if k in parts])
                       for k in range(3))
        self.send(node, 3, [f"S0{i} 20" for i in range(1, 7)], folder, "seis-sensores")
        wait_for(lambda: sum(len(lines(folder / f"lider-p{k}.txt")) for k in range(3)) == 6
                 and converged(), "seis sensores particionados")
        for k in range(3):
            assert len(lines(folder / f"lider-p{k}.txt")) == 2
            assert all(jhash(line.split(";")[1]) % 3 == k for line in lines(folder / f"lider-p{k}.txt"))
        self.mark("Parte B: seis sensores, três sequências independentes e duas cópias por partição")

        for mode in ("rajada", "rajada-quente"):
            for attempt in range(1, 4):
                before = [len(lines(folder / f"lider-p{k}.txt")) for k in range(3)]
                result = self.send(node, 3, [f"{mode} 3000 20"], folder, f"{mode}-{attempt}")
                settle(list(folder.glob("*.txt")), seconds=2.0)
                counts = [len(lines(folder / f"lider-p{k}.txt")) - before[k] for k in range(3)]
                first = int(re.search(r"Primeiro envio: (\d+)", result)[1])
                last = max(max(map(int, re.findall(r"Última escrita: (\d+)",
                                         (folder / f"replica-{ident}.log").read_text(encoding="utf-8"))))
                           for ident in assignments)
                send_time = int(re.search(r"Tempo de envio \(ms\): (\d+)", result)[1])
                self.metrics.append({"cenario": "um-no", "teste": mode, "execucao": attempt,
                                     "registos_por_particao": counts, "envio_ms": send_time,
                                     "convergencia_ms": last - first if converged() else None,
                                     "perdas_produtor_lider": 3000 - sum(counts),
                                     "desequilibrio": max(counts) / (sum(counts) / 3) if sum(counts) else None})

        # Estas leituras de seguimento são posteriores às medições, para permitir verificar
        # a convergência quando uma rajada terminou com uma lacuna abaixo do limiar.
        for attempt in range(5):
            if converged():
                break
            self.send(node, 3, ["S01 20", "S02 20", "S03 20"] * 11,
                      folder, f"seguimento-{attempt}")
            settle(list(folder.glob("*.txt")), seconds=1.0)
        assert converged(), "Persistem lacunas mesmo após leituras de seguimento"

        # Mensagens mal formadas e encaminhamento errado não consomem seq.
        before = len(lines(folder / "lider-p0.txt"))
        wrong = next(f"S{i:02d}" for i in range(1, 21) if jhash(f"S{i:02d}") % 3 != 0)
        self.publish_raw(node, "rep02-in-p0", f"IN;{wrong};20")
        self.publish_raw(node, "rep02-in-p0", "IN;S01;NaN")
        time.sleep(0.5)
        assert len(lines(folder / "lider-p0.txt")) == before
        text = (folder / "lider-0.log").read_text(encoding="utf-8")
        assert "PARTIÇÃO ERRADA:" in text and "REJEITADO:" in text
        self.mark("Parte B: seis rajadas de 3000; chave quente e rejeição de mensagens inválidas")

        for proc in [*leaders, *reps]:
            self.stop(proc)
        result = subprocess.run([self.args.java, "-Dfile.encoding=UTF-8", "-Dstdout.encoding=UTF-8",
                                 "-cp", str(BASE / "rep02-replica" / "out"), "rep02.PartitionCheck", "3", str(folder)],
                                capture_output=True, text=True, encoding="utf-8", timeout=30)
        (folder / "verificacao.log").write_text(result.stdout + result.stderr, encoding="utf-8")
        assert result.returncode == 0 and "VERIFICAÇÃO: OK" in result.stdout, result.stdout[-3000:]
        # O verificador também tem de detetar corrupção, não apenas aceitar ficheiros bons.
        corrupt = self.root / "verificador-invalido"
        corrupt.mkdir()
        (corrupt / "lider-p0.txt").write_text("1;S01;20.0;t\n3;S01;20.0;t\n", encoding="utf-8")
        result = subprocess.run([self.args.java, "-Dfile.encoding=UTF-8", "-Dstdout.encoding=UTF-8",
                                 "-cp", str(BASE / "rep02-replica" / "out"), "rep02.PartitionCheck", "1", str(corrupt)],
                                capture_output=True, text=True, encoding="utf-8", timeout=10)
        assert result.returncode != 0 and "sequência inválida" in result.stdout
        self.mark("PartitionCheck aceita as nove cópias corretas e rejeita sequência com salto/réplicas ausentes")

    def save(self):
        (self.root / "resultados.json").write_text(json.dumps(
            {"testes_passados": self.passed, "medicoes": self.metrics,
             "notas": ["Perdas aleatórias: convergencia_ms=null significa que não convergiu no fim da rajada.",
                       "Dados e daemons IPFS isolados; nenhum repositório IPFS habitual foi alterado."]},
            ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java", required=True, help="Caminho absoluto para java.exe")
    parser.add_argument("--ipfs", required=True, help="Caminho absoluto para ipfs.exe")
    parser.add_argument("--secao", choices=("todas", "nucleo", "medicoes", "particoes"), default="todas")
    parser.add_argument("--output", help="Pasta de resultados; preferir disco local fora do OneDrive")
    args = parser.parse_args()
    suite = Suite(args)
    try:
        suite.compile()
        node1 = suite.kubo()
        if args.secao in ("todas", "nucleo"):
            suite.deterministic(node1)
            suite.interrupted_response(node1)
        if args.secao in ("todas", "particoes"):
            suite.partitions(node1)
        if args.secao in ("todas", "medicoes"):
            suite.loss_measurements(node1, node1, "um-no")
            node2 = suite.kubo()
            suite.loss_measurements(node1, node2, "dois-nos")
        suite.mark("Todos os testes de integração concluídos")
    finally:
        suite.save()
        suite.close()


if __name__ == "__main__":
    main()
