"""Falhas e comparação com COMINDIR01; usa os mesmos nós isolados de testar.py."""
import argparse
import json
from pathlib import Path
import re
import socket
import subprocess
import threading
import time

from testar import BASE, Suite, lines, settle, wait_for


class Additional(Suite):
    def __init__(self, args):
        super().__init__(args)
        self.faults = []

    def fault(self, name, observation):
        self.faults.append({"falha": name, "observacao": observation})
        self.mark(name)

    def files_and_ports(self, node):
        folder = self.root / "ficheiro-e-porto"
        folder.mkdir()
        reps = [self.replica(i, "0", node, folder) for i in (1, 2)]
        self.subscribed(node, ["rep02-p0"])
        leader = self.leader(0, 1, node, folder)
        self.subscribed(node, ["rep02-in-p0"])
        self.send(node, 1, ["S01 20"] * 5, folder, "inicial")
        wait_for(lambda: len(lines(folder / "lider-p0.txt")) == 5 and self.equal(folder, 0, (1, 2)), "base do teste")
        conflict = self.leader(0, 1, node, folder, "porto-ocupado")
        wait_for(lambda: conflict.poll() is not None, "segundo líder rejeitado", 10)
        text = (folder / "porto-ocupado.log").read_text(encoding="utf-8")
        assert "LÍDER FALHOU:" in text, text
        self.fault("Porto de recuperação ocupado", text.strip())
        self.stop(leader)
        (folder / "lider-p0.txt").unlink()  # Ficheiro gerado exclusivamente por este teste.
        leader = self.leader(0, 1, node, folder, "lider-sem-historico")
        self.subscribed(node, ["rep02-in-p0"])
        self.send(node, 1, ["S01 99"], folder, "apos-apagar")
        settle(list(folder.glob("*.txt")))
        assert len(lines(folder / "lider-p0.txt")) == 1 and len(lines(folder / "replica-1-p0.txt")) == 5
        assert "DUPLICADO: 1" in (folder / "replica-1.log").read_text(encoding="utf-8")
        self.fault("Histórico do líder apagado", "Sem exceção: líder recomeça em 1; réplicas mantêm 5 e rejeitam seq=1 como duplicado. Não recupera.")
        for proc in [leader, *reps]:
            self.stop(proc)

    def timeout_and_isolation(self, node):
        folder = self.root / "timeout-e-isolamento"
        folder.mkdir()
        with socket.socket() as server:
            server.bind(("127.0.0.1", 0))
            server.listen()
            server.settimeout(15)
            port = server.getsockname()[1]
            accepted = threading.Event()
            def silent():
                with server.accept()[0] as client:
                    client.recv(1024)
                    accepted.set()
                    time.sleep(6)
            thread = threading.Thread(target=silent, daemon=True)
            thread.start()
            replica = self.replica("isolamento", "0,7", node, folder)
            self.subscribed(node, ["rep02-p0", "rep02-p7"])
            for seq in range(2, 13):
                self.publish_raw(node, "rep02-p7", f"REG;127.0.0.1;{port};{seq};S01;20.0;t")
            assert accepted.wait(timeout=10)
            self.publish_raw(node, "rep02-p0", f"REG;127.0.0.1;{port};1;S02;20.0;t")
            wait_for(lambda: len(lines(folder / "replica-isolamento-p0.txt")) == 1, "partição 0 durante bloqueio de p7", 3)
            log = folder / "replica-isolamento.log"
            wait_for(lambda: "RECUPERAÇÃO FALHOU:" in log.read_text(encoding="utf-8"), "timeout de leitura", 10)
            assert not lines(folder / "replica-isolamento-p7.txt")
            thread.join(timeout=3)
            self.stop(replica)
        self.fault("Timeout TCP e independência entre partições", "p0 escreveu enquanto p7 aguardava TCP; p7 expirou com Read timed out e manteve 11 retidos.")

    def duplicate_processes(self, node):
        folder = self.root / "duas-instancias"
        folder.mkdir()
        reps = [self.replica("2", "8", node, folder, f"instancia-{i}") for i in (1, 2)]
        self.subscribed(node, ["rep02-p8"])
        for seq in range(1, 4):
            self.publish_raw(node, "rep02-p8", f"REG;127.0.0.1;7008;{seq};S01;20.0;t")
            time.sleep(0.2)
        settle(list(folder.glob("*.txt")))
        content = lines(folder / "replica-2-p8.txt")
        logs = "\n".join((folder / f"instancia-{i}.log").read_text(encoding="utf-8") for i in (1, 2))
        assert content != [f"{seq};S01;20.0;t" for seq in range(1, 4)] or "IO p8:" in logs
        self.fault("Duas instâncias com o mesmo id e pasta", f"Foram escritas {len(content)} linhas para 3 registos. synchronized não coordena JVMs distintas. IO observado: {'IO p8:' in logs}.")
        for proc in reps:
            self.stop(proc)

    def leader_down_and_wrong_n(self, node):
        folder = self.root / "lider-parado-e-n-errado"
        folder.mkdir()
        reps = [self.replica(1, "0,1", node, folder), self.replica(2, "1,2", node, folder),
                self.replica(3, "2,0", node, folder)]
        self.subscribed(node, [f"rep02-p{k}" for k in range(3)])
        leaders = [self.leader(k, 3, node, folder) for k in range(3)]
        self.subscribed(node, [f"rep02-in-p{k}" for k in range(3)])
        self.send(node, 3, [f"S0{i} 20" for i in range(1, 7)], folder, "base")
        settle(list(folder.glob("*.txt")))
        initial = [len(lines(folder / f"lider-p{k}.txt")) for k in range(3)]
        producer = self.spawn([self.args.java, "-Dfile.encoding=UTF-8", "-Dstdout.encoding=UTF-8",
                               "-cp", BASE / "rep02-produtor" / "out", "rep02.Producer", "3",
                               f"http://127.0.0.1:{node['api']}/api/v0"], folder, folder / "rajada.log", stdin=True)
        producer.stdin.write("rajada 300 20\nsair\n")
        producer.stdin.flush()
        producer.stdin.close()
        wait_for(lambda: len(lines(folder / "lider-p1.txt")) >= initial[1] + 5, "líder 1 a meio da rajada")
        self.stop(leaders[1])
        producer.wait(timeout=60)
        settle(list(folder.glob("*.txt")))
        after = [len(lines(folder / f"lider-p{k}.txt")) for k in range(3)]
        assert after[0] > initial[0] and after[2] > initial[2]
        output = (folder / "rajada.log").read_text(encoding="utf-8")
        sent_p1 = int(re.search(r"ENVIADOS p1: (\d+)", output)[1])
        missing = sent_p1 - (after[1] - initial[1])
        assert missing > 0
        leaders[1] = self.leader(1, 3, node, folder, "lider-1-regresso")
        self.subscribed(node, ["rep02-in-p1"])
        self.send(node, 3, ["S02 21"], folder, "depois-regresso")
        wait_for(lambda: len(lines(folder / "lider-p1.txt")) == after[1] + 1, "seq após reinício")
        self.fault("Líder p1 parado a meio de uma rajada", f"p0 e p2 continuaram. {missing} leituras aceites pela API para p1 não ficaram no líder; não houve recuperação produtor→líder. Reinício continuou a sequência persistida.")
        counts = [len(lines(folder / f"lider-p{k}.txt")) for k in range(3)]
        self.send(node, 4, [f"S{i:02d} 30" for i in range(1, 21)], folder, "produtor-n4")
        settle(list(folder.glob("*.txt")))
        final = [len(lines(folder / f"lider-p{k}.txt")) for k in range(3)]
        logs = "\n".join((folder / f"lider-{k}.log").read_text(encoding="utf-8") for k in (0, 2))
        logs += (folder / "lider-1-regresso.log").read_text(encoding="utf-8")
        assert "PARTIÇÃO ERRADA:" in logs
        self.fault("Produtor N=4 e líderes N=3", f"Das 20 leituras, {sum(final) - sum(counts)} foram registadas. Outras foram rejeitadas por partição errada ou enviadas para p3, sem líder.")
        for proc in [*leaders, *reps]:
            self.stop(proc)

    def baseline(self, node):
        old = BASE.parent / "E1-COMINDIR01"
        javac = str(Path(self.args.java).with_name("javac.exe" if Path(self.args.java).suffix == ".exe" else "javac"))
        classpaths = {}
        for role in ("lider", "replica"):
            classpath = self.root / ("rep01-" + role)
            sources = old / ("rep01-" + role) / "src" / "rep01"
            names = ["SensorRecord.java", "RecordFile.java", "IpfsPubSub.java",
                     "PubSubLeader.java" if role == "lider" else "PubSubReplica.java"]
            subprocess.run([javac, "--release", "11", "-encoding", "UTF-8", "-d", str(classpath),
                            *[str(sources / name) for name in names]], check=True)
            classpaths[role] = classpath
        for attempt in range(1, 4):
            folder = self.root / f"baseline-{attempt}"
            folder.mkdir()
            api = f"http://127.0.0.1:{node['api']}/api/v0"
            reps = [self.spawn([self.args.java, "-Dfile.encoding=UTF-8", "-Dstdout.encoding=UTF-8", "-cp",
                                classpaths["replica"], "rep01.PubSubReplica", str(ident), api], folder,
                               folder / f"replica-{ident}.log") for ident in (1, 2, 3)]
            self.subscribed(node, ["rep01-temperaturas"])
            leader = self.spawn([self.args.java, "-Dfile.encoding=UTF-8", "-Dstdout.encoding=UTF-8", "-cp",
                                 classpaths["lider"], "rep01.PubSubLeader", api], folder,
                                folder / "lider.log", stdin=True)
            leader.stdin.write("rajada 1000\nsair\n")
            leader.stdin.flush()
            leader.stdin.close()
            leader.wait(timeout=120)
            settle(list(folder.glob("*.txt")), seconds=2.0)
            text = (folder / "lider.log").read_text(encoding="utf-8")
            timing = re.search(r"Primeiro envio: (\d+).*?Último envio: (\d+)", text, re.S)
            assert timing, text[-2000:]
            leader_count = len(lines(folder / "lider.txt"))
            missing = [leader_count - len(lines(folder / f"replica-{i}.txt")) for i in (1, 2, 3)]
            last_write = max((folder / f"replica-{i}.txt").stat().st_mtime_ns // 1_000_000 for i in (1, 2, 3))
            self.metrics.append({"cenario": "um-no", "teste": "COMINDIR01", "execucao": attempt,
                                 "envio_ms": int(timing[2]) - int(timing[1]),
                                 "registos_no_lider": leader_count, "em_falta_por_replica": missing,
                                 "ligacoes_tcp": 0, "registos_tcp": 0,
                                 "convergencia_ms": last_write - int(timing[1]) if not any(missing) else None})
            for proc in [leader, *reps]:
                self.stop(proc)
        self.mark("Comparação: três rajadas reais de 1000 na COMINDIR01 original")

    def replica_stopped_partitioned(self, node):
        folder = self.root / "replica-parada-particoes"
        folder.mkdir()
        assignments = {1: (0, 1), 2: (1, 2), 3: (2, 0)}
        reps = {ident: self.replica(ident, ",".join(map(str, parts)), node, folder,
                                   props=["-Dperda=0.2"] if ident == 3 else ())
                for ident, parts in assignments.items()}
        self.subscribed(node, [f"rep02-p{k}" for k in range(3)])
        leaders = [self.leader(k, 3, node, folder) for k in range(3)]
        self.subscribed(node, [f"rep02-in-p{k}" for k in range(3)])
        self.send(node, 3, ["S01 20", "S02 20", "S03 20"], folder, "base")
        initial = len(lines(folder / "lider-p0.txt"))
        producer = self.spawn([self.args.java, "-Dfile.encoding=UTF-8", "-Dstdout.encoding=UTF-8",
                               "-cp", BASE / "rep02-produtor" / "out", "rep02.Producer", "3",
                               f"http://127.0.0.1:{node['api']}/api/v0"], folder, folder / "rajada.log", stdin=True)
        producer.stdin.write("rajada 1000 20\nsair\n")
        producer.stdin.flush()
        producer.stdin.close()
        wait_for(lambda: len(lines(folder / "lider-p0.txt")) >= initial + 5, "replicação a meio da rajada")
        self.stop(reps[1])
        producer.wait(timeout=60)
        settle(list(folder.glob("*.txt")), seconds=2.0)
        reps[1] = self.replica(1, "0,1", node, folder, "replica-1-regresso")
        self.subscribed(node, ["rep02-p0", "rep02-p1"])
        self.send(node, 3, ["S01 20", "S02 20", "S03 20"] * 11, folder, "seguimento")
        settle(list(folder.glob("*.txt")), seconds=2.0)
        missing = {}
        for ident, parts in assignments.items():
            for k in parts:
                count = len(lines(folder / f"lider-p{k}.txt"))
                missing[f"r{ident}-p{k}"] = count - len(lines(folder / f"replica-{ident}-p{k}.txt"))
        assert missing["r1-p0"] == 0 and missing["r1-p1"] == 0, missing
        output = (folder / "replica-1-regresso.log").read_text(encoding="utf-8")
        assert "RECUPERAÇÃO:" in output
        self.fault("Réplica 1 parada durante rajada 1000, réplica 3 com perda=0.2",
                   "Após reinício e 11 leituras de seguimento por partição, faltas por cópia: " + json.dumps(missing))
        for proc in [*leaders, *reps.values()]:
            self.stop(proc)

    def n1(self, node, size=3000):
        folder = self.root / f"comparacao-n1-{size}"
        folder.mkdir()
        reps = [self.replica(i, "0", node, folder) for i in (1, 2, 3)]
        self.subscribed(node, ["rep02-p0"])
        leader = self.leader(0, 1, node, folder)
        self.subscribed(node, ["rep02-in-p0"])
        for attempt in range(1, 4):
            before = len(lines(folder / "lider-p0.txt"))
            previous = (folder / "replica-3.log").read_text(encoding="utf-8")
            output = self.send(node, 1, [f"rajada {size} 20"], folder, f"rajada-{attempt}")
            settle(list(folder.glob("*.txt")), seconds=2.0)
            count = len(lines(folder / "lider-p0.txt"))
            missing = [count - len(lines(folder / f"replica-{i}-p0.txt")) for i in (1, 2, 3)]
            last_write = max((folder / f"replica-{i}-p0.txt").stat().st_mtime_ns // 1_000_000 for i in (1, 2, 3))
            first = int(re.search(r"Primeiro envio: (\d+)", output)[1])
            current = (folder / "replica-3.log").read_text(encoding="utf-8")
            delta = current[len(previous):]
            state = re.findall(r"ESTADO p0 .*", current)[-1]
            self.metrics.append({"cenario": "um-no", "teste": f"N=1-{size}", "execucao": attempt,
                                 "envio_ms": int(re.search(r"Tempo de envio \(ms\): (\d+)", output)[1]),
                                 "registos_no_lider": count - before, "perdas_produtor_lider": size - (count - before),
                                 "em_falta_por_replica": missing,
                                 "ligacoes_tcp": delta.count("RECUPERAÇÃO:"),
                                 "registos_tcp": delta.count("RECUPERADO:"),
                                 "retidos": int(re.search(r"Retidos: (\d+)", state)[1]),
                                 "convergencia_ms": last_write - first if not any(missing) else None})
        for proc in [leader, *reps]:
            self.stop(proc)
        self.mark(f"Comparação: três rajadas de {size} com N=1 e três réplicas")

    def save(self):
        super().save()
        (self.root / "falhas.json").write_text(json.dumps(self.faults, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java", required=True)
    parser.add_argument("--ipfs", required=True)
    parser.add_argument("--tipo", choices=("falhas", "comparacao", "sem-perdas", "todos"), default="todos")
    parser.add_argument("--output", help="Pasta de resultados; preferir disco local fora do OneDrive")
    suite = Additional(parser.parse_args())
    try:
        node = suite.kubo()
        if suite.args.tipo in ("falhas", "todos"):
            suite.files_and_ports(node)
            suite.timeout_and_isolation(node)
            suite.duplicate_processes(node)
            suite.leader_down_and_wrong_n(node)
            suite.replica_stopped_partitioned(node)
        if suite.args.tipo in ("comparacao", "todos"):
            suite.baseline(node)
        if suite.args.tipo in ("comparacao", "sem-perdas", "todos"):
            suite.n1(node, 1000)
        if suite.args.tipo in ("comparacao", "todos"):
            suite.n1(node)
    finally:
        suite.save()
        suite.close()


if __name__ == "__main__":
    main()
