"""Ensaios reais em duas instâncias Kubo e processos Java isolados.

Executar com --help. Só termina processos que o próprio ensaio criou.
Os dados e variantes ficam fora do código de produção, em --output.
"""
import argparse
import base64
import ctypes
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
HIDDEN = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0


def wait_for(predicate, seconds=20, label='condição'):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.1)
    raise AssertionError('Timeout: ' + label)


def content(path):
    return path.read_text(encoding='utf-8') if path.exists() else ''


def api(port, action, *args):
    query = urllib.parse.urlencode([('arg', a) for a in args])
    with urllib.request.urlopen(urllib.request.Request(
            f'http://127.0.0.1:{port}/api/v0/{action}?{query}', data=b''), timeout=5) as r:
        return r.read().decode()


def topics(port):
    return [base64.urlsafe_b64decode(s[1:] + '=' * (-len(s[1:]) % 4)).decode()
            for s in (json.loads(api(port, 'pubsub/ls')).get('Strings') or [])]


class Suite:
    def __init__(self, args):
        self.args = args
        self.out = Path(args.output).resolve()
        self.out.mkdir(parents=True, exist_ok=True)
        self.daemons = []
        self.results = []

    def kubo(self):
        for i in (1, 2):
            folder = self.out / f'ipfs-{i}'
            env = dict(os.environ, IPFS_PATH=str(folder))
            def command(*args):
                subprocess.run([self.args.ipfs, *args], env=env, check=True,
                               stdout=subprocess.DEVNULL, creationflags=HIDDEN)
            command('init', '--profile=test')
            command('config', 'Addresses.API', f'/ip4/127.0.0.1/tcp/{15000+i}')
            command('config', 'Addresses.Gateway', f'/ip4/127.0.0.1/tcp/{18080+i}')
            command('config', '--json', 'Addresses.Swarm', json.dumps([f'/ip4/127.0.0.1/tcp/{14000+i}']))
            command('config', '--json', 'Bootstrap', '[]')
            command('config', '--json', 'Discovery.MDNS.Enabled', 'false')
            log = open(self.out / f'ipfs-{i}.log', 'w', encoding='utf-8')
            proc = subprocess.Popen([self.args.ipfs, 'daemon', '--enable-pubsub-experiment'],
                                    env=env, stdout=log, stderr=log, creationflags=HIDDEN)
            self.daemons.append((proc, log))
            def ready():
                try:
                    return bool(api(15000+i, 'id'))
                except (OSError, ValueError):
                    return False
            wait_for(ready, label=f'daemon {i}')
        peer = json.loads(api(15002, 'id'))['ID']
        api(15001, 'swarm/connect', f'/ip4/127.0.0.1/tcp/14002/p2p/{peer}')

    def close(self):
        for proc, log in self.daemons:
            proc.terminate()
            try:
                proc.wait(10)
            except subprocess.TimeoutExpired:
                proc.kill(); proc.wait()
            log.close()

    def save(self):
        (self.out / 'resultados.json').write_text(json.dumps(self.results, ensure_ascii=False, indent=2), encoding='utf-8')

    def run(self):
        self.kubo()
        cases = [('8a-figura', 5000, 'figure'), ('8b-rajada', 5000, 'burst'),
                 ('8b-parado', 5000, 'idle')]
        cases += [(f'8c-falha-{i}', 5000, 'crash') for i in range(1, 4)]
        cases += [('8d-pausa', 5000, 'pause')]
        for period in (1000, 5000, 10000):
            cases += [(f'8e-{period}-rajada', period, 'burst'),
                      (f'8e-{period}-parado', period, 'idle'),
                      (f'8e-{period}-falha', period, 'crash')]
        if self.args.quick:
            cases = [('smoke', 1000, 'burst'), ('reinicio', 1000, 'figure'), ('pausa', 1000, 'pause')]
        for name, period, scenario in cases:
            print('INÍCIO', name, flush=True)
            case = Case(self, name, period)
            try:
                case.start()
                getattr(case, scenario)()
                case.check()
                result = {'ensaio': name, 'periodo': period, 'estado': 'OK', 'pasta': str(case.folder)}
                self.results.append(result)
                print('OK', name, flush=True)
            finally:
                case.close()
                self.save()


class Case:
    def __init__(self, suite, name, period, factor=3, leader_cp=None):
        self.suite, self.period, self.factor = suite, period, factor
        self.folder = suite.out / name
        self.folder.mkdir()
        self.processes = []
        self.leaders = {}
        self.leader_cp = leader_cp

    def spawn(self, role, cls, args, label):
        cp = self.leader_cp if role == 'lider' and self.leader_cp else ROOT / f'rep04-{role}' / 'out'
        log = open(self.folder / (label + '.log'), 'a', encoding='utf-8')
        p = subprocess.Popen([self.suite.args.java, f'-Drep04.periodo={self.period}',
                              f'-Drep04.fator={self.factor}', '-cp', str(cp), 'rep04.' + cls, *map(str, args)],
                             cwd=self.folder, stdin=subprocess.PIPE, stdout=log, stderr=log,
                             text=True, encoding='utf-8', creationflags=HIDDEN)
        self.processes.append((p, log))
        return p

    def leader(self, k):
        self.leaders[k] = self.spawn('lider', 'PartitionLeader',
                                    [k, 3, '127.0.0.1', 'http://127.0.0.1:15001/api/v0'], f'lider{k}')

    def start(self, wait_heartbeat=True):
        for i, partitions in [(1, '0,1'), (2, '1,2'), (3, '2,0')]:
            self.spawn('replica', 'Replica', [i, partitions, 15002], f'replica{i}')
        wait_for(lambda: all(('rep04-p'+str(k)) in topics(15002) for k in range(3)), label='subscrições réplicas')
        for k in range(3):
            self.leader(k)
        wait_for(lambda: all(('rep04-in-p'+str(k)) in topics(15001) for k in range(3)), label='subscrições líderes')
        self.spawn('processador', 'AlarmProcessor', [3, 'http://127.0.0.1:15002/api/v0'], 'processador')
        self.producer = self.spawn('produtor', 'Producer', [3, 'http://127.0.0.1:15002/api/v0'], 'produtor')
        # Permite formar a malha e receber pelo menos um heartbeat em todos os monitores.
        if wait_heartbeat:
            wait_for(lambda: all('RECEBIDO;p'+str(k) in content(self.folder/'monitor-produtor.txt') for k in range(3)),
                     seconds=self.period/1000+15, label='heartbeats iniciais')

    def send(self, line):
        self.producer.stdin.write(line + '\n')
        self.producer.stdin.flush()

    def replicas_equal(self):
        for i, ks in [(1, (0,1)), (2, (1,2)), (3, (2,0))]:
            for k in ks:
                if content(self.folder/f'lider-p{k}.txt') != content(self.folder/f'replica-{i}-p{k}.txt'):
                    return False
        return True

    def burst(self):
        self.send('rajada 300 10')
        wait_for(lambda: 'RAJADA: 300' in content(self.folder/'produtor.log'), 90, 'rajada')
        time.sleep(2*self.period/1000 + 1)
        wait_for(self.replicas_equal, 15, 'convergência de todas as réplicas')

    def idle(self):
        time.sleep(60)
        assert 'SUSPEITA' not in content(self.folder/'monitor-produtor.txt'), 'suspeita com líder ocioso'

    def kill(self, k):
        # O instante é anotado imediatamente antes de terminar o processo de teste.
        with (self.folder/'falhas.txt').open('a', encoding='utf-8') as f:
            f.write(f'p{k};{int(time.time()*1000)}\n')
        self.leaders[k].kill(); self.leaders[k].wait()

    def crash(self):
        self.send('rajada 300 10')
        time.sleep(.8)
        self.kill(1)
        time.sleep(30)
        self.leader(1)
        # Com Δ=10s, aguardar também a verificação máxima de 31s antes de voltar a ver heartbeat.
        time.sleep(self.period/1000 + 2)
        self.send('S02 21.0')
        time.sleep(self.period/1000 + 1)
        wait_for(self.replicas_equal, 20, 'convergência após falha')
        for id_ in ('replica1', 'replica2', 'processador', 'produtor'):
            log = content(self.folder/f'monitor-{id_}.txt')
            assert 'SUSPEITA;p1' in log and 'REINICIO;p1' in log, id_

    def figure(self):
        self.send('S01 21.0'); time.sleep(.15)
        self.send('S04 22.5')
        wait_for(lambda: len(content(self.folder/'replica-1-p0.txt').splitlines()) == 2,
                 self.period/1000+15, 'registos 1 e 2')
        def empty_after_two():
            return any(f[3:5] == ['2','0'] for f in (l.split(';') for l in content(self.folder/'hb-p0.txt').splitlines()))
        wait_for(empty_after_two, self.period/1000+10, 'lote vazio depois do registo 2')
        self.send('S01 21.4')
        wait_for(lambda: len(content(self.folder/'lider-p0.txt').splitlines()) == 3, 1, 'append do registo 3')
        self.kill(0)
        wait_for(lambda: 'SUSPEITA;p0' in content(self.folder/'monitor-replica1.txt'),
                 3*self.period/1000+4, 'suspeita p0')
        self.leader(0)
        wait_for(self.replicas_equal, self.period/1000+15, 'recuperação do último registo')
        for id_ in ('replica1', 'replica3'):
            log = content(self.folder/f'monitor-{id_}.txt')
            assert 'REINICIO;p0' in log and 'RECUPERACAO;p0;de=3;ate=3' in log, id_

    def pause(self):
        p = self.leaders[2]
        if os.name == 'nt':
            ntdll = ctypes.WinDLL('ntdll')
            ntdll.NtSuspendProcess.argtypes = [ctypes.c_void_p]
            ntdll.NtResumeProcess.argtypes = [ctypes.c_void_p]
            assert ntdll.NtSuspendProcess(int(p._handle)) == 0
        else:
            import signal
            os.kill(p.pid, signal.SIGSTOP)
        try:
            self.send('S03 22.0')
            time.sleep(20)
        finally:
            if os.name == 'nt':
                assert ntdll.NtResumeProcess(int(p._handle)) == 0
            else:
                os.kill(p.pid, signal.SIGCONT)
        wait_for(lambda: 'RETOMA;p2' in content(self.folder/'monitor-produtor.txt'), 15, 'retoma da pausa')
        time.sleep(self.period/1000 + 1)
        wait_for(self.replicas_equal, 15, 'convergência após pausa')
        assert 'REINICIO;p2' not in content(self.folder/'monitor-produtor.txt')

    def check(self):
        # Parar primeiro os escritores garante uma fotografia coerente para os analisadores.
        self.close()
        for cls in ('HeartbeatCheck', 'CausalCheck'):
            args = [self.suite.args.java, '-cp', str(ROOT/'rep04-replica/out'), 'rep04.'+cls, '3', str(self.folder)]
            if cls == 'HeartbeatCheck' and (self.folder/'falhas.txt').exists():
                args.append(str(self.folder/'falhas.txt'))
            result = subprocess.run(args, text=True, encoding='utf-8', capture_output=True, creationflags=HIDDEN)
            (self.folder/(cls+'.txt')).write_text(result.stdout+result.stderr, encoding='utf-8')
            assert result.returncode == 0, result.stdout + result.stderr

    def close(self):
        for p, log in self.processes:
            if p.poll() is None:
                p.kill(); p.wait()
            log.close()
        self.processes.clear()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--java', default='java')
    parser.add_argument('--ipfs', required=True, help='Caminho do executável Kubo')
    parser.add_argument('--output', required=True, help='Pasta nova fora de OneDrive para os ensaios')
    parser.add_argument('--quick', action='store_true', help='Três ensaios mais curtos, não substituem a bateria completa')
    args = parser.parse_args()
    suite = Suite(args)
    try:
        suite.run()
    finally:
        suite.close()


if __name__ == '__main__':
    main()
