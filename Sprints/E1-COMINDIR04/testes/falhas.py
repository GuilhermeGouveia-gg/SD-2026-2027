"""Cinco variantes deliberadas da secção 9; nunca substitui os fontes finais."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import time
from integracao import Suite, Case, ROOT, HIDDEN, content, wait_for


def compile_variant(suite, name, mutate):
    folder = suite.out / ('variante-' + name)
    src = folder / 'src'
    shutil.copytree(ROOT / 'rep04-lider/src/rep04', src)
    leader = src / 'PartitionLeader.java'
    original = leader.read_text(encoding='utf-8')
    changed = mutate(original)
    assert changed != original
    leader.write_text(changed, encoding='utf-8')
    output = folder / 'out'
    output.mkdir()
    javac = str(Path(suite.args.java).with_name('javac.exe' if Path(suite.args.java).suffix == '.exe' else 'javac'))
    subprocess.run([javac, '--release', '11', '-encoding', 'UTF-8', '-d', str(output),
                    *map(str, src.glob('*.java'))], check=True, creationflags=HIDDEN)
    return output


def race(source):
    start = source.index('        Batch batch;\n', source.index('private void enviarLote'))
    end = source.index('        try {\n            String message', start)
    return source[:start] + '''        List<SensorRecord> closed;
        long nextHb;
        synchronized (file) {
            closed = pendentes;
            pendentes = new ArrayList<>();
            nextHb = ++hb;
        }
        System.out.println("TESTE_FECHO: " + nextHb);
        try { Thread.sleep(700); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        Batch batch = new Batch(host, port, inc, nextHb, lastSeq, closed);
''' + source[end:]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--java', default='java')
    parser.add_argument('--ipfs', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--periodo', type=int, default=5000,
                        help='Período em ms, 5000 por omissão; 1000 permite uma passagem acelerada')
    args = parser.parse_args()
    suite = Suite(args)
    try:
        suite.kubo()
        variants = {
            'sem-vazios': lambda s: s.replace('        Batch batch;\n',
                '        synchronized (file) { if (pendentes.isEmpty()) return; }\n        Batch batch;\n'),
            'tick-heartbeat': lambda s: s.replace('            List<SensorRecord> closed = pendentes;',
                '            lamport++; vector.tick(k);\n            List<SensorRecord> closed = pendentes;'),
            'snapshot-fora': race,
            'ultimo-zero': lambda s: s.replace('inc, ++hb, lastSeq, closed', 'inc, ++hb, 0, closed')
                .replace('            String message = batch.encode();',
                         '            if (batch.hb % 10 == 0) return;\n            String message = batch.encode();'),
            'fator-1': None,
        }
        for name, mutation in variants.items():
            print('INÍCIO falha', name, flush=True)
            cp = compile_variant(suite, name, mutation) if mutation else None
            case = Case(suite, '9-' + name, args.periodo,
                        factor=1 if name == 'fator-1' else 3, leader_cp=cp)
            try:
                case.start(wait_heartbeat=name != 'sem-vazios')
                if name == 'sem-vazios':
                    case.send('S01 21.0')
                    wait_for(lambda: bool(content(case.folder/'lider-p0.txt')), label='primeiro registo')
                    # A leitura ainda pode esperar um período até ao seu único lote.
                    time.sleep(4 * case.period / 1000 + 3)
                    assert 'SUSPEITA;p0' in content(case.folder/'monitor-produtor.txt')
                elif name == 'tick-heartbeat':
                    case.send('S01 21.0')
                    time.sleep(3)
                    assert '!= seq' in content(case.folder/'lider0.log')
                elif name == 'snapshot-fora':
                    # Publicar após o fecho da lista e antes da leitura de ultimoSeq.
                    before = content(case.folder/'lider0.log').count('TESTE_FECHO')
                    case.send('S01 21.0')
                    wait_for(lambda: content(case.folder/'lider0.log').count('TESTE_FECHO') > before,
                             label='fecho da lista')
                    case.send('S04 22.5')
                    time.sleep(3)
                    assert 'RECUPERACAO;p0' in content(case.folder/'monitor-replica1.txt')
                elif name == 'ultimo-zero':
                    # O hb=10 leva o registo final. Aguardar 9 antes de o produzir.
                    wait_for(lambda: ';9;' in content(case.folder/'hb-p0.txt'), 9 * case.period / 1000 + 10, 'hb9')
                    case.send('S01 21.0')
                    wait_for(lambda: bool(content(case.folder/'lider-p0.txt')), label='registo a perder')
                    time.sleep(2 * case.period / 1000 + 2)
                    assert not content(case.folder/'replica-1-p0.txt')
                    assert 'HB_PERDIDO;p0' in content(case.folder/'monitor-produtor.txt')
                else:
                    time.sleep(20)
                case.close()
                results = {'falha': name, 'pasta': str(case.folder)}
                for cls in ('HeartbeatCheck', 'CausalCheck'):
                    # Ausência de qualquer publicação é um resultado válido do ensaio sem-vazios.
                    if name == 'sem-vazios':
                        for k in range(3):
                            (case.folder/f'hb-p{k}.txt').touch(exist_ok=True)
                    cmd = [args.java, '-cp', str(ROOT/'rep04-replica/out'), 'rep04.' + cls,
                           '3', str(case.folder)]
                    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', creationflags=HIDDEN)
                    (case.folder/(cls+'.txt')).write_text(result.stdout+result.stderr, encoding='utf-8')
                    results[cls+'_exit'] = result.returncode
                if name == 'snapshot-fora':
                    assert 'snapshot incoerente' in content(case.folder/'HeartbeatCheck.txt')
                results['suspeitas'] = sum(content(p).count(';SUSPEITA;') for p in case.folder.glob('monitor-*.txt'))
                suite.results.append(results)
                print('OK falha', name, flush=True)
            finally:
                case.close()
                suite.save()
    finally:
        suite.close()


if __name__ == '__main__':
    main()
