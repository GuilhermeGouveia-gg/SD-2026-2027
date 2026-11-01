"""Cenários determinísticos para classificação e tempos dos analisadores."""
import argparse
from datetime import datetime, timezone
from pathlib import Path
import subprocess
from integracao import ROOT, HIDDEN


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--java', default='java')
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    out = Path(args.output).resolve()
    out.mkdir(parents=True)
    def put(name, text):
        (out/name).write_text(text, encoding='utf-8')
    timestamp = datetime.fromtimestamp(100.1, timezone.utc).replace(tzinfo=None).isoformat()
    put('lider-p0.txt', f'1;S01;21.0;{timestamp};1;1\n')
    put('hb-p0.txt', '1000;1;100000;0;0;50\n1000;2;105000;1;1;100\n'
        '1000;3;121000;1;0;50\n140000;1;140000;1;0;50\n')
    put('falhas.txt', 'p0;122000\n')
    put('monitor-replica1.txt',
        '100010;RECEBIDO;p0;inc=1000;hb=1;ultimoSeq=0;n=0;seq=-\n'
        '105010;RECEBIDO;p0;inc=1000;hb=2;ultimoSeq=1;n=1;seq=1..1\n'
        '120100;SUSPEITA;p0;silencio_ms=15090\n'
        '121000;RETOMA;p0;inc=1000;hb=3\n'
        '121000;RECEBIDO;p0;inc=1000;hb=3;ultimoSeq=1;n=0;seq=-\n'
        '136100;SUSPEITA;p0;silencio_ms=15100\n'
        '140000;REINICIO;p0;inc=1000->140000\n'
        '140000;RETOMA;p0;inc=140000;hb=1\n'
        '140000;RECEBIDO;p0;inc=140000;hb=1;ultimoSeq=1;n=0;seq=-\n')
    def run(cls, failures=False):
        cmd = [args.java, '-Duser.timezone=UTC', '-cp', str(ROOT/'rep04-replica/out'), 'rep04.'+cls, '1', str(out)]
        if failures:
            cmd.append(str(out/'falhas.txt'))
        return subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', creationflags=HIDDEN)
    report = run('HeartbeatCheck', True)
    assert report.returncode == 0, report.stderr
    assert 'classificacao=FALSA' in report.stdout and 'classificacao=REAL' in report.stdout
    assert 'deteccao_ms=14100' in report.stdout, report.stdout
    assert 'media=4910.000 max=4910' in report.stdout, report.stdout
    assert 'proporcao_vazios=0.666667' in report.stdout
    assert run('CausalCheck').returncode == 0
    # Um erro causal persistido deve produzir uma saída de erro, não sucesso silencioso.
    put('lider-p0.txt', f'1;S01;21.0;{timestamp};1;2\n')
    assert run('CausalCheck').returncode != 0
    # Um log truncado deve ser rejeitado em vez de produzir métricas plausíveis.
    put('hb-p0.txt', '1000;2;105000;1\n')
    assert run('HeartbeatCheck').returncode != 0
    put('resultado-contratos.txt', 'OK: falsa suspeita, reinício, tempo de deteção, latência, contagem, erro causal e log truncado\n')
    print('OK: contratos dos analisadores')


if __name__ == '__main__':
    main()
