package rep04;

import java.util.ArrayList;
import java.util.List;

/** Contratos que exercitam perda, duplicação, reinício e validação do protocolo. */
public final class Contracts {
    private static void require(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }
    public static void main(String[] args) throws Exception {
        SensorRecord r = SensorRecord.now(1, "S01", 21, 1, VectorClock.parse("1,0,0", 3));
        Batch original = new Batch("127.0.0.1", 7000, 100, 1, 1, List.of(r));
        Batch parsed = Batch.decode(original.encode(), 3);
        require(parsed.records.size() == 1 && parsed.records.get(0).toLine().equals(r.toLine()), "round trip");
        require(Batch.decode(new Batch("localhost", 7000, 100, 2, 1, List.of()).encode(), 3).records.isEmpty(), "lote vazio");
        for (String bad : List.of(original.encode().replace(";1\n", ";2\n"),
                "LOTE;x;0;1;1;0;0", "LOTE;x;7000;1;0;0;0", original.encode() + "\nextra")) {
            try { Batch.decode(bad, 3); throw new AssertionError("aceitou lote inválido"); }
            catch (IllegalArgumentException expected) { }
        }
        // O produtor consegue ler um cabeçalho sem interpretar o corpo.
        require(Batch.header(original.encode().split("\n")[0] + "\ncorpo opaco").count == 1, "header");
        List<String> events = new ArrayList<>();
        HeartbeatMonitor monitor = new HeartbeatMonitor(0, 10, 1, events::add);
        require(monitor.state() == HeartbeatMonitor.State.DESCONHECIDO, "estado inicial");
        monitor.onHeartbeat(100, 1);
        Thread.sleep(25);
        monitor.check(); monitor.check();
        require(events.stream().filter(s -> s.startsWith("SUSPEITA")).count() == 1, "suspeita única");
        monitor.onHeartbeat(100, 1); monitor.onHeartbeat(99, 20);
        require(monitor.state() == HeartbeatMonitor.State.SUSPEITO, "atrasado não renova temporizador");
        monitor.onHeartbeat(100, 3);
        require(monitor.state() == HeartbeatMonitor.State.ATIVO, "retoma");
        require(events.stream().anyMatch(s -> s.startsWith("HB_PERDIDO")), "heartbeat perdido");
        require(events.stream().anyMatch(s -> s.startsWith("RETOMA")), "retoma registada");
        monitor.onHeartbeat(200, 1);
        require(events.stream().anyMatch(s -> s.startsWith("REINICIO")), "reinício");
        System.out.println("OK: contratos de Batch e HeartbeatMonitor");
    }
}
