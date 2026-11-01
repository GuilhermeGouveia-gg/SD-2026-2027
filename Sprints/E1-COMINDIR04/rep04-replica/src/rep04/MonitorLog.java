package rep04;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Serializa as linhas das diferentes subscrições no mesmo ficheiro. */
public final class MonitorLog implements AutoCloseable {
    private final Path path;
    private final ScheduledExecutorService timer;

    public MonitorLog(String id) {
        path = Path.of("monitor-" + id + ".txt");
        timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "verificacao-" + id);
            t.setDaemon(true);
            return t;
        });
    }

    public synchronized void event(String message) {
        String line = System.currentTimeMillis() + ";" + message;
        System.out.println(line);
        try {
            Files.writeString(path, line + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("Não foi possível escrever " + path, e);
        }
    }

    public void start(HeartbeatMonitor[] monitors) {
        timer.scheduleAtFixedRate(() -> {
            try {
                for (HeartbeatMonitor m : monitors) if (m != null) m.check();
            } catch (RuntimeException e) {
                // Uma falha de escrita não deve cancelar as verificações seguintes.
                System.err.println("VERIFICAÇÃO FALHOU: " + e.getMessage());
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    @Override public void close() { timer.shutdownNow(); }
}
