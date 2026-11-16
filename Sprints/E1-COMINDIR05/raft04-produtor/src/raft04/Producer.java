package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Locale;

public final class Producer {
    private final int n;
    private final HeartbeatMonitor[] monitors;
    private final Path producerFile;

    private final IpfsPubSub ipfs =
            new IpfsPubSub("http://127.0.0.1:5001/api/v0");

    public Producer(int n) throws IOException {
        this.n = n;
        this.monitors = new HeartbeatMonitor[n];
        this.producerFile = Paths.get("produtor.txt");
        Path producerDir = this.producerFile.toAbsolutePath().getParent();
        if (producerDir != null) {
            Files.createDirectories(producerDir);
        }
        if (Files.notExists(this.producerFile)) {
            Files.writeString(this.producerFile, "", StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }
        for (int i = 0; i < n; i++) {
            monitors[i] = new HeartbeatMonitor(i, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, s -> System.out.println(s));
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Uso: Producer <N>");
            return;
        }
        int n = Integer.parseInt(args[0]);
        Producer producer = new Producer(n);
        System.out.println("Comandos: <sensor> <temperatura> | p<particao> <sensor> <temperatura>");
        try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                String[] parts = trimmed.split("\\s+");
                try {
                    if (parts.length == 2) {
                        String sensor = parts[0];
                        double temperature = Double.parseDouble(parts[1]);
                        int partition = Math.floorMod(sensor.hashCode(), n);
                        producer.publish(partition, sensor, temperature);
                    } else if (parts.length == 3 && parts[0].startsWith("p")) {
                        int partition = Integer.parseInt(parts[0].substring(1));
                        String sensor = parts[1];
                        double temperature = Double.parseDouble(parts[2]);
                        producer.publish(partition, sensor, temperature);
                    } else {
                        System.err.println("Comando inválido: " + trimmed);
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Temperatura ou partição inválidas: " + trimmed);
                }
            }
        }
    }

    public void publish(int partition, String sensor, double temperature) {
        if (partition < 0 || partition >= n) {
            throw new IllegalArgumentException(
                    "Partição inválida: " + partition);
        }

        if (!Double.isFinite(temperature)) {
            throw new IllegalArgumentException("Temperatura inválida");
        }

        if (monitors[partition].state()
                == HeartbeatMonitor.State.SUSPEITO) {
            System.out.println(
                    "AVISO: partição p" + partition
                            + " sem líder conhecido; leitura "
                            + sensor + " pode perder-se"
            );
        }

        StringBuilder vector = new StringBuilder();

        for (int i = 0; i < n; i++) {
            if (i > 0) {
                vector.append(',');
            }
            vector.append('0');
        }

        String message = Wire.in(
                sensor,
                temperature,
                0L,
                vector.toString()
        );

        try {
            ipfs.publish(Wire.topicIn(partition), message);

            System.out.println(
                    "ENVIADO;particao=p" + partition + ";" + message
            );

            appendProducerLog(
                    partition, sensor, temperature, message
            );
        } catch (IOException e) {
            System.err.println(
                    "Falha ao publicar no IPFS: " + e.getMessage()
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Publicação interrompida");
        }
    }

    public void onHeartbeat(int partition, long term, long leader, long hb) {
        monitors[partition].onHeartbeat(term, leader, hb);
    }

    private void appendProducerLog(int partition, String sensor, double temperature, String message) {
        try {
            Files.writeString(producerFile,
                    System.currentTimeMillis() + ";p" + partition + ";" + sensor + ";" + temperature + ";" + message + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException("Falha ao escrever produtor.txt", e);
        }
    }
}
