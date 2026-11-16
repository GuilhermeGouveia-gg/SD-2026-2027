package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class RecordFile {
    private final Path path;

    public RecordFile(Path path) {
        this.path = path;
    }

    public synchronized void append(SensorRecord record) throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());
        Files.writeString(path, record.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public synchronized List<SensorRecord> readAll() throws IOException {
        if (!Files.exists(path)) {
            return new ArrayList<>();
        }
        List<SensorRecord> out = new ArrayList<>();
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                out.add(SensorRecord.fromLine(line));
            }
        }
        return out;
    }
}
