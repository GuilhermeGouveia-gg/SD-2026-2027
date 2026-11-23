package tpf;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Snapshot imutável dos registos; o contador permite distinguir dados de padding. */
public record Chunk(List<Registo> registos) {
    private static final int MAGIC = 0x54504631; // ASCII TPF1

    public Chunk {
        registos = List.copyOf(registos);
        if (registos.isEmpty()) throw new IllegalArgumentException("Chunk vazio");
    }

    public byte[] serializar(int padBytes) throws IOException {
        if (padBytes <= 0) throw new IllegalArgumentException("Padding inválido");
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buffer)) {
            out.writeInt(MAGIC);
            out.writeInt(registos.size());
            for (Registo r : registos) {
                out.writeLong(r.lsn());
                out.writeLong(r.instante());
                escreverString(out, r.autor());
                escreverString(out, r.canal());
                escreverString(out, r.texto());
            }
        }
        byte[] dados = buffer.toByteArray();
        int extra = (padBytes - dados.length % padBytes) % padBytes;
        return Arrays.copyOf(dados, Math.addExact(dados.length, extra));
    }

    public static Chunk ler(byte[] dados) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(dados))) {
            if (in.readInt() != MAGIC) throw new IOException("Assinatura de chunk inválida");
            int n = in.readInt();
            if (n <= 0 || n > in.available() / 28) throw new IOException("Contagem inválida");
            List<Registo> registos = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                registos.add(new Registo(in.readLong(), in.readLong(), lerString(in), lerString(in), lerString(in)));
            }
            // Só são permitidos zeros depois dos registos declarados.
            while (in.available() > 0) if (in.readByte() != 0) throw new IOException("Padding inválido");
            return new Chunk(registos);
        } catch (IllegalArgumentException e) {
            throw new IOException("Registo inválido no chunk", e);
        }
    }

    /** Tamanho antes do padding, incluindo o cabeçalho de oito bytes. */
    public static int tamanhoRegisto(Registo r) {
        return Math.addExact(28, Math.addExact(r.autor().getBytes(StandardCharsets.UTF_8).length,
                Math.addExact(r.canal().getBytes(StandardCharsets.UTF_8).length,
                        r.texto().getBytes(StandardCharsets.UTF_8).length)));
    }

    private static void escreverString(DataOutputStream out, String valor) throws IOException {
        byte[] bytes = valor.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String lerString(DataInputStream in) throws IOException {
        int tamanho = in.readInt();
        if (tamanho < 0 || tamanho > in.available()) throw new IOException("Comprimento inválido");
        return new String(in.readNBytes(tamanho), StandardCharsets.UTF_8);
    }
}
