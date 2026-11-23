package tpf;

import java.nio.file.Path;

/** Ferramenta offline: abrir um chunk local sem implementar /recuperar antecipadamente. */
public final class InspecionarChunk {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("Uso: InspecionarChunk <pastaLider> <chunkId>");
        Path pasta = Path.of(args[0]);
        ChunkCrypto crypto = new ChunkCrypto(java.nio.file.Files.readAllBytes(pasta.resolve("master.key")));
        Chunk chunk = LocalArchive.abrir(pasta, args[1], crypto);
        for (Registo r : chunk.registos()) {
            System.out.println("LSN=" + r.lsn() + " instante=" + r.instante()
                    + " autor=" + r.autor() + " canal=" + r.canal() + " texto=" + r.texto());
        }
    }
}
