package rep01;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");

        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            long lastSeq = file.lastSeq();
            TreeMap<Long, SensorRecord> buffered = new TreeMap<>();

            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group);

            System.out.println("Réplica " + id + " à escuta em "
                    + GROUP + ":" + PORT);
            System.out.println("Última sequência aplicada: " + lastSeq);

            byte[] buffer = new byte[1000];

            int recebidos = 0;
            int escritos = 0;
            long primeiro = 0;
            long ultimo = 0;

            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                socket.receive(p);
                long instanteRececao = System.currentTimeMillis();

                String line = new String(
                        p.getData(),
                        p.getOffset(),
                        p.getLength(),
                        StandardCharsets.UTF_8
                );

                SensorRecord r;

                // Um registo inválido não termina a réplica.
                try {
                    r = SensorRecord.fromLine(line);
                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + line);
                    continue;
                }

                //Rajada 1000
                recebidos++;
                if (recebidos == 1) {
                    primeiro = instanteRececao;
                }
                ultimo = instanteRececao;
                //

                long seq = r.getSeq();

                // Já foi escrito ou já está em espera.
                if (seq <= lastSeq || buffered.containsKey(seq)) {
                    System.out.println("DUPLICADO: " + seq);
                    continue;
                }

                if (seq == lastSeq + 1) {
                    file.append(r);
                    escritos++;

                    lastSeq = seq;
                    System.out.println("Aplicado: " + r.toLine());

                    // Aplica os registos em espera que passaram a ser consecutivos.
                    while (buffered.containsKey(lastSeq + 1)) {
                        SensorRecord next = buffered.remove(lastSeq + 1);
                        file.append(next);
                        escritos++;

                        lastSeq = next.getSeq();

                        System.out.println(
                                "Aplicado em espera: " + next.toLine()
                        );
                    }
                } else {
                    buffered.put(seq, r);

                    System.out.println("EM ESPERA: " + seq
                            + " (falta " + (lastSeq + 1) + ")");
                }
                System.out.println(
                        "Recebidos: " + recebidos
                                + " | Escritos: " + escritos
                                + " | Em espera: " + buffered.size()
                                + " | Primeira receção: " + primeiro
                                + " | Última receção: " + ultimo
                );
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}
