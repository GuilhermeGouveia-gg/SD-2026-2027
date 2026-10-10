package rep01;
import java.util.concurrent.ThreadLocalRandom;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class MulticastLeader {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        try (MulticastSocket socket = new MulticastSocket();
             Scanner sc = new Scanner(System.in)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.setTimeToLive(1);
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();
            System.out.println("Líder pronto. Formato: <sensor> <temperatura> | 'sair' para terminar");
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;
                String[] p = line.split("\\s+");

                //Rajada 1000
                if (p[0].equalsIgnoreCase("rajada")) {
                    int n;

                    try {
                        if (p.length != 2) throw new NumberFormatException();

                        n = Integer.parseInt(p[1]);

                        if (n <= 0) throw new NumberFormatException();
                    } catch (NumberFormatException e) {
                        System.out.println("Usa: rajada <número positivo>");
                        continue;
                    }

                    long primeiro = 0;
                    long ultimo = 0;

                    for (int i = 0; i < n; i++) {
                        double temp = ThreadLocalRandom.current().nextDouble(15, 30);
                        SensorRecord r = SensorRecord.now(++seq, "SIM", temp);

                        file.append(r);

                        byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);

                        long instante = System.currentTimeMillis();
                        socket.send(new DatagramPacket(m, m.length, group, PORT));

                        if (i == 0) primeiro = instante;
                        ultimo = instante;
                    }

                    System.out.println("Enviados: " + n);
                    System.out.println("Primeiro envio: " + primeiro);
                    System.out.println("Último envio: " + ultimo);

                    continue;
                }
                //

                if (p.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }
                SensorRecord r = SensorRecord.now(++seq, p[0], temp);
                file.append(r);
                byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                socket.send(new DatagramPacket(m, m.length, group, PORT));
                System.out.println("Registado e enviado: " + r.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}
