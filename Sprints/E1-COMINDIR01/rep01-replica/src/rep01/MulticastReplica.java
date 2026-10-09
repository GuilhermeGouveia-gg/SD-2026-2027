package rep01;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group);
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT);
            byte[] buffer = new byte[1000];
            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                socket.receive(p);
                String line = new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8);
                SensorRecord r = SensorRecord.fromLine(line);
                file.append(r);
                System.out.println("Aplicado: " + r.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}
