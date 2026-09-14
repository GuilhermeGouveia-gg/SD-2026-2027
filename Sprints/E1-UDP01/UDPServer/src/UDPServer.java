import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;

public class UDPServer {
    private static final int PORT = 6789;
    private static final int MAX_BUFFER_SIZE = 1000;

    public static void main(String[] args) {
        DatagramSocket socket = null;
        int[] lastAccepted = {0};
        byte[] buffer = new byte[MAX_BUFFER_SIZE];

        try {
            socket = new DatagramSocket(PORT);
            System.out.println("Servidor UDP em escuta na porta " + PORT);

            while (true) {
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                socket.receive(request);

                String payload = new String(request.getData(), 0, request.getLength(), StandardCharsets.UTF_8);
                String reply = processRequest(payload, lastAccepted);

                byte[] replyData = reply.getBytes(StandardCharsets.UTF_8);
                DatagramPacket response = new DatagramPacket(
                        replyData,
                        replyData.length,
                        request.getAddress(),
                        request.getPort()
                );
                socket.send(response);
            }
        } catch (SocketException e) {
            System.out.println("Socket: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (socket != null) {
                socket.close();
            }
        }
    }

    private static String processRequest(String payload, int[] lastAccepted) {
        String cleaned = payload == null ? "" : payload.trim();

        if (cleaned.isEmpty()) {
            return "waitingfor," + (lastAccepted[0] + 1);
        }

        int separator = cleaned.indexOf(',');
        if (separator <= 0 || separator == cleaned.length() - 1) {
            return "waitingfor," + (lastAccepted[0] + 1);
        }

        String sequenceText = cleaned.substring(0, separator).trim();
        String messageText = cleaned.substring(separator + 1).trim();

        int sequence;
        try {
            sequence = Integer.parseInt(sequenceText);
        } catch (NumberFormatException e) {
            return "waitingfor," + (lastAccepted[0] + 1);
        }

        if (sequence == lastAccepted[0] + 1) {
            lastAccepted[0] = sequence;
            return payload;
        }

        return "waitingfor," + (lastAccepted[0] + 1);
    }
}
