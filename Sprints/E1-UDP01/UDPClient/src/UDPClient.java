import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

public class UDPClient {
    private static final int SERVER_PORT = 6789;
    private static final int BUFFER_SIZE = 1000;
    private static final String EXIT_KEYWORD = "sair";

    public static void main(String[] args) {
        try (DatagramSocket socket = new DatagramSocket();
             BufferedReader input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {

            String mode = args.length > 0 ? args[0].toLowerCase(Locale.ROOT) : promptMode(input);
            if (!"auto".equals(mode) && !"manual".equals(mode)) {
                mode = "auto";
            }

            int nextSequence = 1;
            while (true) {
                String message;
                int sequence;

                if ("manual".equals(mode)) {
                    System.out.print("Número de sequência (ou 'auto' para mudar): ");
                    String sequenceText = input.readLine();
                    if (sequenceText == null) {
                        break;
                    }
                    String normalizedSequence = sequenceText.trim();
                    if (normalizedSequence.equalsIgnoreCase("auto")) {
                        mode = "auto";
                        System.out.println("Modo actual: auto");
                        continue;
                    }
                    if (isExitCommand(normalizedSequence)) {
                        break;
                    }
                    try {
                        sequence = Integer.parseInt(normalizedSequence);
                    } catch (NumberFormatException e) {
                        System.out.println("Número inválido. Tenta de novo.");
                        continue;
                    }

                    System.out.print("Mensagem: ");
                    message = input.readLine();
                    if (message == null) {
                        break;
                    }
                    if (isExitCommand(message.trim())) {
                        break;
                    }
                } else {
                    System.out.print("Mensagem (ou 'manual' para trocar de modo, 'sair' para terminar): ");
                    message = input.readLine();
                    if (message == null) {
                        break;
                    }
                    String trimmed = message.trim();
                    if (trimmed.isEmpty()) {
                        continue;
                    }
                    if (trimmed.equalsIgnoreCase("manual") || trimmed.equalsIgnoreCase("toggle") || trimmed.equalsIgnoreCase("mudar")) {
                        mode = "manual";
                        System.out.println("Modo actual: manual");
                        continue;
                    }
                    if (isExitCommand(trimmed)) {
                        break;
                    }
                    sequence = nextSequence;
                    nextSequence++;
                }

                String request = sequence + "," + message.trim();
                String response = sendMessage(socket, request);
                printResponse(response);
            }
        } catch (SocketException e) {
            System.out.println("Socket: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }

    private static String promptMode(BufferedReader input) throws IOException {
        System.out.print("Modo de numeração (auto/manual): ");
        String mode = input.readLine();
        if (mode == null) {
            return "auto";
        }
        String normalized = mode.trim().toLowerCase(Locale.ROOT);
        if ("manual".equals(normalized) || "auto".equals(normalized)) {
            return normalized;
        }
        return "auto";
    }

    private static boolean isExitCommand(String s) {
        return s.equalsIgnoreCase(EXIT_KEYWORD)
                || s.equalsIgnoreCase("quit")
                || s.equalsIgnoreCase("fim");
    }

    private static String sendMessage(DatagramSocket socket, String message) throws IOException {
        InetAddress serverAddress = InetAddress.getByName("localhost");
        byte[] requestData = message.getBytes(StandardCharsets.UTF_8);

        DatagramPacket request = new DatagramPacket(
                requestData,
                requestData.length,
                serverAddress,
                SERVER_PORT
        );
        socket.send(request);

        byte[] replyBuffer = new byte[BUFFER_SIZE];
        DatagramPacket reply = new DatagramPacket(replyBuffer, replyBuffer.length);
        socket.receive(reply);

        return new String(reply.getData(), 0, reply.getLength(), StandardCharsets.UTF_8);
    }

    private static void printResponse(String response) {
        if (response.startsWith("waitingfor,")) {
            String expected = response.substring("waitingfor,".length()).trim();
            System.out.println("Resposta do servidor: waitingfor " + expected);
        } else {
            System.out.println("Resposta do servidor: " + response);
        }
    }
}
