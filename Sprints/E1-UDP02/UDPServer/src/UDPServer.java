import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

public class UDPServer {
    private static final int PORT = 6789;
    private static final int MAX_DATAGRAM_SIZE = 1000;

    /*
     * A lista de receção contém apenas mensagens já entregues, pela ordem certa.
     * ArrayList é adequada porque a operação principal é acrescentar no fim.
     */
    private static final ArrayList<String> receivedMessages = new ArrayList<>();

    /*
     * O HashMap guarda mensagens que chegaram antes da sua vez.
     * A chave é o número de sequência e o valor é o texto da mensagem.
     */
    private static final HashMap<Integer, String> waitingMessages = new HashMap<>();

    // Mensagens entregues durante o processamento do datagrama atual.
    private static final ArrayList<String> deliveredThisStep = new ArrayList<>();

    // L: número da última mensagem entregue por ordem.
    private static int lastMessageInOrder = 0;

    public static void main(String[] args) {
        try (DatagramSocket socket = new DatagramSocket(PORT)) {
            byte[] buffer = new byte[MAX_DATAGRAM_SIZE];
            System.out.println("Servidor UDP em escuta na porta " + PORT);

            while (true) {
                // Cada chamada a receive recebe exatamente um datagrama UDP.
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                socket.receive(request);

                String payload = new String(
                        request.getData(),
                        0,
                        request.getLength(),
                        StandardCharsets.UTF_8
                );

                String reply = processRequest(payload);
                byte[] replyData = reply.getBytes(StandardCharsets.UTF_8);

                // A resposta é enviada para o endereço e porta do cliente que escreveu.
                DatagramPacket response = new DatagramPacket(
                        replyData,
                        replyData.length,
                        request.getAddress(),
                        request.getPort()
                );
                socket.send(response);
            }
        } catch (SocketException e) {
            System.out.println("Erro no socket: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("Erro de comunicação: " + e.getMessage());
        }
    }

    private static String processRequest(String payload) {
        deliveredThisStep.clear();
        String cleaned = payload == null ? "" : payload.trim();

        // Uma mensagem válida tem o formato: numero,mensagem
        int separator = cleaned.indexOf(',');
        if (separator <= 0 || separator == cleaned.length() - 1) {
            printState(cleaned);
            return waitingReply();
        }

        int sequence;
        try {
            sequence = Integer.parseInt(cleaned.substring(0, separator).trim());
        } catch (NumberFormatException e) {
            printState(cleaned);
            return waitingReply();
        }

        String message = cleaned.substring(separator + 1).trim();
        if (message.isEmpty()) {
            printState(cleaned);
            return waitingReply();
        }

        // Guardamos o L anterior para saber se este datagrama foi entregue.
        int previousLastMessage = lastMessageInOrder;
        lastMessageInOrder = processDeliveredMessages(
                lastMessageInOrder,
                sequence,
                message
        );

        printState(cleaned);

        // Se L avançou, mantém-se o comportamento de echo.
        if (lastMessageInOrder > previousLastMessage) {
            return payload;
        }
        return waitingReply();
    }

    /**
     * Processa uma mensagem recebida.
     *
     * Se for a próxima mensagem esperada, entrega-a e entrega também, em cascata,
     * todas as mensagens consecutivas que já estejam na estrutura temporária.
     * Se vier adiantada, guarda-a até que chegue a mensagem em falta.
     * Duplicados não são guardados nem entregues novamente.
     *
     * @return o número da última mensagem processada por ordem
     */
    public static int processDeliveredMessages(
            int nLastMessageInOrder,
            int nCurrentMessage,
            String currentMessage) {

        // Mensagem adiantada: fica guardada, mas L não muda.
        if (nCurrentMessage > nLastMessageInOrder + 1) {
            // putIfAbsent evita que um duplicado substitua a primeira cópia.
            waitingMessages.putIfAbsent(nCurrentMessage, currentMessage);
            return nLastMessageInOrder;
        }

        // Mensagem repetida ou atrasada: ignora-a e mantém L.
        if (nCurrentMessage <= nLastMessageInOrder) {
            return nLastMessageInOrder;
        }

        // Chegou a próxima mensagem esperada.
        int lastDelivered = nCurrentMessage;
        deliverMessage(lastDelivered, currentMessage);

        // Entrega em cascata enquanto existir a mensagem seguinte no HashMap.
        while (waitingMessages.containsKey(lastDelivered + 1)) {
            int nextSequence = lastDelivered + 1;
            String nextMessage = waitingMessages.remove(nextSequence);
            deliverMessage(nextSequence, nextMessage);
            lastDelivered = nextSequence;
        }

        return lastDelivered;
    }

    private static void deliverMessage(int sequence, String message) {
        String deliveredMessage = sequence + "," + message;
        receivedMessages.add(deliveredMessage);
        deliveredThisStep.add(deliveredMessage);
    }

    private static String waitingReply() {
        return "waitingfor," + (lastMessageInOrder + 1);
    }

    // Informação exigida pelo enunciado para demonstrar cada passo do algoritmo.
    private static void printState(String receivedPayload) {
        String shownPayload = receivedPayload.isEmpty() ? "<vazia>" : receivedPayload;

        System.out.println();
        System.out.println("Datagrama recebido: " + shownPayload);
        System.out.println("L = " + lastMessageInOrder);
        System.out.println("Estrutura temporária = " + waitingMessages);
        System.out.println("Mensagens entregues neste passo = " + deliveredThisStep);
        System.out.println("Lista de receção = " + receivedMessages);
    }
}
