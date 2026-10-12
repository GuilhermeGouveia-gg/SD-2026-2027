package rep01;

import java.io.IOException;
import java.util.Scanner;

public class PubSubLeader {
    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) throws IOException, InterruptedException {
        String api = args.length > 0 ? args[0] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub ipfs = new IpfsPubSub(api);
        RecordFile file = new RecordFile("lider.txt");
        long seq = file.lastSeq();
        try (Scanner sc = new Scanner(System.in)) {
            System.out.println("Líder PubSub pronto. Formato: <sensor> <temperatura> | 'rajada <n>' | 'sair'");
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;
                String[] p = line.split("\\s+");

                if (p[0].equalsIgnoreCase("rajada")) {
                    if (p.length != 2) {
                        System.out.println("Usa: rajada <número positivo>");
                        continue;
                    }
                    int n;
                    try {
                        n = Integer.parseInt(p[1]);
                    } catch (NumberFormatException e) {
                        System.out.println("Usa: rajada <número positivo>");
                        continue;
                    }
                    long primeiro = 0;
                    long ultimo = 0;
                    for (int i = 0; i < n; i++) {
                        double temp = Math.random() * 15 + 15;
                        SensorRecord r = SensorRecord.now(++seq, "SIM", temp);
                        file.append(r);
                        long now = System.currentTimeMillis();
                        if (i == 0) primeiro = now;
                        ultimo = now;
                        ipfs.publish(TOPIC, r.toLine());
                    }
                    System.out.println("Enviados: " + n);
                    System.out.println("Primeiro envio: " + primeiro);
                    System.out.println("Último envio: " + ultimo);
                    continue;
                }

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
                ipfs.publish(TOPIC, r.toLine());
                System.out.println("Registado e enviado: " + r.toLine());
            }
        }
    }
}
