package consumer;

import java.io.IOException;


public class Consumer {

    private static String HOSTNAME = "";

    private static int SERVER_PORT = -1;

    /**
     * @param args [server-ip] [server-port]
     */
    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("Usage: java -jar consumer.jar [server-ip] [server-port]");
            System.exit(0);
        }

        HOSTNAME = args[0];
        if (!isHostnameValid(HOSTNAME)) {
            System.err.println("Error: '" + HOSTNAME + "' is not a valid hostname.");
            System.exit(1);
        }

        SERVER_PORT = Integer.parseInt(args[1]);
        if (!isPortNumberValid(SERVER_PORT)) {
            System.err.println("Error: " + SERVER_PORT + " is not a valid server port number.");
            System.exit(1);
        }

        runReceiverService(HOSTNAME, SERVER_PORT);
    }

    private static boolean isHostnameValid(String hostname) {
        return (!hostname.isEmpty());
    }

    private static boolean isPortNumberValid(int port) {
        // Port numbers range from 0 to 65535, but port numbers 0 to 1023 are
        // reserved for privileged services and designated as well-known ports.
        return (port >= 0 && port <= 65535);
    }

    private static void runReceiverService(final String HOSTNAME,
                                           final int SERVER_PORT) {
        try {
            // Try to connect to server..
            EasySocket master = new EasySocket(HOSTNAME, SERVER_PORT);
            System.out.println("Connected to host..\n");

            // Send master notice of availability
            // todo port number needs to be dynamic
            master.writeString("consumer\n7070");

            // Get list of producers
            String[] listOfAddressesFromMaster = master.readString().split("\\r?\\n");

            // Respond OK
            master.writeString("consumer says OK");
            master.closeStreams();

            // todo Start up 'sever' part of consumer

            // Create a minion for each producer address
            for (String address: listOfAddressesFromMaster) {
                if (address.trim().isEmpty()) {
                    break;
                }

                System.out.println("Received producer address from master: " + address);

                // format is ip:port
                int portSeparator = address.indexOf(":");
                String producerHostName = address.substring(0, portSeparator);
                int producerPort = Integer.parseInt(address.substring(portSeparator + 1));

                // Start minion thread for consumer
                EasySocket minion = new EasySocket(producerHostName, producerPort);
                System.out.println("Minion is connected has connected to producer with address: " + address);
                new Thread(new TcpMinion(minion)).start();
            }

        } catch (IOException ex) {
            System.err.println("Failed to connect to host.");
            System.exit(1);
        }
    }


    private static class TcpMinion implements Runnable {

        private final EasySocket minion;

        TcpMinion(EasySocket minion) {
            this.minion = minion;
        }

        @Override
        public void run() {
            try {
                while (minion.isConnected()) {
                    String stringFromConsumer = minion.readString();

                    if (stringFromConsumer.trim().isEmpty()) {
                        break;
                    }

                    System.out.println(stringFromConsumer);
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            } finally {
                System.out.println("Minion disconnected from host.");
                minion.closeStreams();
            }
        }
    }
}
