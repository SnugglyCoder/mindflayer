package consumer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;


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

    private static void spawnMinions(String[] producerAddresses) {
        for (String address: producerAddresses) {
            if (address.trim().isEmpty()) {
                break;
            }

            System.out.println("Received producer address from master: " + address);

            // format is ip:port
            int portSeparator = address.indexOf(":");
            String producerHostName = address.substring(0, portSeparator);
            int producerPort = Integer.parseInt(address.substring(portSeparator + 1));

            try {
                // Start minion thread for consumer
                EasySocket minion = new EasySocket(producerHostName, producerPort);
                System.out.println("Minion connected to producer with address: " + address);
                new Thread(new TcpMinion(minion)).start();
            } catch (IOException ex) {
                System.err.println("Minion failed to connect to producer with address: " + address);
            }
        }
    }

    private static void runReceiverService(final String HOSTNAME,
                                           final int SERVER_PORT) {
        try {
            // Try to setup listener for master..
            TcpMasterService tcpMasterService = new TcpMasterService();
            new Thread(tcpMasterService).start();

            // Try to connect to master..
            EasySocket master = new EasySocket(HOSTNAME, SERVER_PORT);
            System.out.println("Connected to host..\n");

            // Send master notice of availability
            master.writeString("consumer\n" + tcpMasterService.getPort());

            // Get list of producers
            String[] listOfAddressesFromMaster = master.readString().split("\\r?\\n");

            // Respond OK
            master.writeString("consumer says OK");
            master.closeStreams();

            // Create a minion for each producer address
            spawnMinions(listOfAddressesFromMaster);

        } catch (IOException ex) {
            System.err.println("Failed setup initial connection to master.");
            System.exit(1);
        }

    }

    // todo double check is static ok here?
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
                System.out.println("Minion disconnected from producer.");
                minion.closeStreams();
            }
        }
    }

    public static class TcpMasterService implements Runnable {

        private ServerSocket serverSocket;

        @Override
        public void run() {
            try {
                // Setup the welcoming socket.
                ServerSocket serverSocket = new ServerSocket(0);

                // Notify the service has been setup successfully.
                System.out.println(
                        "Starting master server listening service at: " + new Date() + '\n'
                                + "Master listening service connected to port: "
                                + serverSocket.getLocalPort());

                // Listen for master; handle master it until it's finished then wait again
                while (true) {
                    Socket socket = serverSocket.accept();
                    EasySocket master = new EasySocket(socket.getInetAddress().getHostAddress(), socket.getPort());
                    System.out.println("Master detected. Attempting to setup thread.");

                    try {
                        // Thread for master setup successfully
                        System.out.println(
                                "Thread started for master:\n\t"
                                        + "IP Address: "
                                        + master.getInetAddress().getHostAddress()
                                        + " | Name: ");

                        String message= master.readString().trim();

                        if (message.equalsIgnoreCase("SHUTDOWN")) {
                            // todo make sure all open connections are closed gracefully
                            // This is a temporary brutal method
                            System.exit(0);
                        } else {
                            // Get list of producers
                            String[] listOfAddressesFromMaster = master.readString().split("\\r?\\n");

                            // Respond OK
                            master.writeString("consumer says OK");

                            // Create a minion for each producer address
                            spawnMinions(listOfAddressesFromMaster);
                        }

                    } catch (IOException ex) {
                        // Ignore because it's likely that master is lost.
                    } finally {
                        // Losing the master- perform cleanup.
                        System.out.println(
                                "Lost master:\n\t"
                                        + "IP Address: "
                                        + master.getInetAddress().getHostAddress()
                                        + "\nPerforming cleanup..");
                        master.closeStreams();
                        System.out.println("Cleanup complete.");
                    }
                }
            } catch (IOException ex) {
                System.err.println("Failed to setup server socket to listen for master.");
            }
        }

        int getPort() {
            if (serverSocket != null) {
                return serverSocket.getLocalPort();
            }
            return 0;
        }
    }

}
