package consumer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;


public class Consumer {

    private static String HOSTNAME = "";

    private static int SERVER_PORT = -1;

    private static String GROUP_ID = "";

    private static String TOPIC = "topic";

    private static ArrayList<TcpMinion> minions = new ArrayList<>();

    /**
     * @param args [server-ip] [server-port] [group-id]
     */
    public static void main(String[] args) {

        if (args.length < 3) {
            System.out.println("Usage: java -jar consumer.jar [server-ip] [server-port] [group-id]");
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

        GROUP_ID = args[2];

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
                EasySocket minionEasySocket = new EasySocket(producerHostName, producerPort);
                System.out.println("Minion connected to producer with address: " + address);
                minionEasySocket.writeString(GROUP_ID);

                TcpMinion minion = new TcpMinion(minionEasySocket);
                minions.add(minion);
                new Thread(minion).start();
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

            // Allow the user to pull the plug
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutdown hook ran! Notifying master server and minions of shutdown and closing connections.\n");
                tcpMasterService.shutdown();
            }));

            // Try to connect to master..
            EasySocket master = new EasySocket(HOSTNAME, SERVER_PORT);
            System.out.println("Connected to host..\n");

            // Send master notice of availability
            master.writeString("consumer\n" + tcpMasterService.serverPort);

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

    private static class TcpMinion implements Runnable {

        private final EasySocket minion;

        TcpMinion(EasySocket minion) {
            this.minion = minion;
        }

        public void shutdown() {
            minion.writeString("");
            minion.closeStreams();
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
        public int serverPort = 0;

        public void shutdown() {
            for (TcpMinion minion: minions) {
                // Send minion notification here if needed
                minion.shutdown();
                System.out.println("SHUTDOWN minion");
            }
            try {
                // Send server notification here if needed
                if (serverSocket != null) {
                    serverSocket.close();
                }
                System.out.println("SHUTDOWN master listening server");

                EasySocket master = new EasySocket(HOSTNAME, SERVER_PORT);
                System.out.println("Connected to host..\n");

                // Send master notice of shutdown
                master.writeString("consumer\n" +
                                   "exit\n" +
                                   TOPIC + "\n" +
                                   serverPort + "\n");

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try {
                // Setup the welcoming socket.
                ServerSocket serverSocket = new ServerSocket(0);
                serverPort = serverSocket.getLocalPort();

                // Notify the service has been setup successfully.
                System.out.println(
                        "Starting master server listening service at: " + new Date() + '\n'
                                + "Master listening service connected to port: "
                                + serverPort);

                // Listen for master; handle master it until it's finished then wait again
                while (true) {
                    Socket master = serverSocket.accept();
                    System.out.println("Master detected. Attempting to setup thread.");

                    try {
                        // Thread for master setup successfully
                        System.out.println(
                                "Thread started for master:\n\t"
                                        + "IP Address: "
                                        + master.getInetAddress().getHostAddress());

                        DataOutputStream toServer = new DataOutputStream(master.getOutputStream());
                        toServer.flush();
                        DataInputStream fromServer = new DataInputStream(master.getInputStream());

                        String message = readString(fromServer).trim();

                        if (message.equalsIgnoreCase("SHUTDOWN")) {
                            // todo make sure all open connections are closed gracefully
                            // This is a temporary brutal method
                            System.exit(0);
                        } else {
                            // Get list of producers
                            String[] listOfAddressesFromMaster = message.split("\\r?\\n");

                            // Respond OK
                            writeString(toServer, "consumer says OK");

                            for (String address: listOfAddressesFromMaster) {
                                System.out.println(address);
                            }

                            // Create a minion for each producer address
                            spawnMinions(listOfAddressesFromMaster);

                            toServer.close();
                            fromServer.close();
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
                        System.out.println("Cleanup complete.");
                    }
                }
            } catch (IOException ex) {
                System.err.println("Failed to setup server socket to listen for master.");
            }
        }

        public String readString(DataInputStream fromServer) throws IOException {
            String stringFromServer = "";
            try {
                byte[] bytes = new byte[1024];
                fromServer.read(bytes);
                stringFromServer = new String(bytes);
                System.out.println(stringFromServer);
            } catch (IOException ex) {
                System.err.println("Failed to read bytes from host.");
            }
            return stringFromServer;
        }

        public void writeString(DataOutputStream toServer, String string) {
            try {
                toServer.write(string.getBytes());
            } catch (IOException ex) {
                System.err.println("Failed to write string to host.");
            }
        }

    }

}
