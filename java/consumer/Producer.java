package consumer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;

public class Producer {

    private static String TOPIC = "topic";

    private static ArrayList<String> groupIDs = new ArrayList<>();
    private static ArrayList<Integer> groupCounters = new ArrayList<>();
    private static ArrayList<TcpProducer> producers = new ArrayList<>();

    private static void runProducerService(final String HOSTNAME,
                                           final int SERVER_PORT) {
        try {
            // Try to setup listener for consumers..
            TcpConsumerListenerService consumerListenerService = new TcpConsumerListenerService();
            new Thread(consumerListenerService).start();

            // Allow the user to pull the plug
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutdown hook ran! Notifying master server and minions of shutdown and closing connections.\n");
                consumerListenerService.shutdown();
            }));

            // Try to connect to master..
            EasySocket master = new EasySocket(HOSTNAME, SERVER_PORT);
            System.out.println("Connected to host..\n");

            // Send master notice of availability
            master.writeString("producer\n" +
                    consumerListenerService.serverPort + "\n" +
                    TOPIC);

            // todo this won't happen, discuss this with Jacob
            // Get list of producers
            String[] listOfAddressesFromMaster = master.readString().split("\\r?\\n");

            // Respond OK
            master.writeString("producer says OK");
            master.closeStreams();

        } catch (IOException ex) {
            System.err.println("Failed setup initial connection to master.");
            System.exit(1);
        }

    }

    private static class TcpProducer implements Runnable {

        private final EasySocket producer;

        TcpProducer(EasySocket producer) {
            this.producer = producer;
        }

        public void shutdown() {
            producer.writeString("");
            producer.closeStreams();
        }

        @Override
        public void run() {
            try {
                while (producer.isConnected()) {
                    String stringFromConsumer = producer.readString();

                    if (stringFromConsumer.trim().isEmpty()) {
                        break;
                    }

                    System.out.println(stringFromConsumer);
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            } finally {
                System.out.println("Producer disconnected from consumer.");
                producer.closeStreams();
            }
        }
    }

    private static class TcpConsumerListenerService implements Runnable {

        private ServerSocket serverSocket;
        public int serverPort = 0;

        public void shutdown() {
            for (TcpProducer producer: producers) {
                producer.shutdown();
                System.out.println("SHUTDOWN minion");
            }
            try {

                System.out.println("SHUTDOWN master listening server");

                EasySocket master = new EasySocket("HOSTNAME", 4126);
                System.out.println("Connected to host..\n");

                // Send master notice of shutdown
                master.writeString("producer\n" +
                        "exit\n" +
                        TOPIC + "\n" +
                        serverPort + "\n");

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // todo put each producer on its own thread
        // todo setup group IDs for each producer
        // todo setup group counters for each producer

        @Override
        public void run() {
            try {
                // Setup the welcoming socket.
                ServerSocket serverSocket = new ServerSocket(0);
                serverPort = serverSocket.getLocalPort();

                // Notify the service has been setup successfully.
                System.out.println(
                        "Starting consumer listening service at: " + new Date() + '\n'
                                + "Consumer listening service connected to port: "
                                + serverPort);

                // Listen for master; handle master it until it's finished then wait again
                while (true) {
                    Socket consumer = serverSocket.accept();
                    System.out.println("Consumer detected. Attempting to setup thread.");

                    try {
                        // Thread for consumer setup successfully
                        System.out.println(
                                "Thread started for consumer:\n\t"
                                        + "IP Address: "
                                        + consumer.getInetAddress().getHostAddress());

                        DataOutputStream toConsumer = new DataOutputStream(consumer.getOutputStream());
                        toConsumer.flush();

                        // todo start sending data here!!! What am I supposed to send? Random data?
                        writeString(toConsumer, "consumer says OK");

                        toConsumer.close();

                    } catch (IOException ex) {
                        // Ignore because it's likely that master is lost.
                    } finally {
                        // Losing the master- perform cleanup.
                        System.out.println(
                                "Lost consumer:\n\t"
                                        + "IP Address: "
                                        + consumer.getInetAddress().getHostAddress()
                                        + "\nPerforming cleanup..");
                        System.out.println("Cleanup complete.");
                    }
                }
            } catch (IOException ex) {
                System.err.println("Failed to setup server socket to listen for consumers.");
            }
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
