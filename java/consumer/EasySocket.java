package consumer;

import java.io.*;
import java.io.DataInputStream;
import java.net.Socket;

public class EasySocket extends Socket {

    private final int BUFFER_SIZE = 1024;

    private DataInputStream fromServer;
    private DataOutputStream toServer;

    public EasySocket() {
        super();
    }

    public EasySocket(String host,
                      int port) throws IOException {
        super(host, port);
        setupStreams();
    }

    public EasySocket(String host,
                      int port,
                      String customName) throws IOException {
        this(host, port);
    }

    public void setFromServer(DataInputStream fromServer) {
        this.fromServer = fromServer;
    }

    public void setToServer(DataOutputStream toServer) {
        this.toServer = toServer;
    }

    public DataInputStream getFromServer() {
        return fromServer;
    }

    public DataOutputStream getToServer() {
        return toServer;
    }

    public String readString() throws IOException {
        String stringFromServer = "";
        try {
            byte[] bytes = new byte[BUFFER_SIZE];
            fromServer.read(bytes);
            stringFromServer = new String(bytes);
        } catch (IOException ex) {
            System.err.println("Failed to read bytes from host.");
        }
        return stringFromServer;
    }

    public void writeString(String string) {
        try {
            toServer.write(string.getBytes());
        } catch (IOException ex) {
            System.err.println("Failed to write string to host.");
        }
    }

    public void closeStreams() {
        try {
            toServer.close();
            fromServer.close();
        } catch (IOException ex) {
            System.err.println("Failed to close streams.");
        }
    }

    private void setupStreams() {
        try {
            toServer = new DataOutputStream(this.getOutputStream());
            toServer.flush();
            fromServer = new DataInputStream(this.getInputStream());
        } catch (IOException ex) {
            ex.printStackTrace();
            System.err.println("Failed to setup streams.");
        }
    }
}
