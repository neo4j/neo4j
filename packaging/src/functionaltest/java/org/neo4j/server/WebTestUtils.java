package org.neo4j.server;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URI;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class WebTestUtils {

    private static boolean available(int port) {
        if (port < 1111 || port > 9999) {
            throw new IllegalArgumentException("Invalid start port: " + port);
        }

        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return false;
    }
    
    public static int nextAvailablePortNumber() {
        int nonPriveledgedPortNumber = 1111;
        while (!available(nonPriveledgedPortNumber)) {
            nonPriveledgedPortNumber++;
        }
        return nonPriveledgedPortNumber;
    }
    
    public static ClientResponse sendGetRequestTo(URI targetUri) {
        return Client.create().resource(targetUri).get(ClientResponse.class);
    }
}
