package org.neo4j.ext.udc.org.neo4j.ext.udc.impl;

import java.io.*;
import java.net.*;

public class Pinger {
    public void ping(String address, String storeId, String kernelVersion) throws IOException {
        String uri = "http://" + address + "/" + "?id=" + storeId + "+v=" + kernelVersion;
        URL url = new URL(uri);
        URLConnection con = url.openConnection();

        con.setDoInput(true);
        con.setDoOutput(false);
        con.setUseCaches(false);
        con.connect();

        con.getInputStream();
    }
}