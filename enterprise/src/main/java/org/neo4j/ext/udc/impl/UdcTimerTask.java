package org.neo4j.ext.udc.impl;


import java.io.IOException;
import java.util.TimerTask;

public class UdcTimerTask extends TimerTask {
    private String host;
    private String version;
    private String storeId;
    private Pinger pinger;

    public UdcTimerTask(String host, String version, String storeId)
    {

        this.host = host;
        this.version = version;
        this.storeId = storeId;
        pinger = new Pinger();
    }

    @Override
    public void run()
    {
        try {
            pinger.ping(host, storeId, version);
        } catch (IOException e) {}
    }
}
