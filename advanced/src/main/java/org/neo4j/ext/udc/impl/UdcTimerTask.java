package org.neo4j.ext.udc.impl;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;

public class UdcTimerTask extends TimerTask {

    // ABKTODO: make this thread safe
    public static final Map<String, Integer> successCounts = new HashMap<String, Integer>();
    public static final Map<String, Integer> failureCounts = new HashMap<String, Integer>();
    
    private String host;
    private String version;
    private String storeId;
    private Pinger pinger;

    public UdcTimerTask(String host, String version, String storeId)
    {
        successCounts.put(storeId, 0);
        failureCounts.put(storeId, 0);

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
            incrementSuccessCount(storeId);
            System.out.println("UDC update to " + host + " worked!");
        } catch (IOException e) {
            System.err.println("UDC update to " + host + " failed, because: " + e);
            incrementFailureCount(storeId);
        }
    }

    private void incrementSuccessCount(String storeId) {
        Integer currentCount = successCounts.get(storeId);
        currentCount++;
        successCounts.put(storeId, currentCount);
        System.out.println("successful update to host " + host);
    }

    private void incrementFailureCount(String storeId) {
        Integer currentCount = failureCounts.get(storeId);
        currentCount++;
        failureCounts.put(storeId, currentCount);
        System.out.println("failure update to host " + host);
    }
}
