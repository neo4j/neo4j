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

        Map<String,String> udcFields = new HashMap<String, String>();
        udcFields.put("id", storeId);
        udcFields.put("v", version);

        pinger = new Pinger(host, udcFields);
    }

    @Override
    public void run()
    {
        try {
            pinger.ping();
            incrementSuccessCount(storeId);
        } catch (IOException e) {
            // ABK: commenting out to not annoy people
            // System.err.println("UDC update to " + host + " failed, because: " + e);
            incrementFailureCount(storeId);
        }
    }

    private void incrementSuccessCount(String storeId) {
        Integer currentCount = successCounts.get(storeId);
        currentCount++;
        successCounts.put(storeId, currentCount);
    }

    private void incrementFailureCount(String storeId) {
        Integer currentCount = failureCounts.get(storeId);
        currentCount++;
        failureCounts.put(storeId, currentCount);
    }
}
