package org.neo4j.ext.udc.impl;


import java.io.IOException;
import java.util.*;

public class UdcTimerTask extends TimerTask {

    // ABKTODO: make this thread safe
    public static final Map<String, Integer> successCounts = new HashMap<String, Integer>();
    public static final Map<String, Integer> failureCounts = new HashMap<String, Integer>();
    
    private String storeId;
    private Pinger pinger;

    public UdcTimerTask(String host, String version, String storeId)
    {
        successCounts.put(storeId, 0);
        failureCounts.put(storeId, 0);

        this.storeId = storeId;

        Map<String,String> udcFields = new HashMap<String, String>();
        udcFields.put("id", storeId);
        udcFields.put("v", version);

        pinger = new Pinger(host, mergeSystemPropertiesWith(udcFields));
    }

    private Map<String, String> mergeSystemPropertiesWith(Map<String, String> udcFields) {
        Map<String, String> mergedMap = new HashMap<String, String>();
        mergedMap.putAll(udcFields);
        Properties sysProps = System.getProperties();
        Enumeration sysPropsNames = sysProps.propertyNames();
        while (sysPropsNames.hasMoreElements()) {
            String sysPropName = (String) sysPropsNames.nextElement();
            if (sysPropName.startsWith("neo4j.ext.udc")) {
                mergedMap.put(sysPropName.substring("neo4j.ext.udc".length()+1), sysProps.get(sysPropName).toString());
            }
        }
        return mergedMap;
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
