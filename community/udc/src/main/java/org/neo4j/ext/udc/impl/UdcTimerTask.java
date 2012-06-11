/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ext.udc.impl;


import org.neo4j.ext.udc.Edition;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimerTask;

import static org.neo4j.ext.udc.UdcConstants.*;

public class UdcTimerTask extends TimerTask
{

    // ABKTODO: make this thread safe
    public static final Map<String, Integer> successCounts = new HashMap<String, Integer>();
    public static final Map<String, Integer> failureCounts = new HashMap<String, Integer>();

    private final String storeId;
    private final Pinger pinger;

    public UdcTimerTask(String host, String version, String revision, String storeId, String source, boolean crashPing, String registration, String mac, String tags, Edition edition, Integer clusterNameHash)
    {
        successCounts.put( storeId, 0 );
        failureCounts.put( storeId, 0 );

        this.storeId = storeId;

        Map<String, String> udcFields = new HashMap<String, String>();

        add(udcFields, ID, storeId);
        add(udcFields, VERSION, version);
        add(udcFields, REVISION, revision);

        add(udcFields, EDITION, edition);
        add(udcFields, TAGS, tags);
        add(udcFields, CLUSTER_HASH, clusterNameHash);
        add(udcFields, SOURCE, source);
        add(udcFields, REGISTRATION, registration);
        add(udcFields, MAC, mac);

        Map<String, String> params = mergeSystemPropertiesWith( udcFields );

        pinger = new Pinger( host, params, crashPing );
    }

    private void add(Map<String, String> udcFields, String name, Object value) {
        if ( value == null ) return;
        String str = value.toString().trim();
        if (str.isEmpty()) return;
        udcFields.put(name, str);
    }

    private Map<String, String> mergeSystemPropertiesWith( Map<String, String> udcFields )
    {
        Map<String, String> mergedMap = new HashMap<String, String>();
        mergedMap.putAll( udcFields );
        Properties sysProps = System.getProperties();
        Enumeration sysPropsNames = sysProps.propertyNames();
        while ( sysPropsNames.hasMoreElements() )
        {
            String sysPropName = (String)sysPropsNames.nextElement();
            if ( sysPropName.startsWith( "neo4j.ext.udc" ) )
            {
                mergedMap.put( sysPropName.substring( "neo4j.ext.udc".length() + 1 ), sysProps.get( sysPropName ).toString() );
            }
        }
        return mergedMap;
    }

    @Override
    public void run()
    {
        try
        {
            pinger.ping();
            incrementSuccessCount( storeId );
        } catch ( IOException e )
        {
            // ABK: commenting out to not annoy people
            // System.err.println("UDC update to " + host + " failed, because: " + e);
            incrementFailureCount( storeId );
        }
    }

    private void incrementSuccessCount( String storeId )
    {
        Integer currentCount = successCounts.get( storeId );
        currentCount++;
        successCounts.put( storeId, currentCount );
    }

    private void incrementFailureCount( String storeId )
    {
        Integer currentCount = failureCounts.get( storeId );
        currentCount++;
        failureCounts.put( storeId, currentCount );
    }
}
