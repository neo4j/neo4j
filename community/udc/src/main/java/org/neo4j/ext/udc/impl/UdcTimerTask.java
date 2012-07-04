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


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;

public class UdcTimerTask extends TimerTask
{

    // ABKTODO: make this thread safe
    public static final Map<String, Integer> successCounts = new HashMap<String, Integer>();
    public static final Map<String, Integer> failureCounts = new HashMap<String, Integer>();

    private final String storeId;
    private final Pinger pinger;

    public UdcTimerTask(String host, String storeId, boolean crashPing, Map<String, String> params)
    {
        successCounts.put( storeId, 0 );
        failureCounts.put( storeId, 0 );

        this.storeId = storeId;

        pinger = new Pinger( host, params, crashPing );
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
