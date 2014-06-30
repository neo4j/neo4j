/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ha.monitoring;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.com.monitor.RequestMonitor;

public class EideticRequestMonitor implements RequestMonitor
{
    private final List<Map<String, String>> requests = new LinkedList<Map<String, String>>();
    private int requestsEnded;

    @Override
    public synchronized void beginRequest( Map<String, String> requestContext )
    {
        requests.add( requestContext );
    }

    @Override
    public synchronized void endRequest( Throwable t )
    {
        requestsEnded++;
        if (t != null)
            t.printStackTrace();
    }

    public synchronized List<Map<String, String>> getRequests()
    {
        return requests;
    }

    public synchronized int getRequestsEnded()
    {
        return requestsEnded;
    }
}
