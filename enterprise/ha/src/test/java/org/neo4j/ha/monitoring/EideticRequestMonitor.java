/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.monitor.RequestMonitor;

/**
 * Write operations are guarded by synchronization, read operations are not, since they are expected to take place
 * after the object is no longer alive.
 */
public class EideticRequestMonitor implements RequestMonitor
{
    private final AtomicInteger startedRequests = new AtomicInteger( 0 );
    private final AtomicInteger endedRequests = new AtomicInteger( 0 );

    @Override
    public void beginRequest( SocketAddress remoteAddress, RequestType<?> requestType, RequestContext requestContext )
    {
        startedRequests.incrementAndGet();
    }

    @Override
    public void endRequest( Throwable t )
    {
        endedRequests.incrementAndGet();
    }

    public int getStartedRequests()
    {
        return startedRequests.get();
    }

    public int getEndedRequests()
    {
        return endedRequests.get();
    }
}
