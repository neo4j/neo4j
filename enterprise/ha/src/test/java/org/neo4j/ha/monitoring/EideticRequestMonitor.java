/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
