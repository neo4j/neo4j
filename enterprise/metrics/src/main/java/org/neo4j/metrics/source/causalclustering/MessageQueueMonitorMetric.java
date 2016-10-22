/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.metrics.source.causalclustering;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.neo4j.causalclustering.messaging.monitoring.MessageQueueMonitor;

public class MessageQueueMonitorMetric implements MessageQueueMonitor
{
    private Map<String, LongAdder> droppedMessages = new TreeMap<>();
    private Map<String, AtomicLong> queueSize = new TreeMap<>();

    @Override
    public Long droppedMessages()
    {
        return droppedMessages.values().stream().mapToLong( LongAdder::longValue ).sum();
    }

    @Override
    public void droppedMessage( InetSocketAddress destination )
    {
        droppedMessages.get( destination.toString() ).increment();
    }

    @Override
    public void queueSize( InetSocketAddress destination, long size )
    {
        queueSize.get( destination.toString() ).set( size );
    }

    @Override
    public Long queueSizes()
    {
        return queueSize.values().stream().mapToLong( AtomicLong::get ).sum();
    }

    @Override
    public void register( InetSocketAddress destination )
    {
        if ( !droppedMessages.containsKey( destination.toString() ) )
        {
            droppedMessages.put( destination.toString(), new LongAdder() );
        }

        if ( !queueSize.containsKey( destination.getHostString() ) )
        {
            queueSize.put( destination.toString(), new AtomicLong() );
        }
    }
}
