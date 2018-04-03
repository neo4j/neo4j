/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.bolt.runtime;

import io.netty.channel.Channel;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.bolt.v1.runtime.Job;
import org.neo4j.logging.Log;

public class BoltConnectionReadLimiter implements BoltConnectionQueueMonitor
{
    private final ConcurrentHashMap<String, AtomicInteger> counters = new ConcurrentHashMap<>();
    private final Log log;
    private final int lowWatermark;
    private final int highWatermark;

    public BoltConnectionReadLimiter( Log log, int lowWatermark, int highWatermark )
    {
        if ( highWatermark <= 0 )
        {
            throw new IllegalArgumentException( "invalid highWatermark value" );
        }

        if ( lowWatermark < 0 || lowWatermark >= highWatermark )
        {
            throw new IllegalArgumentException( "invalid lowWatermark value" );
        }

        this.log = log;
        this.lowWatermark = lowWatermark;
        this.highWatermark = highWatermark;
    }

    protected int getLowWatermark()
    {
        return lowWatermark;
    }

    protected int getHighWatermark()
    {
        return highWatermark;
    }

    @Override
    public void enqueued( BoltConnection to, Job job )
    {
        checkLimitsOnEnqueue( to, counters.computeIfAbsent( to.id(), k -> new AtomicInteger( 0 ) ).incrementAndGet() );
    }

    @Override
    public void drained( BoltConnection from, Collection<Job> batch )
    {
        checkLimitsOnDequeue( from, counters.computeIfAbsent( from.id(), k -> new AtomicInteger( 0 ) ).addAndGet( -batch.size() ) );
    }

    private void checkLimitsOnEnqueue( BoltConnection connection, int currentSize )
    {
        Channel channel = connection.channel();

        if ( currentSize > highWatermark && channel.config().isAutoRead() )
        {
            if ( log != null )
            {
                log.warn( "Channel [%s]: client produced %d messages on the worker queue, auto-read is being disabled.", channel.remoteAddress(), currentSize );
            }

            channel.config().setAutoRead( false );
        }
    }

    private void checkLimitsOnDequeue( BoltConnection connection, int currentSize )
    {
        Channel channel = connection.channel();

        if ( currentSize <= lowWatermark && !channel.config().isAutoRead() )
        {
            if ( log != null )
            {
                log.warn( "Channel [%s]: consumed messages on the worker queue below %d, auto-read is being enabled.", channel.remoteAddress(), lowWatermark );
            }

            channel.config().setAutoRead( true );
        }
    }

}
