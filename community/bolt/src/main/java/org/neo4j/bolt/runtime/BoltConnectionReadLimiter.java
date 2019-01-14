/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.bolt.v1.runtime.Job;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

/**
 * Queue monitor that changes {@link Channel} auto-read setting based on the job queue size.
 * Methods {@link #enqueued(BoltConnection, Job)} and {@link #drained(BoltConnection, Collection)} are synchronized to make sure
 * queue size and channel auto-read are modified together as an atomic operation.
 */
public class BoltConnectionReadLimiter implements BoltConnectionQueueMonitor
{
    private final Log log;
    private final int lowWatermark;
    private final int highWatermark;

    private int queueSize;

    public BoltConnectionReadLimiter( LogService logService, int lowWatermark, int highWatermark )
    {
        if ( highWatermark <= 0 )
        {
            throw new IllegalArgumentException( "invalid highWatermark value" );
        }

        if ( lowWatermark < 0 || lowWatermark >= highWatermark )
        {
            throw new IllegalArgumentException( "invalid lowWatermark value" );
        }

        this.log = logService.getInternalLog( getClass() );
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
    public synchronized void enqueued( BoltConnection to, Job job )
    {
        queueSize += 1;
        checkLimitsOnEnqueue( to );
    }

    @Override
    public synchronized void drained( BoltConnection from, Collection<Job> batch )
    {
        queueSize -= batch.size();
        checkLimitsOnDequeue( from );
    }

    private void checkLimitsOnEnqueue( BoltConnection connection )
    {
        Channel channel = connection.channel();

        if ( queueSize > highWatermark && channel.config().isAutoRead() )
        {
            if ( log != null )
            {
                log.warn( "Channel [%s]: client produced %d messages on the worker queue, auto-read is being disabled.", channel.remoteAddress(), queueSize );
            }

            channel.config().setAutoRead( false );
        }
    }

    private void checkLimitsOnDequeue( BoltConnection connection )
    {
        Channel channel = connection.channel();

        if ( queueSize <= lowWatermark && !channel.config().isAutoRead() )
        {
            if ( log != null )
            {
                log.warn( "Channel [%s]: consumed messages on the worker queue below %d, auto-read is being enabled.", channel.remoteAddress(), lowWatermark );
            }

            channel.config().setAutoRead( true );
        }
    }

}
