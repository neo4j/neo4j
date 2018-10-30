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
package org.neo4j.bolt.v1.runtime;

import io.netty.channel.Channel;

import java.util.Collection;

import org.neo4j.logging.Log;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

import static java.util.Objects.requireNonNull;

/**
 * Queue monitor that changes {@link Channel} auto-read setting based on the job queue size.
 * Methods {@link #enqueued(Job)} and {@link #drained(Collection)} are synchronized to make sure
 * queue size and channel auto-read are modified together as an atomic operation.
 */
public class BoltChannelAutoReadLimiter implements BoltWorkerQueueMonitor
{
    protected static final String LOW_WATERMARK_NAME = "low_watermark";
    protected static final String HIGH_WATERMARK_NAME = "high_watermark";

    private final Channel channel;
    private final Log log;
    private final int lowWatermark;
    private final int highWatermark;

    private int queueSize;

    public BoltChannelAutoReadLimiter( Channel channel, Log log )
    {
        this( channel, log, FeatureToggles.getInteger( BoltChannelAutoReadLimiter.class, LOW_WATERMARK_NAME, 100 ),
                FeatureToggles.getInteger( BoltChannelAutoReadLimiter.class, HIGH_WATERMARK_NAME, 300 ) );
    }

    public BoltChannelAutoReadLimiter( Channel channel, Log log, int lowWatermark, int highWatermark )
    {
        if ( highWatermark <= 0 )
        {
            throw new IllegalArgumentException( "invalid highWatermark value" );
        }

        if ( lowWatermark < 0 || lowWatermark >= highWatermark )
        {
            throw new IllegalArgumentException( "invalid lowWatermark value" );
        }

        this.channel = requireNonNull( channel );
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
    public synchronized void enqueued( Job job )
    {
        queueSize += 1;
        checkLimitsOnEnqueue();
    }

    @Override
    public synchronized void dequeued( Job job )
    {
        queueSize -= 1;
        checkLimitsOnDequeue();
    }

    @Override
    public synchronized void drained( Collection<Job> jobs )
    {
        queueSize -= jobs.size();
        checkLimitsOnDequeue();
    }

    private void checkLimitsOnEnqueue()
    {
        if ( queueSize > highWatermark && channel.config().isAutoRead() )
        {
            if ( log != null )
            {
                log.warn( "Channel [%s]: client produced %d messages on the worker queue, auto-read is being disabled.", channel.id(), queueSize );
            }

            channel.config().setAutoRead( false );
        }
    }

    private void checkLimitsOnDequeue()
    {
        if ( queueSize <= lowWatermark && !channel.config().isAutoRead() )
        {
            if ( log != null )
            {
                log.warn( "Channel [%s]: consumed messages on the worker queue below %d, auto-read is being enabled.", channel.id(), queueSize );
            }

            channel.config().setAutoRead( true );
        }
    }

}
