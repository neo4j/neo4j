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

import java.net.SocketAddress;
import java.time.Clock;

import org.neo4j.bolt.v1.runtime.Job;

public class MetricsReportingBoltConnection implements BoltConnection
{
    private final BoltConnection delegate;
    private final BoltConnectionMetricsMonitor metricsMonitor;
    private Clock clock;

    public MetricsReportingBoltConnection( BoltConnection delegate, BoltConnectionMetricsMonitor metricsMonitor, Clock clock )
    {
        this.delegate = delegate;
        this.metricsMonitor = metricsMonitor;
        this.clock = clock;
    }

    @Override
    public String id()
    {
        return delegate.id();
    }

    @Override
    public SocketAddress localAddress()
    {
        return delegate.localAddress();
    }

    @Override
    public SocketAddress remoteAddress()
    {
        return delegate.remoteAddress();
    }

    @Override
    public Channel channel()
    {
        return delegate.channel();
    }

    @Override
    public String principal()
    {
        return delegate.principal();
    }

    @Override
    public boolean hasPendingJobs()
    {
        return delegate.hasPendingJobs();
    }

    @Override
    public void start()
    {
        delegate.start();
        metricsMonitor.connectionOpened();
    }

    @Override
    public void enqueue( Job job )
    {
        metricsMonitor.messageReceived();
        long queuedAt = clock.millis();
        delegate.enqueue( machine ->
        {
            long queueTime = clock.millis() - queuedAt;
            metricsMonitor.messageProcessingStarted( queueTime );
            try
            {
                job.perform( machine );
                metricsMonitor.messageProcessingCompleted( clock.millis() - queuedAt - queueTime );
            }
            catch ( Throwable t )
            {
                metricsMonitor.messageProcessingFailed();
                throw t;
            }
       } );
    }

    @Override
    public boolean processNextBatch()
    {
        metricsMonitor.connectionActivated();

        try
        {
            boolean continueProcessing = delegate.processNextBatch();

            if ( !continueProcessing )
            {
                metricsMonitor.connectionClosed();
            }

            return continueProcessing;
        }
        finally
        {
            metricsMonitor.connectionWaiting();
        }
    }

    @Override
    public void interrupt()
    {
        delegate.interrupt();
    }

    @Override
    public void stop()
    {
        delegate.stop();
    }

}
