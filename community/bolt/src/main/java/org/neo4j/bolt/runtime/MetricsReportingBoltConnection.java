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

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.Job;
import org.neo4j.kernel.impl.logging.LogService;

public class MetricsReportingBoltConnection extends DefaultBoltConnection
{
    private final BoltConnectionMetricsMonitor metricsMonitor;
    private final Clock clock;

    MetricsReportingBoltConnection( BoltChannel channel, PackOutput output, BoltStateMachine machine, LogService logService,
            BoltConnectionLifetimeListener listener, BoltConnectionQueueMonitor queueMonitor, BoltConnectionMetricsMonitor metricsMonitor, Clock clock )
    {
        this( channel, output, machine, logService, listener, queueMonitor, DEFAULT_MAX_BATCH_SIZE, metricsMonitor, clock );
    }

    MetricsReportingBoltConnection( BoltChannel channel, PackOutput output, BoltStateMachine machine, LogService logService,
            BoltConnectionLifetimeListener listener,
            BoltConnectionQueueMonitor queueMonitor, int maxBatchSize, BoltConnectionMetricsMonitor metricsMonitor,
            Clock clock )
    {
        super( channel, output, machine, logService, listener, queueMonitor, maxBatchSize );
        this.metricsMonitor = metricsMonitor;
        this.clock = clock;
    }

    @Override
    public void start()
    {
        super.start();
        metricsMonitor.connectionOpened();
    }

    @Override
    public void enqueue( Job job )
    {
        metricsMonitor.messageReceived();
        long queuedAt = clock.millis();
        super.enqueue( machine ->
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
    public boolean processNextBatch( int batchCount, boolean exitIfNoJobsAvailable )
    {
        metricsMonitor.connectionActivated();

        try
        {
            boolean continueProcessing = super.processNextBatch( batchCount, exitIfNoJobsAvailable );

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

}
