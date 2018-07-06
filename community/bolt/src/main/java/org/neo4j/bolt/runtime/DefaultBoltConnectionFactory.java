/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.bolt.transport.TransportThrottleGroup;
import org.neo4j.bolt.v1.transport.ChunkedOutput;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.monitoring.Monitors;

import static java.util.Objects.requireNonNull;

public class DefaultBoltConnectionFactory implements BoltConnectionFactory
{
    private final BoltSchedulerProvider schedulerProvider;
    private final TransportThrottleGroup throttleGroup;
    private final LogService logService;
    private final Clock clock;
    private final BoltConnectionQueueMonitor queueMonitor;
    private final Monitors monitors;
    private final BoltConnectionMetricsMonitor metricsMonitor;

    public DefaultBoltConnectionFactory( BoltSchedulerProvider schedulerProvider, TransportThrottleGroup throttleGroup,
            LogService logService, Clock clock, BoltConnectionQueueMonitor queueMonitor, Monitors monitors )
    {
        this.schedulerProvider = schedulerProvider;
        this.throttleGroup = throttleGroup;
        this.logService = logService;
        this.clock = clock;
        this.queueMonitor = queueMonitor;
        this.monitors = monitors;
        this.metricsMonitor = monitors.newMonitor( BoltConnectionMetricsMonitor.class );
    }

    @Override
    public BoltConnection newConnection( BoltChannel channel, BoltStateMachine stateMachine )
    {
        requireNonNull( channel );
        requireNonNull( stateMachine );

        BoltScheduler scheduler = schedulerProvider.get( channel );
        BoltConnectionQueueMonitor connectionQueueMonitor =
                queueMonitor == null ? scheduler : new BoltConnectionQueueMonitorAggregate( scheduler, queueMonitor );
        ChunkedOutput chunkedOutput = new ChunkedOutput( channel.rawChannel(), throttleGroup );

        BoltConnection connection;
        if ( monitors.hasListeners( BoltConnectionMetricsMonitor.class ) )
        {
            connection = new MetricsReportingBoltConnection( channel, chunkedOutput, stateMachine, logService, scheduler,
                    connectionQueueMonitor, metricsMonitor, clock );
        }
        else
        {
            connection = new DefaultBoltConnection( channel, chunkedOutput, stateMachine, logService, scheduler,
                    connectionQueueMonitor );
        }

        connection.start();

        return connection;
    }
}
