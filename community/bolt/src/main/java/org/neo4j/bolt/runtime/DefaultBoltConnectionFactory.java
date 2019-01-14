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
import org.neo4j.bolt.transport.TransportThrottleGroup;
import org.neo4j.bolt.v1.transport.ChunkedOutput;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.internal.LogService;

import static java.util.Objects.requireNonNull;

public class DefaultBoltConnectionFactory implements BoltConnectionFactory
{
    private final BoltSchedulerProvider schedulerProvider;
    private final TransportThrottleGroup throttleGroup;
    private final LogService logService;
    private final Clock clock;
    private final Config config;
    private final Monitors monitors;
    private final BoltConnectionMetricsMonitor metricsMonitor;

    public DefaultBoltConnectionFactory( BoltSchedulerProvider schedulerProvider, TransportThrottleGroup throttleGroup,
            Config config, LogService logService, Clock clock, Monitors monitors )
    {
        this.schedulerProvider = schedulerProvider;
        this.throttleGroup = throttleGroup;
        this.config = config;
        this.logService = logService;
        this.clock = clock;
        this.monitors = monitors;
        this.metricsMonitor = monitors.newMonitor( BoltConnectionMetricsMonitor.class );
    }

    @Override
    public BoltConnection newConnection( BoltChannel channel, BoltStateMachine stateMachine )
    {
        requireNonNull( channel );
        requireNonNull( stateMachine );

        BoltScheduler scheduler = schedulerProvider.get( channel );
        BoltConnectionReadLimiter readLimiter = createReadLimiter( config, logService );
        BoltConnectionQueueMonitor connectionQueueMonitor = new BoltConnectionQueueMonitorAggregate( scheduler, readLimiter );
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

    private static BoltConnectionReadLimiter createReadLimiter( Config config, LogService logService )
    {
        int lowWatermark = config.get( GraphDatabaseSettings.bolt_inbound_message_throttle_low_water_mark );
        int highWatermark = config.get( GraphDatabaseSettings.bolt_inbound_message_throttle_high_water_mark );
        return new BoltConnectionReadLimiter( logService, lowWatermark, highWatermark );
    }
}
