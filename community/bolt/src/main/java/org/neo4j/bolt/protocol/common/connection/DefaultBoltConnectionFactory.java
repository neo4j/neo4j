/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.protocol.common.connection;

import static java.util.Objects.requireNonNull;
import static org.neo4j.bolt.protocol.common.connection.DefaultBoltConnection.DEFAULT_MAX_BATCH_SIZE;

import java.time.Clock;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.handler.KeepAliveHandler;
import org.neo4j.bolt.runtime.scheduling.BoltConnectionQueueMonitor;
import org.neo4j.bolt.runtime.scheduling.BoltConnectionQueueMonitorAggregate;
import org.neo4j.bolt.runtime.scheduling.BoltConnectionReadLimiter;
import org.neo4j.bolt.runtime.scheduling.BoltScheduler;
import org.neo4j.bolt.runtime.scheduling.BoltSchedulerProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;

public class DefaultBoltConnectionFactory implements BoltConnectionFactory {
    private final BoltSchedulerProvider schedulerProvider;
    private final LogService logService;
    private final Clock clock;
    private final Config config;
    private final BoltConnectionMetricsMonitor metricsMonitor;

    public DefaultBoltConnectionFactory(
            BoltSchedulerProvider schedulerProvider,
            Config config,
            LogService logService,
            Clock clock,
            Monitors monitors) {
        this.schedulerProvider = schedulerProvider;
        this.config = config;
        this.logService = logService;
        this.clock = clock;
        this.metricsMonitor = monitors.newMonitor(BoltConnectionMetricsMonitor.class);
    }

    @Override
    public BoltConnection newConnection(BoltChannel channel, StateMachine stateMachine) {
        requireNonNull(channel);
        requireNonNull(stateMachine);

        BoltScheduler scheduler = schedulerProvider.get(channel);
        BoltConnectionReadLimiter readLimiter = createReadLimiter(config, logService);
        BoltConnectionQueueMonitor connectionQueueMonitor =
                new BoltConnectionQueueMonitorAggregate(scheduler, readLimiter);

        var keepAliveHandler = createKeepAliveHandler();
        if (keepAliveHandler != null) {
            channel.rawChannel().pipeline().addLast(KeepAliveHandler.NAME, keepAliveHandler);
        }

        BoltConnection connection = new DefaultBoltConnection(
                channel,
                stateMachine,
                logService,
                scheduler,
                connectionQueueMonitor,
                DEFAULT_MAX_BATCH_SIZE,
                metricsMonitor,
                clock);
        connection.start();

        return connection;
    }

    private static BoltConnectionReadLimiter createReadLimiter(Config config, LogService logService) {
        int lowWatermark = config.get(GraphDatabaseInternalSettings.bolt_inbound_message_throttle_low_water_mark);
        int highWatermark = config.get(GraphDatabaseInternalSettings.bolt_inbound_message_throttle_high_water_mark);
        return new BoltConnectionReadLimiter(logService, lowWatermark, highWatermark);
    }

    private KeepAliveHandler createKeepAliveHandler() {
        var mechanism = config.get(BoltConnector.connection_keep_alive_type);
        var interval = config.get(BoltConnector.connection_keep_alive).toMillis();

        if (mechanism == BoltConnector.KeepAliveRequestType.OFF) {
            return null;
        }

        return new KeepAliveHandler(logService, mechanism != BoltConnector.KeepAliveRequestType.ALL, interval);
    }
}
