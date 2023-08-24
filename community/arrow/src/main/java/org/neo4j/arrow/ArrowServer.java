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
package org.neo4j.arrow;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ArrowConnectorInternalSettings;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

public class ArrowServer extends LifecycleAdapter {

    private final FlightServer.Builder flightServerBuilder;
    private final RootAllocator allocator;
    private final Config config;
    private final Log log;

    private FlightServer flightServer;
    private volatile boolean running;

    public ArrowServer(Config config, Log log) {
        this.config = config;
        this.log = log;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.flightServerBuilder = FlightServer.builder().allocator(allocator);
        this.running = false;
    }

    @Override
    public void init() throws Exception {
        var listenAddress = config.get(ArrowConnectorInternalSettings.listen_address);
        flightServerBuilder.location(Location.forGrpcInsecure(listenAddress.getHostname(), listenAddress.getPort()));
        log.info("Configured Arrow connector with listener address %s", listenAddress);
    }

    @Override
    public void start() throws Exception {
        this.flightServer =
                flightServerBuilder.producer(new Neo4jFlightProducer()).build();
        this.flightServer.start();
        this.running = true;
        log.info("Arrow server started");
    }

    @Override
    public void stop() throws Exception {
        log.info("Requested Arrow server shutdown");
        this.allocator.close();
        this.flightServer.close();
        this.running = false;
    }

    @Override
    public void shutdown() throws Exception {
        log.info("Arrow server has been shut down");
    }

    public boolean isRunning() {
        return this.running;
    }
}
