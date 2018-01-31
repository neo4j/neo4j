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

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.v1.runtime.BoltFactory;
import org.neo4j.kernel.impl.logging.LogService;

public class DefaultBoltConnectionFactory implements BoltConnectionFactory
{
    private final BoltFactory machineFactory;
    private final BoltScheduler scheduler;
    private final OutOfBandStrategy outOfBandStrategy;
    private final LogService logService;
    private final Clock clock;
    private final BoltConnectionQueueMonitor queueMonitor;

    public DefaultBoltConnectionFactory( BoltFactory machineFactory, BoltScheduler scheduler, OutOfBandStrategy outOfBandStrategy, LogService logService,
            Clock clock, BoltConnectionQueueMonitor queueMonitor )
    {
        this.machineFactory = machineFactory;
        this.scheduler = scheduler;
        this.outOfBandStrategy = outOfBandStrategy;
        this.logService = logService;
        this.clock = clock;
        this.queueMonitor = queueMonitor == null ?  scheduler : new BoltConnectionQueueMonitorAggregate( scheduler, queueMonitor );
    }

    @Override
    public BoltConnection newConnection( BoltChannel channel )
    {
        BoltConnection connection =
                new DefaultBoltConnection( channel, machineFactory.newMachine( channel, () -> {
                }, clock ),
                        logService, outOfBandStrategy, scheduler, queueMonitor );

        connection.start();

        return connection;
    }

}
