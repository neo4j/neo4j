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
    private final BoltSchedulerProvider schedulerProvider;
    private final LogService logService;
    private final Clock clock;
    private final BoltConnectionQueueMonitor queueMonitor;

    public DefaultBoltConnectionFactory( BoltFactory machineFactory, BoltSchedulerProvider schedulerProvider, LogService logService, Clock clock,
            BoltConnectionQueueMonitor queueMonitor )
    {
        this.machineFactory = machineFactory;
        this.schedulerProvider = schedulerProvider;
        this.logService = logService;
        this.clock = clock;
        this.queueMonitor = queueMonitor;
    }

    @Override
    public BoltConnection newConnection( BoltChannel channel )
    {
        BoltScheduler scheduler = schedulerProvider.get( channel );
        BoltConnectionQueueMonitor connectionQueueMonitor =
                queueMonitor == null ? scheduler : new BoltConnectionQueueMonitorAggregate( scheduler, queueMonitor );
        BoltConnection connection =
<<<<<<< HEAD
                new DefaultBoltConnection( channel, machineFactory.newMachine( channel, () -> {
                }, clock ),
                        logService, outOfBandStrategy, scheduler, queueMonitor );
=======
                new DefaultBoltConnection( channel, machineFactory.newMachine( channel, clock ), logService, scheduler, connectionQueueMonitor );
>>>>>>> 1ba1d2f8c3f... Make `BoltScheduler` configurable per bolt connector

        connection.start();

        return connection;
    }

}
