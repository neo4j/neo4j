/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.concurrent;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.v1.runtime.BoltFactory;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.bolt.v1.runtime.WorkerFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.scheduler.JobScheduler.Group.THREAD_ID;
import static org.neo4j.scheduler.JobScheduler.Groups.sessionWorker;

/**
 * A {@link WorkerFactory} implementation that creates one thread for every session started, requests are then executed
 * in the session-specific thread.
 *
 * This resolves a tricky issue where sharing threads for multiple sessions can cause complex deadlocks. It does so
 * at the expense of creating, potentially, many threads. However, this approach is much less complex than using
 * a thread pool, and is the preferred approach of several highly scalable relational databases.
 *
 * If we find ourselves with tens of thousands of concurrent sessions per neo4j instance, we may want to introduce an
 * alternate strategy.
 */
public class ThreadedWorkerFactory implements WorkerFactory
{
    private final BoltFactory connector;
    private final JobScheduler scheduler;
    private final LogService logging;
    private final Clock clock;

    public ThreadedWorkerFactory( BoltFactory connector, JobScheduler scheduler, LogService logging, Clock clock )
    {
        this.connector = connector;
        this.scheduler = scheduler;
        this.logging = logging;
        this.clock = clock;
    }

    @Override
    public BoltWorker newWorker( BoltChannel boltChannel )
    {
        BoltStateMachine machine = connector.newMachine( boltChannel, clock );
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logging );

        scheduler.schedule( sessionWorker, worker, stringMap( THREAD_ID, machine.key() ) );

        return worker;
    }
}
