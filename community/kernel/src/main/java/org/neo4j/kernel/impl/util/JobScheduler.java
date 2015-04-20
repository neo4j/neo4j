/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * To be expanded, the idea here is to have a database-global service for running jobs, handling jobs crashing and so on.
 */
public interface JobScheduler extends Lifecycle
{
    /**
     * This is an exhaustive list of job types that run in the database. It should be expanded as needed for new groups
     * of jobs.
     *
     * For now, this does naming only, but it will allow us to define per-group configuration, such as how to handle
     * failures, shared threads and (later on) affinity strategies.
     */
    enum Group
    {
        indexPopulation,
        masterTransactionPushing,

        /**
         * Rolls back idle transactions on the server.
         */
        serverTransactionTimeout,

        /**
         * Aborts idle slave lock sessions on the master.
         */
        slaveLocksTimeout,

        /**
         * Pulls updates from the master.
         */
        pullUpdates,

        /**
         * Gathers approximated data about the underlying data store.
         */
        indexSamplingController,
        indexSampling,
        pageCacheEviction,
    }

    interface JobHandle
    {
        void cancel( boolean mayInterruptIfRunning );
    }

    JobHandle schedule( Group group, Runnable job );

    JobHandle scheduleRecurring( Group group, Runnable runnable, long period, TimeUnit timeUnit );

    JobHandle scheduleRecurring( Group group, Runnable runnable, long initialDelay, long period, TimeUnit timeUnit );
}
