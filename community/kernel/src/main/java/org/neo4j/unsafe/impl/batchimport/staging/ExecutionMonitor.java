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
package org.neo4j.unsafe.impl.batchimport.staging;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.time.Clocks;

/**
 * Gets notified now and then about {@link StageExecution}, where statistics can be read and displayed,
 * aggregated or in other ways make sense of the data of {@link StageExecution}.
 */
public interface ExecutionMonitor
{
    /**
     * Signals start of import. Called only once and before any other method.
     *
     * @param dependencyResolver {@link DependencyResolver} for getting dependencies from.
     */
    default void initialize( DependencyResolver dependencyResolver )
    {   // empty by default
    }

    /**
     * Signals the start of a {@link StageExecution}.
     */
    void start( StageExecution execution );

    /**
     * Signals the end of the execution previously {@link #start(StageExecution) started}.
     */
    void end( StageExecution execution, long totalTimeMillis );

    /**
     * Called after all {@link StageExecution stage executions} have run.
     */
    void done( boolean successful, long totalTimeMillis, String additionalInformation );

    /**
     * @return next time stamp when this monitor would like to check that status of current execution.
     */
    long nextCheckTime();

    /**
     * Called periodically while executing a {@link StageExecution}.
     */
    void check( StageExecution execution );

    /**
     * Base implementation with most methods defaulting to not doing anything.
     */
    abstract class Adapter implements ExecutionMonitor
    {
        private final Clock clock;
        private final long intervalMillis;

        public Adapter( Clock clock, long time, TimeUnit unit )
        {
            this.clock = clock;
            this.intervalMillis = unit.toMillis( time );
        }

        public Adapter( long time, TimeUnit unit )
        {
            this( Clocks.systemClock(), time, unit );
        }

        @Override
        public long nextCheckTime()
        {
            return clock.millis() + intervalMillis;
        }

        @Override
        public void start( StageExecution execution )
        {   // Do nothing by default
        }

        @Override
        public void end( StageExecution execution, long totalTimeMillis )
        {   // Do nothing by default
        }

        @Override
        public void done( boolean successful, long totalTimeMillis, String additionalInformation )
        {   // Do nothing by default
        }
    }
}
