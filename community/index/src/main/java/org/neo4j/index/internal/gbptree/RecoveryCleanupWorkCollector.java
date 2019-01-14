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
package org.neo4j.index.internal.gbptree;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Place to add recovery cleanup work to be done as part of recovery of {@link GBPTree}.
 * <p>
 * {@link Lifecycle#init()} must prepare implementing class to be reused, probably by cleaning current state. After
 * this, implementing class must be ready to receive new jobs through {@link #add(CleanupJob)}.
 * <p>
 * Jobs may be processed during {@link #add(CleanupJob) add} or {@link Lifecycle#start() start}.
 * <p>
 * Take full responsibility for closing added {@link CleanupJob CleanupJobs} as soon as possible after run.
 */
public abstract class RecoveryCleanupWorkCollector extends LifecycleAdapter
{
    private static ImmediateRecoveryCleanupWorkCollector immediateInstance;
    private static IgnoringRecoveryCleanupWorkCollector ignoringInstance;

    /**
     * Adds {@link CleanupJob} to this collector.
     *
     * @param job cleanup job to perform, now or at some point in the future.
     */
    abstract void add( CleanupJob job );

    void executeWithExecutor( CleanupJobGroupAction action )
    {
        ExecutorService executor = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
        try
        {
            action.execute( executor );
        }
        finally
        {
            shutdownExecutorAndVerifyNoLeaks( executor );
        }
    }
    private void shutdownExecutorAndVerifyNoLeaks( ExecutorService executor )
    {
        List<Runnable> leakedTasks = executor.shutdownNow();
        if ( leakedTasks.size() != 0 )
        {
            throw new IllegalStateException( "Tasks leaked from CleanupJob. Tasks where " + leakedTasks.toString() );
        }
    }

    /**
     * {@link CleanupJob#run( ExecutorService ) Runs} {@link #add(CleanupJob) added} cleanup jobs right away in the thread
     * calling {@link #add(CleanupJob)}.
     */
    public static RecoveryCleanupWorkCollector immediate()
    {
        if ( immediateInstance == null )
        {
            immediateInstance = new ImmediateRecoveryCleanupWorkCollector();
        }
        return immediateInstance;
    }

    /**
     * Ignore all clean jobs.
     */
    public static RecoveryCleanupWorkCollector ignore()
    {
        if ( ignoringInstance == null )
        {
            ignoringInstance = new IgnoringRecoveryCleanupWorkCollector();
        }
        return ignoringInstance;
    }

    /**
     * {@link RecoveryCleanupWorkCollector} which runs added {@link CleanupJob} as part of the {@link #add(CleanupJob)}
     * call in the caller thread.
     */
    static class ImmediateRecoveryCleanupWorkCollector extends RecoveryCleanupWorkCollector
    {
        @Override
        public void add( CleanupJob job )
        {
            executeWithExecutor( executor ->
            {
                try
                {
                    job.run( executor );
                }
                finally
                {
                    job.close();
                }
            } );
        }
    }

    /**
     * {@link RecoveryCleanupWorkCollector} ignoring all {@link CleanupJob} added to it.
     */
    static class IgnoringRecoveryCleanupWorkCollector extends RecoveryCleanupWorkCollector
    {
        @Override
        public void add( CleanupJob job )
        {
            job.close();
        }
    }

    @FunctionalInterface
    interface CleanupJobGroupAction
    {
        void execute( ExecutorService executor );
    }
}
