/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A job cleaning something up after recovery. Usually added to {@link RecoveryCleanupWorkCollector}.
 * <p>
 * Report state of cleaning progress.
 */
public interface CleanupJob
{
    /**
     * @return {@code true} if gbptree still needs cleaning, meaning job is not yet finished or has not started at all.
     * {@code false} if gbptree does not need cleaning, meaning job has finished or it was never needed in the first place.
     */
    boolean needed();

    /**
     * @return {@code true} if the job has failed. Use {@link #getCause()} to see cause of failure.
     */
    boolean hasFailed();

    /**
     * @return Cause of failure if {@link #hasFailed()} or {@code null} if job has not failed.
     */
    Throwable getCause();

    /**
     * Mark this job as closed and cleanup all it's resources.
     */
    void close();

    /**
     * Run cleanup job and use provided executor for parallel tasks.
     * This method will wait for all jobs passed to executor to finish before returning.
     */
    void run( Executor executor );

    /**
     * A {@link CleanupJob} that doesn't need cleaning, i.e. it's already clean.
     */
    CleanupJob CLEAN = new Adaptor();

    /**
     * Executor of asynchronous units of work needed during the recovery clean up.
     */
    @FunctionalInterface
    interface Executor
    {
        <T> JobResult<T> submit( String jobDescription, Callable<T> job );
    }

    /**
     * Result handle of jobs executed by {@link Executor}.
     */
    @FunctionalInterface
    interface JobResult<T>
    {
        /**
         * Waits if necessary for the computation to complete, and then
         * retrieves its result, similar to {@link Future#get()}.
         *
         * @return the computed result
         * @throws ExecutionException if the computation threw an exception
         * @throws InterruptedException if the current thread was interrupted while waiting
         */
        T get() throws ExecutionException, InterruptedException;
    }

    class Adaptor implements CleanupJob
    {
        @Override
        public void run( Executor executor )
        {   // no-op
        }

        @Override
        public boolean needed()
        {
            return false;
        }

        @Override
        public boolean hasFailed()
        {
            return false;
        }

        @Override
        public Throwable getCause()
        {
            return null;
        }

        @Override
        public void close()
        {   // no-op
        }
    }
}
