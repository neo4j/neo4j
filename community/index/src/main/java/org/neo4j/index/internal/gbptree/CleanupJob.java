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
package org.neo4j.index.internal.gbptree;

/**
 * A job cleaning something up after recovery. Usually added to {@link RecoveryCleanupWorkCollector}.
 * <p>
 * Report state of cleaning progress.
 */
public interface CleanupJob extends Runnable
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
    Exception getCause();

    /**
     * A {@link CleanupJob} that doesn't need cleaning, i.e. it's already clean.
     */
    CleanupJob CLEAN = new CleanupJob()
    {
        @Override
        public void run()
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
        public Exception getCause()
        {
            return null;
        }
    };
}
