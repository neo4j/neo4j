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
 * Place to add recovery cleanup work to be done as part of recovery of {@link GBPTree}.
 */
public interface RecoveryCleanupWorkCollector extends Runnable
{
    /**
     * Adds {@link CleanupJob} to this collector.
     *
     * @param job cleanup job to perform, now or at some point in the future.
     */
    void add( CleanupJob job );

    /**
     * {@link CleanupJob#run() Runs} {@link #add(CleanupJob) added} cleanup jobs right away in the thread
     * calling {@link #add(CleanupJob)}.
     */
    RecoveryCleanupWorkCollector IMMEDIATE = new RecoveryCleanupWorkCollector()
    {
        @Override
        public void add( CleanupJob job )
        {
            job.run();
        }

        @Override
        public void run()
        {   // no-op
        }
    };
}
