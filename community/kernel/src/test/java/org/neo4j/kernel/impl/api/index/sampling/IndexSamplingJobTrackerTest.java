/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index.sampling;

import org.junit.Test;

import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.test.DoubleLatch;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexSamplingJobTrackerTest
{
    @Test
    public void shouldNotAcceptMoreJobsThanAllowed() throws Throwable
    {
        // given
        JobScheduler jobScheduler = new Neo4jJobScheduler();
        jobScheduler.init();

        IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( jobScheduler, 1 );
        final DoubleLatch latch = new DoubleLatch();

        // when
        assertTrue( jobTracker.canExecuteMoreSamplingJobs() );

        jobTracker.scheduleSamplingJob( new Runnable()
        {
            @Override
            public void run()
            {
                latch.start();
                latch.awaitFinish();
            }
        } );

        // then
        latch.awaitStart();

        assertFalse(jobTracker.canExecuteMoreSamplingJobs() );

        latch.finish();

        // eventually we accept new jobs
        while( ! jobTracker.canExecuteMoreSamplingJobs() )
        {
            Thread.yield();
        }
    }
}
