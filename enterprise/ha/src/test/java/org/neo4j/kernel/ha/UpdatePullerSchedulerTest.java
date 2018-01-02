/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OnDemandJobScheduler;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class UpdatePullerSchedulerTest
{
    private UpdatePuller updatePuller;

    @Before
    public void setUp()
    {
        updatePuller = mock( UpdatePuller.class );
    }

    @Test
    public void skipUpdatePullingSchedulingWithZeroInterval() throws Throwable
    {
        JobScheduler jobScheduler = mock( JobScheduler.class );
        UpdatePullerScheduler pullerScheduler =
                new UpdatePullerScheduler( jobScheduler, NullLogProvider.getInstance(), updatePuller, 0 );

        // when start puller scheduler - nothing should be scheduled
        pullerScheduler.init();

        verifyZeroInteractions( jobScheduler, updatePuller );

        // should be able shutdown scheduler
        pullerScheduler.shutdown();
    }

    @Test
    public void scheduleUpdatePulling() throws Throwable
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        UpdatePullerScheduler pullerScheduler =
                new UpdatePullerScheduler( jobScheduler, NullLogProvider.getInstance(), updatePuller, 10 );

        // schedule update pulling and run it
        pullerScheduler.init();
        jobScheduler.runJob();

        verify( updatePuller ).pullUpdates();
        assertNotNull( "Job should be scheduled", jobScheduler.getJob() );

        // stop scheduler - job should be canceled
        pullerScheduler.shutdown();

        assertNull( "Job should be canceled", jobScheduler.getJob() );
    }

}
