/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha.cluster;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.scheduler.JobScheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith( MockitoJUnitRunner.class )
public class DefaultConversationSPITest
{

    @Mock( answer = Answers.RETURNS_MOCKS )
    private Locks locks;
    @Mock
    private JobScheduler jobScheduler;
    @InjectMocks
    private DefaultConversationSPI conversationSpi;

    @Test
    public void testAcquireClient()
    {
        conversationSpi.acquireClient();

        verify(locks).newClient();
    }

    @Test
    public void testScheduleRecurringJob()
    {
        Runnable job = mock( Runnable.class );
        JobScheduler.Group group = new JobScheduler.Group( "group" );
        conversationSpi.scheduleRecurringJob( group, 0, job );

        verify( jobScheduler ).scheduleRecurring( group, job, 0, TimeUnit.MILLISECONDS );
    }
}
