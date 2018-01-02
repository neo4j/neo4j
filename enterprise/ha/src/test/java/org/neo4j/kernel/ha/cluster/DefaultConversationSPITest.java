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
package org.neo4j.kernel.ha.cluster;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.util.JobScheduler;

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
    public void testAcquireClient() throws Exception
    {
        conversationSpi.acquireClient();

        verify(locks).newClient();
    }

    @Test
    public void testScheduleRecurringJob() throws Exception
    {
        Runnable job = mock( Runnable.class );
        JobScheduler.Group group = mock( JobScheduler.Group.class );
        conversationSpi.scheduleRecurringJob( group, 0, job );

        verify( jobScheduler ).scheduleRecurring( group, job, 0, TimeUnit.MILLISECONDS );
    }
}
