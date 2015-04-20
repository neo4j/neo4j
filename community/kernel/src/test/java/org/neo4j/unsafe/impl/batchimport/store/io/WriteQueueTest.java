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
package org.neo4j.unsafe.impl.batchimport.store.io;

import org.junit.Test;

import org.neo4j.unsafe.impl.batchimport.executor.TaskExecutor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WriteQueueTest
{
    @Test
    public void shouldExecuteTheJobImmediately() throws Exception
    {
        // GIVEN
        assertFalse( jobMonitor.hasActiveJobs() );

        // WHEN
        queue.offer( JOB1 );

        // THEN
        assertTrue( jobMonitor.hasActiveJobs() );
        verify( executor, times( 1 ) ).submit( queue );
        queue.run( null ); // call it manually after verification
        verify( JOB1, times( 1 ) ).execute();

        assertFalse( jobMonitor.hasActiveJobs() );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldSubmitToExecutorOnlyIfTheQueueWasEmpty() throws Exception
    {
        // GIVEN
        assertFalse( jobMonitor.hasActiveJobs() );

        // make queue non-empty
        queue.offer( JOB1 );
        reset( executor ); // forget call to execute for job1

        // WHEN
        queue.offer( JOB2 );

        // THEN
        assertTrue( jobMonitor.hasActiveJobs() );
        verify( executor, never() ).submit( queue );
    }

    @Test
    public void shouldDrainAllOfferedAtOnce() throws Exception
    {
        // GIVEN
        assertFalse( jobMonitor.hasActiveJobs() );

        // WHEN
        queue.offer( JOB1 );
        queue.offer( JOB2 );

        // THEN
        assertTrue( jobMonitor.hasActiveJobs() );
        verify( executor, times( 1 ) ).submit( queue );
        queue.run( null ); // call it manually after verification
        verify( JOB1, times( 1 ) ).execute();
        verify( JOB2, times( 1 ) ).execute();
        verify( JOB3, never() ).execute();

        assertFalse( jobMonitor.hasActiveJobs() );

        reset( executor, JOB1, JOB2, JOB3 );

        // WHEN
        queue.offer( JOB3 );

        // THEN
        assertTrue( jobMonitor.hasActiveJobs() );
        verify( executor, times( 1 ) ).submit( queue );
        queue.run( null ); // call it manually after verification
        verify( JOB1, never() ).execute();
        verify( JOB2, never() ).execute();
        verify( JOB3, times( 1 ) ).execute();

        assertFalse( jobMonitor.hasActiveJobs() );
    }

    @SuppressWarnings( "unchecked" )
    private final TaskExecutor<Void> executor = mock( TaskExecutor.class );
    private final JobMonitor jobMonitor = new JobMonitor();
    private final WriteQueue queue = new WriteQueue( executor, jobMonitor );

    private final WriteJob JOB1 = mock( WriteJob.class );
    private final WriteJob JOB2 = mock( WriteJob.class );
    private final WriteJob JOB3 = mock( WriteJob.class );
}
