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
package org.neo4j.unsafe.impl.batchimport.store.io;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class WriteQueueTest
{
    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldSubmitToExecutorOnFirstOffer() throws Exception
    {
        // GIVEN
        ExecutorService executor = mock( ExecutorService.class );
        WriteQueue queue = new WriteQueue( executor );

        // WHEN/THEN
        WriteJob job1 = mock( WriteJob.class );
        queue.offer( job1 );
        verify( executor, times( 1 ) ).submit( any( Callable.class ) );

        // WHEN/THEN
        WriteJob job2 = mock( WriteJob.class );
        queue.offer( job2 );
        verifyNoMoreInteractions( executor );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldDrainAllOfferedAtOnce() throws Exception
    {
        // GIVEN
        ExecutorService executor = mock( ExecutorService.class );
        WriteQueue queue = new WriteQueue( executor );

        // WHEN/THEN
        WriteJob job1 = mock( WriteJob.class );
        WriteJob job2 = mock( WriteJob.class );
        queue.offer( job1 );
        queue.offer( job2 );
        assertArrayEquals( new WriteJob[] {job1, job2}, queue.drain() );
        verify( executor, times( 1 ) ).submit( any( Callable.class ) );
        reset( executor );

        // WHEN/THEN
        WriteJob job3 = mock( WriteJob.class );
        queue.offer( job3 );
        assertArrayEquals( new WriteJob[] {job3}, queue.drain() );
        verify( executor, times( 1 ) ).submit( any( Callable.class ) );
    }
}
