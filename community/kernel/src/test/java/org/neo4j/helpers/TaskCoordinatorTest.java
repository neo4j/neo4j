/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.helpers;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.test.Barrier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TaskCoordinatorTest
{
    @Test
    public void shouldCancelAllTasksWithOneCall() throws Exception
    {
        // given
        TaskCoordinator coordinator = new TaskCoordinator( 1, TimeUnit.MILLISECONDS );

        try ( TaskControl task1 = coordinator.newInstance();
              TaskControl task2 = coordinator.newInstance();
              TaskControl task3 = coordinator.newInstance() )
        {
            assertFalse( task1.cancellationRequested() );
            assertFalse( task2.cancellationRequested() );
            assertFalse( task3.cancellationRequested() );

            // when
            coordinator.cancel();

            // then
            assertTrue( task1.cancellationRequested() );
            assertTrue( task2.cancellationRequested() );
            assertTrue( task3.cancellationRequested() );
        }
    }

    @Test
    public void shouldAwaitCompletionOfAllTasks() throws Exception
    {
        // given
        final TaskCoordinator coordinator = new TaskCoordinator( 1, TimeUnit.MILLISECONDS );
        final AtomicReference<String> state = new AtomicReference<>();
        final List<String> states = new ArrayList<>();
        final Barrier.Control phaseA = new Barrier.Control();
        final Barrier.Control phaseB = new Barrier.Control();
        final Barrier.Control phaseC = new Barrier.Control();

        state.set( "A" );
        new Thread( "awaitCompletion" )
        {
            @Override
            public void run()
            {
                try
                {
                    states.add( state.get() ); // expects A
                    phaseA.reached();
                    states.add( state.get() ); // expects B
                    phaseB.await();
                    phaseB.release();
                    coordinator.awaitCompletion();
                    states.add( state.get() ); // expects C
                    phaseC.reached();
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
            }
        }.start();

        // when
        try ( TaskControl task1 = coordinator.newInstance();
              TaskControl task2 = coordinator.newInstance() )
        {
            phaseA.await();
            state.set( "B" );
            phaseA.release();
            phaseC.release();
            phaseB.reached();
            state.set( "C" );
        }
        phaseC.await();

        // then
        assertEquals( Arrays.asList( "A", "B", "C" ), states );
    }
}
