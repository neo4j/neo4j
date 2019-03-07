/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.util.concurrent;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.helpers.Exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunnablesTest
{
    @Test
    void runAllMustRunAll()
    {
        // given
        Task task1 = new Task();
        Task task2 = new Task();
        Task task3 = new Task();

        // when
        Runnables.runAll( "", task1, task2, task3 );

        // then
        assertRun( task1, task2, task3 );
    }

    @Test
    void runAllMustRunAllAndPropagateError()
    {
        // given
        Task task1 = new Task();
        Task task2 = new Task();
        Task task3 = new Task();
        Error expectedError = new Error( "Killroy was here" );
        Runnable throwingTask = error( expectedError );

        List<Runnable> runnables = Arrays.asList( task1, task2, task3, throwingTask );
        Collections.shuffle(runnables );

        // when
        String failureMessage = "Something wrong, Killroy must be here somewhere.";
        RuntimeException actual = assertThrows( RuntimeException.class, () -> Runnables.runAll( failureMessage, runnables.toArray( new Runnable[0] ) ) );

        // then
        assertRun( task1, task2, task3 );
        assertTrue( Exceptions.findCauseOrSuppressed( actual, t -> t == expectedError ).isPresent() );
        assertEquals( failureMessage, actual.getMessage() );
    }

    @Test
    void runAllMustRunAllAndPropagateMultipleErrors()
    {
        // given
        Task task1 = new Task();
        Task task2 = new Task();
        Task task3 = new Task();
        Error expectedError = new Error( "Killroy was here" );
        Runnable throwingTask1 = error( expectedError );
        RuntimeException expectedException = new RuntimeException( "and here" );
        Runnable throwingTask2 = runtimeException( expectedException );

        List<Runnable> runnables = Arrays.asList( task1, task2, task3, throwingTask1, throwingTask2 );
        Collections.shuffle(runnables );

        // when
        String failureMessage = "Something wrong, Killroy must be here somewhere.";
        RuntimeException actual = assertThrows( RuntimeException.class, () -> Runnables.runAll( failureMessage, runnables.toArray( new Runnable[0] ) ) );

        // then
        assertRun( task1, task2, task3 );
        assertTrue( Exceptions.findCauseOrSuppressed( actual, t -> t == expectedError ).isPresent() );
        assertTrue( Exceptions.findCauseOrSuppressed( actual, t -> t == expectedException ).isPresent() );
        assertEquals( failureMessage, actual.getMessage() );
    }

    private Runnable error( Error error )
    {
        return () ->
        {
            throw error;
        };
    }

    private Runnable runtimeException( RuntimeException runtimeException )
    {
        return () ->
        {
            throw runtimeException;
        };
    }

    private void assertRun( Task... tasks )
    {
        for ( Task task : tasks )
        {
            assertTrue( task.run, "didn't run all expected tasks" );
        }
    }

    private class Task implements Runnable
    {
        private boolean run;

        @Override
        public void run()
        {
            run = true;
        }
    }
}
