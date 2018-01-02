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
package org.neo4j.unsafe.impl.batchimport.executor;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Future;

import org.neo4j.helpers.Exceptions;
import org.neo4j.test.Barrier;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DynamicTaskExecutorTest
{
    @Test
    public void shouldExecuteTasksInParallel() throws Exception
    {
        // GIVEN
        TaskExecutor<Void> executor = new DynamicTaskExecutor<>( 2, 0, 5, new ParkStrategy.Park( 1 ),
                getClass().getSimpleName() );
        ControlledTask task1 = new ControlledTask();
        TestTask task2 = new TestTask();

        // WHEN
        executor.submit( task1 );
        task1.latch.awaitStart();
        executor.submit( task2 );
        while ( task2.executed == 0 )
        {   // Busy loop
        }
        task1.latch.finish();
        while ( task1.executed == 0 )
        {   // Busy loop
        }
        executor.shutdown( true );

        // THEN
        assertEquals( 1, task1.executed );
        assertEquals( 1, task2.executed );
    }

    @Test
    public void shouldIncrementNumberOfProcessorsWhenRunning() throws Exception
    {
        // GIVEN
        TaskExecutor<Void> executor = new DynamicTaskExecutor<>( 1, 0, 5, new ParkStrategy.Park( 1 ),
                getClass().getSimpleName() );
        ControlledTask task1 = new ControlledTask();
        TestTask task2 = new TestTask();

        // WHEN
        executor.submit( task1 );
        task1.latch.awaitStart();
        executor.submit( task2 );
        executor.setNumberOfProcessors( 2 );
        while ( task2.executed == 0 )
        {   // With one additional worker, the second task can execute even if task one is still executing
        }
        task1.latch.finish();
        while ( task1.executed == 0 )
        {   // Busy loop
        }
        executor.shutdown( true );

        // THEN
        assertEquals( 1, task1.executed );
        assertEquals( 1, task2.executed );
    }

    @Test
    public void shouldDecrementNumberOfProcessorsWhenRunning() throws Exception
    {
        // GIVEN
        TaskExecutor<Void> executor = new DynamicTaskExecutor<>( 2, 0, 5, new ParkStrategy.Park( 1 ),
                getClass().getSimpleName() );
        ControlledTask task1 = new ControlledTask();
        ControlledTask task2 = new ControlledTask();
        ControlledTask task3 = new ControlledTask();
        TestTask task4 = new TestTask();

        // WHEN
        executor.submit( task1 );
        executor.submit( task2 );
        task1.latch.awaitStart();
        task2.latch.awaitStart();
        executor.submit( task3 );
        executor.submit( task4 );
        executor.setNumberOfProcessors( 1 );
        task1.latch.finish();
        task2.latch.finish();
        task3.latch.awaitStart();
        Thread.sleep( 200 ); // gosh, a Thread.sleep...
        assertEquals( 0, task4.executed );
        task3.latch.finish();
        executor.shutdown( true );

        // THEN
        assertEquals( 1, task1.executed );
        assertEquals( 1, task2.executed );
        assertEquals( 1, task3.executed );
        assertEquals( 1, task4.executed );
    }

    @Test
    public void shouldExecuteMultipleTasks() throws Exception
    {
        // GIVEN
        TaskExecutor<Void> executor = new DynamicTaskExecutor<>( 30, 0, 5, new ParkStrategy.Park( 1 ),
                getClass().getSimpleName() );
        ExpensiveTask[] tasks = new ExpensiveTask[1000];

        // WHEN
        for ( int i = 0; i < tasks.length; i++ )
        {
            executor.submit( tasks[i] = new ExpensiveTask( 10 ) );
        }
        executor.shutdown( true );

        // THEN
        for ( ExpensiveTask task : tasks )
        {
            assertEquals( 1, task.executed );
        }
    }

    @Test
    public void shouldShutDownOnTaskFailure() throws Exception
    {
        // GIVEN
        TaskExecutor<Void> executor = new DynamicTaskExecutor<>( 30, 0, 5, new ParkStrategy.Park( 1 ),
                getClass().getSimpleName() );

        // WHEN
        IOException exception = new IOException( "Test message" );
        FailingTask task = new FailingTask( exception );
        executor.submit( task );
        task.latch.await();
        task.latch.release();

        // THEN
        assertExceptionOnSubmit( executor, exception );
    }

    @Test
    public void shouldShutDownOnTaskFailureEvenIfOtherTasksArePending() throws Exception
    {
        // GIVEN
        TaskExecutor<Void> executor = new DynamicTaskExecutor<>( 2, 0, 10, new ParkStrategy.Park( 1 ),
                getClass().getSimpleName() );
        IOException exception = new IOException( "Test message" );
        ControlledTask firstBlockingTask = new ControlledTask();
        ControlledTask secondBlockingTask = new ControlledTask();
        executor.submit( firstBlockingTask );
        executor.submit( secondBlockingTask );
        firstBlockingTask.latch.awaitStart();
        secondBlockingTask.latch.awaitStart();

        FailingTask failingTask = new FailingTask( exception );
        executor.submit( failingTask );

        ControlledTask thirdBlockingTask = new ControlledTask();
        executor.submit( thirdBlockingTask );

        // WHEN
        firstBlockingTask.latch.finish();
        failingTask.latch.await();
        failingTask.latch.release();

        // THEN
        assertExceptionOnSubmit( executor, exception );
        executor.shutdown( false ); // call would block if the shutdown as part of failure doesn't complete properly
    }

    @Test
    public void shouldSurfaceTaskErrorInAssertHealthy() throws Exception
    {
        // GIVEN
        TaskExecutor<Void> executor = new DynamicTaskExecutor<>( 2, 0, 10, new ParkStrategy.Park( 1 ),
                getClass().getSimpleName() );
        IOException exception = new IOException( "Failure" );

        // WHEN
        FailingTask failingTask = new FailingTask( exception );
        executor.submit( failingTask );
        failingTask.latch.await();
        failingTask.latch.release();

        // WHEN
        for ( int i = 0; i < 5; i++ )
        {
            try
            {
                executor.assertHealthy();
                // OK, so the executor hasn't executed the finally block after task was done yet
                Thread.sleep( 100 );
            }
            catch ( Exception e )
            {
                assertTrue( Exceptions.contains( e, exception.getMessage(), exception.getClass() ) );
                return;
            }
        }
        fail( "Should not be considered healthy after failing task" );
    }

    @Test
    public void shouldLetShutdownCompleteInEventOfPanic() throws Exception
    {
        // GIVEN
        final TaskExecutor<Void> executor = new DynamicTaskExecutor<>( 2, 0, 10, new ParkStrategy.Park( 1 ),
                getClass().getSimpleName() );
        IOException exception = new IOException( "Failure" );

        // WHEN
        FailingTask failingTask = new FailingTask( exception );
        executor.submit( failingTask );
        failingTask.latch.await();

        // WHEN
        try ( OtherThreadExecutor<Void> closer = new OtherThreadExecutor<>( "closer", null ) )
        {
            Future<Void> shutdown = closer.executeDontWait( new WorkerCommand<Void,Void>()
            {
                @Override
                public Void doWork( Void state ) throws Exception
                {
                    executor.shutdown( true );
                    return null;
                }
            } );
            while ( !closer.waitUntilWaiting().isAt( DynamicTaskExecutor.class, "shutdown" ) )
            {
                Thread.sleep( 10 );
            }

            // Here we've got a shutdown call stuck awaiting queue to be empty (since true was passed in)
            // at the same time we've got a FailingTask ready to throw its exception and another task
            // sitting in the queue after it. Now make the task throw that exception.
            failingTask.latch.release();

            // Some time after throwing this, the shutdown request should have been completed.
            shutdown.get();
        }
    }

    @Test
    public void shouldRespectMaxProcessors() throws Exception
    {
        // GIVEN
        final TaskExecutor<Void> executor = new DynamicTaskExecutor<>( 1, 4, 10, new ParkStrategy.Park( 1 ),
                getClass().getSimpleName() );

        // WHEN/THEN
        assertEquals( 1, executor.numberOfProcessors() );
        assertFalse( executor.decrementNumberOfProcessors() );
        assertTrue( executor.incrementNumberOfProcessors() );
        assertEquals( 2, executor.numberOfProcessors() );
        executor.setNumberOfProcessors( 10 );
        assertEquals( 4, executor.numberOfProcessors() );
        executor.shutdown( true );
    }

    private void assertExceptionOnSubmit( TaskExecutor<Void> executor, IOException exception )
    {
        Exception submitException = null;
        for ( int i = 0; i < 5 && submitException == null; i++ )
        {
            try
            {
                executor.submit( new EmptyTask() );
                Thread.sleep( 100 );
            }
            catch ( Exception e )
            {
                submitException = e;
            }
        }
        assertNotNull( submitException );
        assertEquals( exception, submitException.getCause() );
    }

    private static class TestTask implements Task<Void>
    {
        protected volatile int executed;

        @Override
        public void run( Void nothing )
        {
            executed++;
        }
    }

    private static class EmptyTask implements Task<Void>
    {
        @Override
        public void run( Void nothing ) throws Exception
        {   // Do nothing
        }
    }

    private static class FailingTask implements Task<Void>
    {
        private final Exception exception;
        private final Barrier.Control latch = new Barrier.Control();

        public FailingTask( Exception exception )
        {
            this.exception = exception;
        }

        @Override
        public void run( Void nothing ) throws Exception
        {
            try
            {
                throw exception;
            }
            finally
            {
                latch.reached();
            }
        }
    }

    private static class ExpensiveTask extends TestTask
    {
        private final int millis;

        ExpensiveTask( int millis )
        {
            this.millis = millis;
        }

        @Override
        public void run( Void nothing )
        {
            try
            {
                Thread.sleep( millis );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
            super.run( nothing );
        }
    }

    private static class ControlledTask extends TestTask
    {
        private final DoubleLatch latch = new DoubleLatch();

        @Override
        public void run( Void nothing )
        {
            latch.startAndAwaitFinish();
            super.run( nothing );
        }
    }
}
