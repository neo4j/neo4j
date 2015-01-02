/**
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
package org.neo4j.unsafe.impl.batchimport.executor;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import org.neo4j.kernel.impl.transaction.log.ParkStrategy;
import org.neo4j.test.DoubleLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DynamicTaskExecutorTest
{
    @Test
    public void shouldExecuteTasksInParallel() throws Exception
    {
        // GIVEN
        TaskExecutor executor = new DynamicTaskExecutor( 2, 5, new ParkStrategy.Park( 1 ), getClass().getSimpleName() );
        ControlledTask task1 = new ControlledTask();
        Task task2 = new Task();

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
        TaskExecutor executor = new DynamicTaskExecutor( 1, 5, new ParkStrategy.Park( 1 ), getClass().getSimpleName() );
        ControlledTask task1 = new ControlledTask();
        Task task2 = new Task();

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
        TaskExecutor executor = new DynamicTaskExecutor( 2, 5, new ParkStrategy.Park( 1 ), getClass().getSimpleName() );
        ControlledTask task1 = new ControlledTask();
        ControlledTask task2 = new ControlledTask();
        ControlledTask task3 = new ControlledTask();
        Task task4 = new Task();

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
        TaskExecutor executor = new DynamicTaskExecutor( 30, 5, new ParkStrategy.Park( 1 ), getClass().getSimpleName() );
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
        TaskExecutor executor = new DynamicTaskExecutor( 30, 5, new ParkStrategy.Park( 1 ), getClass().getSimpleName() );

        // WHEN
        IOException exception = new IOException( "Test message" );
        FailingTask task = new FailingTask( exception );
        executor.submit( task );
        task.latch.await();

        // THEN
        assertExceptionOnSubmit( executor, exception );
    }

    @Test
    public void shouldShutDownOnTaskFailureEvenIfOtherTasksArePending() throws Exception
    {
        // GIVEN
        TaskExecutor executor = new DynamicTaskExecutor( 2, 10, new ParkStrategy.Park( 1 ), getClass().getSimpleName() );
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

        // THEN
        assertExceptionOnSubmit( executor, exception );
        executor.shutdown( false ); // call would block if the shutdown as part of failure doesn't complete properly
    }

    private void assertExceptionOnSubmit( TaskExecutor executor, IOException exception )
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

    private static class Task implements Callable<Void>
    {
        protected volatile int executed;

        @Override
        public Void call()
        {
            executed++;
            return null;
        }
    }

    private static class EmptyTask implements Callable<Void>
    {
        @Override
        public Void call() throws Exception
        {
            return null;
        }
    }

    private static class FailingTask implements Callable<Void>
    {
        private final Exception exception;
        private final CountDownLatch latch = new CountDownLatch( 1 );

        public FailingTask( Exception exception )
        {
            this.exception = exception;
        }

        @Override
        public Void call() throws Exception
        {
            try
            {
                throw exception;
            }
            finally
            {
                latch.countDown();
            }
        }
    }

    private static class ExpensiveTask extends Task
    {
        private final int millis;

        ExpensiveTask( int millis )
        {
            this.millis = millis;
        }

        @Override
        public Void call()
        {
            try
            {
                Thread.sleep( millis );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
            return super.call();
        }
    }

    private static class ControlledTask extends Task
    {
        private final DoubleLatch latch = new DoubleLatch();

        @Override
        public Void call()
        {
            latch.startAndAwaitFinish();
            return super.call();
        }
    }
}
