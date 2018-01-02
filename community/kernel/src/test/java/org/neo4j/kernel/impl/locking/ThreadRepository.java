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
package org.neo4j.kernel.impl.locking;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import static java.util.concurrent.locks.LockSupport.getBlocker;

import static org.junit.Assert.assertArrayEquals;

public class ThreadRepository implements TestRule
{
    public interface Task
    {
        void perform() throws Exception;
    }

    public interface ThreadInfo
    {
        StackTraceElement[] getStackTrace();

        Object blocker();

        Thread.State getState();
    }

    private Repository repository;
    private final long timeout;
    private final TimeUnit unit;

    public ThreadRepository( long timeout, TimeUnit unit )
    {
        this.timeout = timeout;
        this.unit = unit;
    }

    public ThreadInfo execute( Task... tasks )
    {
        return repository.createThread( null, tasks );
    }

    public ThreadInfo execute( String name, Task... tasks )
    {
        return repository.createThread( name, tasks );
    }

    public Signal signal()
    {
        return new Signal( new CountDownLatch( 1 ) );
    }

    public Await await()
    {
        return await( 1 );
    }

    public Await await( int events )
    {
        return new Await( new CountDownLatch( events ) );
    }

    public Events events()
    {
        return new Events();
    }

    public static class Signal implements Task
    {
        private final CountDownLatch latch;

        private Signal( CountDownLatch latch )
        {
            this.latch = latch;
        }

        public Await await()
        {
            return new Await( latch );
        }

        public void awaitNow() throws InterruptedException
        {
            latch.await();
        }

        @Override
        public void perform() throws Exception
        {
            latch.countDown();
        }
    }

    public static class Await implements Task
    {
        private final CountDownLatch latch;

        private Await( CountDownLatch latch )
        {
            this.latch = latch;
        }

        public Signal signal()
        {
            return new Signal( latch );
        }

        public void release()
        {
            latch.countDown();
        }

        @Override
        public void perform() throws Exception
        {
            latch.await();
        }
    }

    public class Events
    {
        private final List<String> collected;

        private Events()
        {
            collected = new CopyOnWriteArrayList<>();
        }

        public Task trigger( final String event )
        {
            return new Task()
            {
                @Override
                public void perform() throws Exception
                {
                    collected.add( event );
                }
            };
        }

        public void assertInOrder( String... events ) throws Exception
        {
            try
            {
                completeThreads();
            }
            catch ( Error | Exception ok )
            {
                throw ok;
            }
            catch ( Throwable throwable )
            {
                throw new Exception( "Unexpected Throwable", throwable );
            }
            String[] actual = collected.toArray( new String[events.length] );
            assertArrayEquals( events, actual );
        }

        public List<String> snapshot()
        {
            return new ArrayList<>( collected );
        }
    }

    @Override
    public Statement apply( final Statement base, final Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                repository = new Repository( description );
                List<Throwable> failures = new ArrayList<>();
                try
                {
                    base.evaluate();
                }
                catch ( Throwable failure )
                {
                    failures.add( failure );
                }
                finally
                {
                    completeThreads( failures );
                }
                MultipleFailureException.assertEmpty( failures );
            }
        };
    }

    private void completeThreads() throws Throwable
    {
        List<Throwable> failures = new ArrayList<>();
        completeThreads( failures );
        MultipleFailureException.assertEmpty( failures );
    }

    private void completeThreads( List<Throwable> failures )
    {
        if ( repository != null )
        {
            repository.completeAll( failures );
        }
        repository = null;
    }

    private class Repository
    {
        private final Description description;
        private int i;
        private final List<TaskThread> threads = new ArrayList<>();

        Repository( Description description )
        {
            this.description = description;
        }

        synchronized TaskThread createThread( String name, Task[] tasks )
        {
            TaskThread thread = new TaskThread( nextName( name ), tasks );
            threads.add( thread );
            thread.start();
            return thread;
        }

        private String nextName( String name )
        {
            return description.getMethodName() + "-" + (++i) + (name == null ? "" : (":" + name));
        }

        void completeAll( List<Throwable> failures )
        {
            for ( TaskThread thread : threads )
            {
                try
                {
                    thread.complete( failures, timeout, unit );
                }
                catch ( InterruptedException interrupted )
                {
                    failures.add( interrupted );
                }
            }
        }
    }

    private static class TaskThread extends Thread implements ThreadInfo
    {
        private final Task[] tasks;
        private Exception failure;

        TaskThread( String name, Task[] tasks )
        {
            super( name );
            this.tasks = tasks;
        }

        void complete( List<Throwable> failures, long timeout, TimeUnit unit ) throws InterruptedException
        {
            join( unit.toMillis( timeout ) );
            if ( isAlive() )
            {
                failures.add( new ThreadStillRunningException( this ) );
            }
            if ( failure != null )
            {
                failures.add( failure );
            }
        }

        @Override
        public void run()
        {
            try
            {
                for ( Task task : tasks )
                {
                    task.perform();
                }
            }
            catch ( Exception e )
            {
                failure = e;
            }
        }

        @Override
        public Object blocker()
        {
            return getBlocker( this );
        }
    }

    private static class ThreadStillRunningException extends Exception
    {
        ThreadStillRunningException( TaskThread thread )
        {
            super( '"' + thread.getName() + "\"; state=" + thread.getState() + "; blockedOn=" + thread.blocker() );
            setStackTrace( thread.getStackTrace() );
        }

        @Override
        public synchronized Throwable fillInStackTrace()
        {
            return this;
        }
    }
}
