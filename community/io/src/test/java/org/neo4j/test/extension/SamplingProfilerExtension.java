/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.test.extension;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class SamplingProfilerExtension implements TestRule, SamplingProfilerExtension.Profiler
{
    private final ProfilerImpl profiler = new ProfilerImpl();

    private void beforeEach()
    {
        profiler.reset();
    }

    private void afterEach( boolean failed ) throws InterruptedException
    {
        profiler.stop();
        if ( failed )
        {
            profiler.printProfile();
        }
    }

    @Override
    public Statement apply( Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                beforeEach();
                try
                {
                    base.evaluate();
                    afterEach( false );
                }
                catch ( Throwable th )
                {
                    afterEach( true );
                    throw th;
                }
            }
        };
    }

    @Override
    public AutoCloseable profile( Thread threadToProfile, long initialDelayNanos )
    {
        return profiler.profile( threadToProfile, initialDelayNanos );
    }

    public interface Profiler
    {
        Profiler NULL = ( thread, delay ) -> null;

        AutoCloseable profile( Thread threadToProfile, long initialDelayNanos );
    }

    private static class ProfilerImpl implements Profiler
    {
        private static final long SAMPLE_INTERVAL_NANOS = TimeUnit.MICROSECONDS.toNanos( 100 );

        private final ArrayList<Thread> samplerThreads = new ArrayList<>();
        private final AtomicBoolean stopped = new AtomicBoolean();
        private final ConcurrentHashMap<Thread,Sample> samples = new ConcurrentHashMap<>();

        synchronized void reset()
        {
            stopped.set( false );
            samples.clear();
        }

        @Override
        public AutoCloseable profile( Thread threadToProfile, long initialDelayNanos )
        {
            Thread samplerThread = new Thread( () ->
            {
                sleep( initialDelayNanos );
                Sample root = samples.computeIfAbsent( threadToProfile, k -> new Sample( null ) );
                while ( !stopped.get() && !Thread.currentThread().isInterrupted() && threadToProfile.isAlive() )
                {
                    StackTraceElement[] frames = threadToProfile.getStackTrace();
                    record( root, frames );
                    sleep( SAMPLE_INTERVAL_NANOS );
                }
            } );
            samplerThreads.add( samplerThread );
            samplerThread.start();
            return samplerThread::interrupt;
        }

        private void sleep( long intervalNanos )
        {
            LockSupport.parkNanos( this, intervalNanos );
        }

        private synchronized void record( Sample root, StackTraceElement[] frames )
        {
            root.count++;
            HashMap<StackTraceElement,Sample> level = root.children;
            // Iterate sample in reverse, since index 0 is top of the stack (most recent method invocation) and we record bottom-to-top.
            for ( int i = frames.length - 1; i >= 0 ; i-- )
            {
                StackTraceElement frame = frames[i];
                Sample sample = level.computeIfAbsent( frame, Sample::new );
                sample.count++;
                level = sample.children;
            }
        }

        void stop() throws InterruptedException
        {
            stopped.set( true );
            for ( Thread thread : samplerThreads )
            {
                thread.interrupt();
                thread.join();
            }
            samplerThreads.clear();
        }

        synchronized void printProfile()
        {
            for ( Map.Entry<Thread,Sample> entry : samples.entrySet() )
            {
                Thread thread = entry.getKey();
                Sample rootSample = entry.getValue();
                rootSample.intoOrdered();
                System.err.println( "Profile (" + rootSample.count + " samples) " + thread.getName() );
                double total = rootSample.count;
                printSampleTree( System.err, total, rootSample.orderedChildren, 2 );
            }
        }

        private void printSampleTree( PrintStream out, double total, PriorityQueue<Sample> children, int indent )
        {
            Sample child;
            while ( (child = children.poll()) != null )
            {
                for ( int i = 0; i < indent; i++ )
                {
                    out.print( ' ' );
                }
                out.printf( "%.2f%%: %s%n", child.count / total * 100.0, child.stackTraceElement );
                printSampleTree( out, total, child.orderedChildren, indent + 2 );
            }
        }

        private static final class Sample implements Comparable<Sample>
        {
            private final StackTraceElement stackTraceElement;
            private HashMap<StackTraceElement, Sample> children;
            private PriorityQueue<Sample> orderedChildren;
            private long count;

            private Sample( StackTraceElement stackTraceElement )
            {
                this.stackTraceElement = stackTraceElement;
                children = new HashMap<>();
            }

            @Override
            public int compareTo( Sample o )
            {
                // Higher count orders first.
                return Long.compare( o.count, this.count );
            }

            void intoOrdered()
            {
                orderedChildren = new PriorityQueue<>();
                orderedChildren.addAll( children.values() );
                children.clear();
                for ( Sample child : orderedChildren )
                {
                    child.intoOrdered();
                }
            }
        }
    }
}
