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

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.JUnitException;

import java.io.Closeable;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static java.lang.String.format;

public class SamplingProfilerExtension extends StatefullFieldExtension<SamplingProfilerExtension.Profiler>
        implements BeforeEachCallback, AfterEachCallback, AfterAllCallback
{
    private static final String PROFILER = "samplingProfiler";
    private static final ExtensionContext.Namespace PROFILER_NAMESPACE = ExtensionContext.Namespace.create( PROFILER );

    @Override
    public void beforeEach( ExtensionContext context )
    {
        ProfilerImpl profiler = (ProfilerImpl) getStoredValue( context );
        profiler.reset();
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        ProfilerImpl profiler = (ProfilerImpl) getStoredValue( context );
        try
        {
            profiler.stop();
            if ( context.getExecutionException().isPresent() )
            {
                profiler.printProfile();
            }
        }
        catch ( Exception e )
        {
            throw new JUnitException( format( "Fail to stop profiler for %s test.", context.getDisplayName() ), e );
        }
    }

    @Override
    protected String getFieldKey()
    {
        return PROFILER;
    }

    @Override
    protected Class<Profiler> getFieldType()
    {
        return Profiler.class;
    }

    @Override
    protected Profiler createField( ExtensionContext extensionContext )
    {
        return new ProfilerImpl();
    }

    @Override
    protected ExtensionContext.Namespace getNameSpace()
    {
        return PROFILER_NAMESPACE;
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
