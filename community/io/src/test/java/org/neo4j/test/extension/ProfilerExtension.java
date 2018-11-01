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

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.JUnitException;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.neo4j.graphdb.Resource;
import org.neo4j.io.IOUtils;

import static java.lang.String.format;

class ProfilerExtension implements BeforeEachCallback, AfterEachCallback, Profiler
{
    private static final long DEFAULT_SAMPLE_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos( 10 );

    private final ArrayList<Thread> samplerThreads = new ArrayList<>();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final ConcurrentHashMap<Thread,Sample> samples = new ConcurrentHashMap<>();
    private final ArrayList<Consumer<ProfilerConfig>> delayedConfiguration = new ArrayList<>();
    private long sampleIntervalNanos = DEFAULT_SAMPLE_INTERVAL_NANOS;
    private boolean enableOutputOnSuccess;
    private boolean closeOutputWhenDone;
    private PrintStream out = System.err;
    private AtomicLong underSampling = new AtomicLong();

    @Override
    public void beforeEach( ExtensionContext context )
    {
        synchronized ( this )
        {
            stopped.set( false );
            samples.clear();
            underSampling.set( 0 );
        }
        if ( !delayedConfiguration.isEmpty() )
        {
            ArrayList<Consumer<ProfilerConfig>> list = new ArrayList<>( delayedConfiguration );
            delayedConfiguration.clear();
            Configurator configurator = new Configurator();
            for ( Consumer<ProfilerConfig> consumer : list )
            {
                consumer.accept( configurator );
            }
        }
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        try
        {
            stop();
            if ( enableOutputOnSuccess || context.getExecutionException().isPresent() )
            {
                printProfile( out, context );
            }
            if ( closeOutputWhenDone )
            {
                IOUtils.closeAllSilently( out );
            }
        }
        catch ( Exception e )
        {
            throw new JUnitException( format( "Fail to stop profiler for %s test.", context.getDisplayName() ), e );
        }
    }

    @Override
    public Resource profile( Thread threadToProfile, long initialDelayNanos )
    {
        long baseline = System.nanoTime();
        Thread samplerThread = new Thread( () ->
        {
            long nextSleepBaseline = baseline;//sleep( baseline, initialDelayNanos );
            Sample root = samples.computeIfAbsent( threadToProfile, k -> new Sample( null ) );
            while ( !stopped.get() && !Thread.currentThread().isInterrupted() && threadToProfile.isAlive() )
            {
                nextSleepBaseline = sleep( nextSleepBaseline, sampleIntervalNanos );
                StackTraceElement[] frames = threadToProfile.getStackTrace();
                record( root, frames );
            }
        } );
        samplerThreads.add( samplerThread );
        samplerThread.setName( "Sampler for " + threadToProfile.getName() );
        samplerThread.setPriority( Thread.NORM_PRIORITY + 1 );
        samplerThread.setDaemon( true );
        samplerThread.start();
        return samplerThread::interrupt;
    }

    private long sleep( long baselineNanos, long delayNanoes )
    {
        long nextBaseline = System.nanoTime();
        long sleepNanos = delayNanoes - (nextBaseline - baselineNanos);
        if ( sleepNanos > 0 )
        {
            LockSupport.parkNanos( this, sleepNanos );
        }
        else
        {
            underSampling.getAndIncrement();
            Thread.yield(); // The sampler thread runs with slightly elevated priority, so we yield to give the profiled thread a chance to run.
        }
        return nextBaseline + delayNanoes;
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

    private void stop() throws InterruptedException
    {
        stopped.set( true );
        for ( Thread thread : samplerThreads )
        {
            thread.interrupt();
            thread.join();
        }
        samplerThreads.clear();
    }

    private synchronized void printProfile( PrintStream out, ExtensionContext context )
    {
        String displayName = context.getTestClass().map( Class::getSimpleName ).orElse( "class" ) + "." + context.getDisplayName();
        out.println( "### Profiler output for " + displayName );
        if ( underSampling.get() > 0 )
        {
            long allSamplesTotal = samples.reduceToLong( Long.MAX_VALUE, ( thread, sample ) -> sample.count, 0, ( a, b ) -> a + b );
            out.println( "Info: Did not achieve target sampling frequency. " + underSampling + " of " + allSamplesTotal + " samples were delayed." );
        }
        for ( Map.Entry<Thread,Sample> entry : samples.entrySet() )
        {
            Thread thread = entry.getKey();
            Sample rootSample = entry.getValue();
            rootSample.intoOrdered();
            out.println( "Profile (" + rootSample.count + " samples) " + thread.getName() );
            double total = rootSample.count;
            printSampleTree( out, total, rootSample.orderedChildren, 2 );
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

    class Configurator implements ProfilerConfig
    {
        @Override
        public ProfilerConfig enableOutputOnSuccess( boolean enabled )
        {
            ProfilerExtension.this.enableOutputOnSuccess = enabled;
            return this;
        }

        @Override
        public ProfilerConfig outputTo( PrintStream out )
        {
            ProfilerExtension.this.out = out;
            return this;
        }

        @Override
        public ProfilerConfig closeOutputWhenDone( boolean closeOutputWhenDone )
        {
            ProfilerExtension.this.closeOutputWhenDone = closeOutputWhenDone;
            return this;
        }

        @Override
        public ProfilerConfig sampleIntervalNanos( long sampleIntervalNanos )
        {
            ProfilerExtension.this.sampleIntervalNanos = sampleIntervalNanos;
            return this;
        }

        @Override
        public ProfilerConfig delayedConfig( Consumer<ProfilerConfig> delayedConfigChange )
        {
            ProfilerExtension.this.delayedConfiguration.add( delayedConfigChange );
            return this;
        }

        @Override
        public Profiler profiler()
        {
            return ProfilerExtension.this;
        }
    }
}
