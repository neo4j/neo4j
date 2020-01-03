/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.resources;

import java.io.PrintStream;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

class SamplingProfiler implements Profiler
{
    private static final long DEFAULT_SAMPLE_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos( 1 );

    private final ConcurrentLinkedQueue<Thread> samplerThreads = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final ConcurrentHashMap<Thread,Sample> samples = new ConcurrentHashMap<>();
    private final AtomicLong sampleIntervalNanos = new AtomicLong( DEFAULT_SAMPLE_INTERVAL_NANOS );
    private final AtomicLong underSampling = new AtomicLong();

    @Override
    public void reset()
    {
        stopped.set( false );
        samples.clear();
        underSampling.set( 0 );
    }

    @Override
    public void finish() throws InterruptedException
    {
        stopped.set( true );
        Thread thread;
        while ( (thread = samplerThreads.poll()) != null )
        {
            thread.interrupt();
            thread.join();
        }
    }

    @Override
    public void printProfile( PrintStream out, String profileTitle )
    {
        out.println( "### " + profileTitle );
        if ( underSampling.get() > 0 )
        {
            long allSamplesTotal = samples.reduceToLong( Long.MAX_VALUE, ( thread, sample ) -> sample.get(), 0, ( a, b ) -> a + b );
            out.println( "Info: Did not achieve target sampling frequency. " + underSampling + " of " + allSamplesTotal + " samples were delayed." );
        }
        for ( Map.Entry<Thread,Sample> entry : samples.entrySet() )
        {
            Thread thread = entry.getKey();
            Sample rootSample = entry.getValue();
            rootSample.intoOrdered();
            out.println( "Profile (" + rootSample.get() + " samples) " + thread.getName() );
            double total = rootSample.get();
            printSampleTree( out, total, rootSample.orderedChildren, 2 );
        }
    }

    @Override
    public void setSampleIntervalNanos( long nanos )
    {
        sampleIntervalNanos.set( nanos );
    }

    @Override
    public ProfiledInterval profile( Thread threadToProfile, long initialDelayNanos )
    {
        long capturedSampleIntervalNanos = sampleIntervalNanos.get();
        long baseline = System.nanoTime();
        Thread samplerThread = new Thread( () ->
        {
            long nextSleepBaseline = initialDelayNanos > 0 ? sleep( baseline, initialDelayNanos ) : baseline;
            Sample root = samples.computeIfAbsent( threadToProfile, k -> new Sample( null ) );
            while ( !stopped.get() && threadToProfile.isAlive() )
            {
                StackTraceElement[] frames = threadToProfile.getStackTrace();
                if ( Thread.currentThread().isInterrupted() ) // Avoid recording samples that overlap with the end of the profiling interval.
                {
                    break;
                }
                record( root, frames );
                nextSleepBaseline = sleep( nextSleepBaseline, capturedSampleIntervalNanos );
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

    private void record( Sample root, StackTraceElement[] frames )
    {
        root.getAndIncrement();
        Map<StackTraceElement,Sample> level = root.children;
        // Iterate sample in reverse, since index 0 is top of the stack (most recent method invocation) and we record bottom-to-top.
        for ( int i = frames.length - 1; i >= 0 ; i-- )
        {
            StackTraceElement frame = frames[i];
            Sample sample = level.computeIfAbsent( frame, Sample::new );
            sample.getAndIncrement();
            level = sample.children;
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
            out.printf( "%.2f%%: %s%n", child.get() / total * 100.0, child.stackTraceElement );
            printSampleTree( out, total, child.orderedChildren, indent + 2 );
        }
    }

    private static final class Sample extends AtomicLong implements Comparable<Sample>
    {
        private final StackTraceElement stackTraceElement;
        private Map<StackTraceElement, Sample> children;
        private PriorityQueue<Sample> orderedChildren;

        private Sample( StackTraceElement stackTraceElement )
        {
            this.stackTraceElement = stackTraceElement;
            children = new ConcurrentHashMap<>();
        }

        @Override
        public int compareTo( Sample o )
        {
            // Higher count orders first.
            return Long.compare( o.get(), this.get() );
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
