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
package org.neo4j.monitoring;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.function.Consumer;

import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;

import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class VmPauseMonitor
{
    private final long measurementDurationNs;
    private final long stallAlertThresholdNs;
    private final Monitor monitor;
    private final JobScheduler jobScheduler;
    private final Consumer<VmPauseInfo> listener;
    private JobHandle job;

    public VmPauseMonitor( Duration measureInterval, Duration stallAlertThreshold, Monitor monitor, JobScheduler jobScheduler, Consumer<VmPauseInfo> listener )
    {
        this.measurementDurationNs = Preconditions.requirePositive( measureInterval.toNanos() );
        this.stallAlertThresholdNs = Preconditions.requireNonNegative( stallAlertThreshold.toNanos() );
        this.monitor = requireNonNull( monitor );
        this.jobScheduler = requireNonNull( jobScheduler );
        this.listener = requireNonNull( listener );
    }

    public void start()
    {
        monitor.started();
        Preconditions.checkState( job == null, "VM pause monitor is already started" );
        job = requireNonNull( jobScheduler.schedule( Group.VM_PAUSE_MONITOR, this::run ) );
    }

    public void stop()
    {
        monitor.stopped();
        Preconditions.checkState( job != null, "VM pause monitor is not started" );
        job.cancel( true );
        job = null;
    }

    private void run()
    {
        try
        {
            monitor();
        }
        catch ( InterruptedException ignore )
        {
            monitor.interrupted();
        }
        catch ( RuntimeException e )
        {
            monitor.failed( e );
        }
    }

    @VisibleForTesting
    void monitor() throws InterruptedException
    {
        GcStats lastGcStats = getGcStats();
        long nextCheckPoint = nanoTime() + measurementDurationNs;

        while ( !isStopped() )
        {
            NANOSECONDS.sleep( measurementDurationNs );
            final long now = nanoTime();
            final long pauseNs = max( 0L, now - nextCheckPoint );
            nextCheckPoint = now + measurementDurationNs;

            final GcStats gcStats = getGcStats();
            if ( pauseNs >= stallAlertThresholdNs )
            {
                final VmPauseInfo pauseInfo = new VmPauseInfo(
                        NANOSECONDS.toMillis( pauseNs ),
                        gcStats.time - lastGcStats.time,
                        gcStats.count - lastGcStats.count
                );
                listener.accept( pauseInfo );
            }
            lastGcStats = gcStats;
        }
    }

    @SuppressWarnings( "MethodMayBeStatic" )
    @VisibleForTesting
    boolean isStopped()
    {
        return currentThread().isInterrupted();
    }

    public static class VmPauseInfo
    {
        private final long pauseTime;
        private final long gcTime;
        private final long gcCount;

        VmPauseInfo( long pauseTime, long gcTime, long gcCount )
        {
            this.pauseTime = pauseTime;
            this.gcTime = gcTime;
            this.gcCount = gcCount;
        }

        public long getPauseTime()
        {
            return pauseTime;
        }

        @Override
        public String toString()
        {
            return format( "{pauseTime=%d, gcTime=%d, gcCount=%d}", pauseTime, gcTime, gcCount );
        }
    }

    private static GcStats getGcStats()
    {
        long time = 0;
        long count = 0;
        for ( GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans() )
        {
            time += gcBean.getCollectionTime();
            count += gcBean.getCollectionCount();
        }
        return new GcStats( time, count );
    }

    private static class GcStats
    {
        private final long time;
        private final long count;

        private GcStats( long time, long count )
        {
            this.time = time;
            this.count = count;
        }
    }

    public interface Monitor
    {
        void started();

        void stopped();

        void interrupted();

        void failed( Exception e );

        class Adapter implements Monitor
        {
            @Override
            public void started()
            {   // no-op
            }

            @Override
            public void stopped()
            {   // no-op
            }

            @Override
            public void interrupted()
            {   // no-op
            }

            @Override
            public void failed( Exception e )
            {   // no-op
            }
        }

        Monitor EMPTY = new Adapter();
    }
}
