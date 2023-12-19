/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.schedule;

import org.neo4j.causalclustering.core.consensus.schedule.TimerService.TimerName;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.String.format;

/**
 * A timer which can be set to go off at a future point in time.
 * <p>
 * When the timer goes off a timeout event is said to occur and the registered
 * {@link TimeoutHandler} will be invoked.
 */
public class Timer
{
    private final TimerName name;
    private final JobScheduler scheduler;
    private final Log log;
    private final JobScheduler.Group group;
    private final TimeoutHandler handler;

    private Timeout timeout;
    private Delay delay;
    private JobScheduler.JobHandle job;
    private long activeJobId;

    /**
     * Creates a timer in the deactivated state.
     *
     * @param name The name of the timer.
     * @param scheduler The underlying scheduler used.
     * @param group The scheduler group used.
     * @param handler The timeout handler.
     */
    Timer( TimerName name, JobScheduler scheduler, Log log, JobScheduler.Group group, TimeoutHandler handler )
    {
        this.name = name;
        this.scheduler = scheduler;
        this.log = log;
        this.group = group;
        this.handler = handler;
    }

    /**
     * Activates the timer to go off at the specified timeout. Calling this method
     * when the timer already is active will shift the timeout to the new value.
     *
     * @param newTimeout The new timeout value.
     */
    public synchronized void set( Timeout newTimeout )
    {
        delay = newTimeout.next();
        timeout = newTimeout;
        long jobId = newJobId();
        job = scheduler.schedule( group, () -> handle( jobId ), delay.amount(), delay.unit() );
    }

    private long newJobId()
    {
        activeJobId = activeJobId + 1;
        return activeJobId;
    }

    private void handle( long jobId )
    {
        synchronized ( this )
        {
            if ( activeJobId != jobId )
            {
                return;
            }
        }

        try
        {
            handler.onTimeout( this );
        }
        catch ( Throwable e )
        {
            log.error( format( "[%s] Handler threw exception", canonicalName() ), e );
        }
    }

    /**
     * Resets the timer based on the currently programmed timeout.
     */
    public synchronized void reset()
    {
        if ( timeout == null )
        {
            throw new IllegalStateException( "You can't reset until you have set a timeout" );
        }
        set( timeout );
    }

    /**
     * Deactivates the timer and cancels a currently running job.
     * <p>
     * Be careful to not have a timeout handler executing in parallel with a
     * timer, because this will just cancel the timer. If you for example
     * {@link #reset()} in the timeout handler, but keep executing the handler,
     * then a subsequent cancel will not ensure that the first execution of the
     * handler was cancelled.
     *
     * @param cancelMode The mode of cancelling.
     */
    public void cancel( CancelMode cancelMode )
    {
        JobScheduler.JobHandle job;

        synchronized ( this )
        {
            activeJobId++;
            job = this.job;
        }

        if ( job != null )
        {
            try
            {
                if ( cancelMode == CancelMode.SYNC_WAIT )
                {
                    job.waitTermination();
                }
                else if ( cancelMode == CancelMode.ASYNC_INTERRUPT )
                {
                    job.cancel( true );
                }
            }
            catch ( Exception e )
            {
                log.warn( format( "[%s] Cancelling timer threw exception", canonicalName() ), e );
            }
        }
    }

    /**
     * Schedules the timer for an immediate timeout.
     */
    public synchronized void invoke()
    {
        long jobId = newJobId();
        job = scheduler.schedule( group, () -> handle( jobId ) );
    }

    synchronized Delay delay()
    {
        return delay;
    }

    public TimerName name()
    {
        return name;
    }

    private String canonicalName()
    {
        return name.getClass().getCanonicalName() + "." + name.name();
    }

    public enum CancelMode
    {
        /**
         * Asynchronously cancels.
         */
        ASYNC,

        /**
         * Asynchronously cancels and interrupts the handler.
         */
        ASYNC_INTERRUPT,

        /**
         * Synchronously cancels and waits for the handler to finish.
         */
        SYNC_WAIT,

        /*
         * Note that SYNC_INTERRUPT cannot be supported, since the underlying
         * primitive is a future which cannot be cancelled/interrupted and awaited.
         */
    }
}
