/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.helper;

import java.util.concurrent.CancellationException;

import org.neo4j.function.ThrowingAction;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A robust job catches and logs any exceptions, but keeps running if the job
 * is a recurring one. Any exceptions also end up in the supplied log instead
 * of falling through to syserr. Remaining Throwables (generally errors) are
 * logged but fall through, so that recurring jobs are stopped and the error
 * gets double visibility.
 */
public class RobustJobSchedulerWrapper
{
    private final JobScheduler delegate;
    private final Log log;

    public RobustJobSchedulerWrapper( JobScheduler delegate, Log log )
    {
        this.delegate = delegate;
        this.log = log;
    }

    public JobScheduler.JobHandle schedule( String name, long delayMillis, ThrowingAction<Exception> action )
    {
        return delegate.schedule( new JobScheduler.Group( name ),
                () -> withErrorHandling( action ), delayMillis, MILLISECONDS );
    }

    public JobScheduler.JobHandle scheduleRecurring( String name, long periodMillis, ThrowingAction<Exception> action )
    {
        return delegate.scheduleRecurring( new JobScheduler.Group( name ),
                () -> withErrorHandling( action ), periodMillis, MILLISECONDS );
    }

    /**
     * Last line of defense error handling.
     */
    private void withErrorHandling( ThrowingAction<Exception> action )
    {
        try
        {
            action.apply();
        }
        catch ( Exception e )
        {
            log.warn( "Uncaught exception", e );
        }
        catch ( Throwable t )
        {
            log.error( "Uncaught error rethrown", t );
            throw t;
        }
    }
}
