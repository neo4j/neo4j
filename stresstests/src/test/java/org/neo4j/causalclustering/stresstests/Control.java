/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.stresstests;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import org.neo4j.concurrent.Futures;
import org.neo4j.logging.Log;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.function.Suppliers.untilTimeExpired;

public class Control
{
    private final AtomicBoolean stopTheWorld = new AtomicBoolean();
    private final BooleanSupplier keepGoing;
    private final Log log;
    private final long totalDurationMinutes;
    private Throwable failure;

    public Control( Config config )
    {
        this.log = config.logProvider().getLog( getClass() );
        long workDurationMinutes = config.workDurationMinutes();
        this.totalDurationMinutes = workDurationMinutes + config.shutdownDurationMinutes();

        BooleanSupplier notExpired = untilTimeExpired( workDurationMinutes, MINUTES );
        this.keepGoing = () -> !stopTheWorld.get() && notExpired.getAsBoolean();
    }

    public boolean keepGoing()
    {
        return keepGoing.getAsBoolean();
    }

    public synchronized void onFailure( Throwable cause )
    {
        if ( failure == null )
        {
            failure = cause;
        }
        else
        {
            failure.addSuppressed( cause );
        }
        log.error( "Failure occurred", cause );
        stopTheWorld.set( true );
    }

    public synchronized void assertNoFailure()
    {
        if ( failure != null )
        {
            throw new RuntimeException( "Test failed", failure );
        }
    }

    public void awaitEnd( Iterable<Future<?>> completions ) throws InterruptedException, TimeoutException, ExecutionException
    {
        Futures.combine( completions ).get( totalDurationMinutes, MINUTES );
    }
}
