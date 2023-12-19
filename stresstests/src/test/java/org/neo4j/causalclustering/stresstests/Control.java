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
