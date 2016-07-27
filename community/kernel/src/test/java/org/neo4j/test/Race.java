/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Simple race scenario, a utility for executing multiple threads coordinated to start at the same time.
 * Add contestants with {@link #addContestant(Runnable)} and then when all have been added, start them
 * simultaneously using {@link #go()}, which will block until all contestants have completed.
 * Any errors from contestants are propagated out from {@link #go()}.
 */
public class Race
{
    private final List<Contestant> contestants = new ArrayList<>();
    private volatile CountDownLatch readySet;
    private final CountDownLatch go = new CountDownLatch( 1 );
    private final boolean addSomeMinorRandomStartDelays;

    public Race()
    {
        this( false );
    }

    public Race( boolean addSomeMinorRandomStartDelays )
    {
        this.addSomeMinorRandomStartDelays = addSomeMinorRandomStartDelays;
    }

    public void addContestant( Runnable contestant )
    {
        contestants.add( new Contestant( contestant, contestants.size() ) );
    }

    /**
     * Starts the race and waits indefinitely for all contestants to either fail or succeed.
     *
     * @throws Throwable on any exception thrown from any contestant.
     */
    public void go() throws Throwable
    {
        go( 0, TimeUnit.MILLISECONDS );
    }

    /**
     * Starts the race and waits {@code maxWaitTime} for all contestants to either fail or succeed.
     *
     * @param maxWaitTime max time to wait for all contestants, 0 means indefinite wait.
     * @param unit {@link TimeUnit} that {£{@code maxWaitTime} is given in.
     * @throws TimeoutException if all contestants haven't either succeeded or failed within the given time.
     * @throws Throwable on any exception thrown from any contestant.
     */
    public void go( long maxWaitTime, TimeUnit unit ) throws Throwable
    {
        readySet = new CountDownLatch( contestants.size() );
        for ( Contestant contestant : contestants )
        {
            contestant.start();
        }
        readySet.await();
        go.countDown();

        int errorCount = 0;
        long maxWaitTimeMillis = MILLISECONDS.convert( maxWaitTime, unit );
        long waitedSoFar = 0;
        for ( Contestant contestant : contestants )
        {
            if ( maxWaitTime == 0 )
            {
                contestant.join();
            }
            else
            {
                if ( waitedSoFar >= maxWaitTimeMillis )
                {
                    throw new TimeoutException( "Didn't complete after " + maxWaitTime + " " + unit );
                }
                long time = currentTimeMillis();
                contestant.join( maxWaitTimeMillis - waitedSoFar );
                waitedSoFar += (currentTimeMillis() - time);
            }
            if ( contestant.error != null )
            {
                errorCount++;
            }
        }

        if ( errorCount > 1 )
        {
            Throwable errors = new Throwable( "Multiple errors found" );
            for ( Contestant contestant : contestants )
            {
                if ( contestant.error != null )
                {
                    errors.addSuppressed( contestant.error );
                }
            }
            throw errors;
        }
        if ( errorCount == 1 )
        {
            for ( Contestant contestant : contestants )
            {
                if ( contestant.error != null )
                {
                    throw contestant.error;
                }
            }
        }
    }

    private class Contestant extends Thread
    {
        private volatile Throwable error;

        Contestant( Runnable code, int nr )
        {
            super( code, "Contestant#" + nr );
        }

        @Override
        public void run()
        {
            readySet.countDown();
            try
            {
                go.await();
            }
            catch ( InterruptedException e )
            {
                error = e;
                interrupt();
                return;
            }

            if ( addSomeMinorRandomStartDelays )
            {
                randomlyDelaySlightly();
            }

            try
            {
                super.run();
            }
            catch ( Throwable e )
            {
                error = e;
                throw e;
            }
        }

        private void randomlyDelaySlightly()
        {
            int target = ThreadLocalRandom.current().nextInt( 1_000_000_000 );
            for ( int i = 0; i < target; i++ )
            {
                i = i;
            }
        }
    }
}
