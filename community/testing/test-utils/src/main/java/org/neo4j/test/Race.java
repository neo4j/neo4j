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
package org.neo4j.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

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
    private static final int UNLIMITED = 0;

    public interface ThrowingRunnable
    {
        void run() throws Throwable;
    }

    private final List<Contestant> contestants = new ArrayList<>();
    private volatile CountDownLatch readySet;
    private final CountDownLatch go = new CountDownLatch( 1 );
    private volatile boolean addSomeMinorRandomStartDelays;
    private volatile BooleanSupplier endCondition;
    private volatile boolean failure;
    private boolean asyncExecution;

    public Race withRandomStartDelays()
    {
        this.addSomeMinorRandomStartDelays = true;
        return this;
    }

    /**
     * Adds an end condition to this race. The race will end whenever an end condition is met
     * or when there's one contestant failing (throwing any sort of exception).
     *
     * @param endConditions one or more end conditions, such that when returning {@code true}
     * signals that the race should end.
     * @return this {@link Race} instance.
     */
    public Race withEndCondition( BooleanSupplier... endConditions )
    {
        for ( BooleanSupplier endCondition : endConditions )
        {
            this.endCondition = mergeEndCondition( endCondition );
        }
        return this;
    }

    /**
     * Convenience for adding an end condition which is based on time. This will have contestants
     * end after the given duration (time + unit).
     *
     * @param time time value.
     * @param unit unit of time in {@link TimeUnit}.
     * @return this {@link Race} instance.
     */
    public Race withMaxDuration( long time, TimeUnit unit )
    {
        long endTime = currentTimeMillis() + unit.toMillis( time );
        this.endCondition = mergeEndCondition( () -> currentTimeMillis() >= endTime );
        return this;
    }

    private BooleanSupplier mergeEndCondition( BooleanSupplier additionalEndCondition )
    {
        BooleanSupplier existingEndCondition = endCondition;
        return existingEndCondition == null ? additionalEndCondition :
            () -> existingEndCondition.getAsBoolean() || additionalEndCondition.getAsBoolean();
    }

    /**
     * Convenience for wrapping contestants, especially for lambdas, which throws any sort of
     * checked exception.
     *
     * @param runnable actual contestant.
     * @return contestant wrapped in a try-catch (and re-throw as unchecked exception).
     */
    public static Runnable throwing( ThrowingRunnable runnable )
    {
        return () ->
        {
            try
            {
                runnable.run();
            }
            catch ( Throwable e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    public void addContestants( int count, Runnable contestant )
    {
        addContestants( count, contestant, UNLIMITED );
    }

    public void addContestants( int count, Runnable contestant, int maxNumberOfRuns )
    {
        for ( int i = 0; i < count; i++ )
        {
            addContestant( contestant, maxNumberOfRuns );
        }
    }

    public void addContestant( Runnable contestant )
    {
        addContestant( contestant, UNLIMITED );
    }

    public void addContestant( Runnable contestant, int maxNumberOfRuns )
    {
        contestants.add( new Contestant( contestant, contestants.size(), maxNumberOfRuns ) );
    }

    /**
     * Starts the race and returns without waiting for contestants to complete.
     * Any exception thrown by contestant will be lost.
     */
    public void goAsync() throws Throwable
    {
        asyncExecution = true;
        go( 0, TimeUnit.MILLISECONDS );
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
        if ( endCondition == null )
        {
            endCondition = () -> true;
        }

        readySet = new CountDownLatch( contestants.size() );
        for ( Contestant contestant : contestants )
        {
            contestant.start();
        }
        readySet.await();
        go.countDown();

        if ( asyncExecution )
        {
            return;
        }

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
                long time = currentTimeMillis();
                contestant.join( maxWaitTimeMillis - waitedSoFar );
                waitedSoFar += currentTimeMillis() - time;
                if ( waitedSoFar >= maxWaitTimeMillis )
                {
                    throw new TimeoutException( "Didn't complete after " + maxWaitTime + " " + unit );
                }
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
        private final int maxNumberOfRuns;
        private int runs;

        Contestant( Runnable code, int nr, int maxNumberOfRuns )
        {
            super( code, "Contestant#" + nr );
            this.maxNumberOfRuns = maxNumberOfRuns;
            this.setUncaughtExceptionHandler( ( thread, error ) ->
            {
            } );
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
                while ( !failure )
                {
                    super.run();
                    if ( (maxNumberOfRuns != UNLIMITED && ++runs == maxNumberOfRuns) || endCondition.getAsBoolean() )
                    {
                        break;
                    }
                }
            }
            catch ( Throwable e )
            {
                error = e;
                failure = true; // <-- global flag
                throw e;
            }
        }

        private void randomlyDelaySlightly()
        {
            int millis = ThreadLocalRandom.current().nextInt( 100 );
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 + millis ) );
        }
    }
}
