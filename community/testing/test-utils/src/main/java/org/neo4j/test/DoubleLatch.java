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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DoubleLatch
{
    private static final long FIVE_MINUTES = TimeUnit.MINUTES.toMillis( 5 );
    private final CountDownLatch startSignal;
    private final CountDownLatch finishSignal;
    private final boolean awaitUninterruptibly;

    public DoubleLatch()
    {
        this( 1 );
    }

    public DoubleLatch( int numberOfContestants )
    {
        this( numberOfContestants, false );
    }

    public DoubleLatch( int numberOfContestants, boolean awaitUninterruptibly )
    {
        this.startSignal = new CountDownLatch( numberOfContestants );
        this.finishSignal = new CountDownLatch( numberOfContestants );
        this.awaitUninterruptibly = awaitUninterruptibly;
    }

    public void startAndWaitForAllToStartAndFinish()
    {
        startAndWaitForAllToStart();
        waitForAllToFinish();
    }

    public void startAndWaitForAllToStart()
    {
        start();
        waitForAllToStart();
    }

    public void start()
    {
        startSignal.countDown();
    }

    public void waitForAllToStart()
    {
        awaitLatch( startSignal, awaitUninterruptibly );
    }

    public void finishAndWaitForAllToFinish()
    {
        finish();
        waitForAllToFinish();
    }

    public void finish()
    {
        finishSignal.countDown();
    }

    public void waitForAllToFinish()
    {
        awaitLatch( finishSignal, awaitUninterruptibly );
    }

    public static void awaitLatch( CountDownLatch latch )
    {
        awaitLatch( latch, false );
    }

    public static void awaitLatch( CountDownLatch latch, boolean uninterruptedWaiting )
    {
        long now = System.currentTimeMillis();
        long deadline = System.currentTimeMillis() + FIVE_MINUTES;

        while ( now < deadline )
        {
            try
            {

                long waitingTime = Math.min( Math.max(0, deadline - now), 5000L );
                if ( latch.await( waitingTime, TimeUnit.MILLISECONDS ) )
                {
                    return;
                }
                else
                {
                    Thread.yield();
                }
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                if ( ! uninterruptedWaiting )
                {
                    throw new RuntimeException( "Thread interrupted while waiting on latch", e );
                }
            }
            now = System.currentTimeMillis();
        }
        throw new AssertionError( "Latch specified waiting time elapsed." );
    }

    @Override
    public String toString()
    {
        return super.toString() + "[Start[" + startSignal.getCount() + "], Finish[" + finishSignal.getCount() + "]]";
    }
}
