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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DoubleLatch
{
    private static final int FIVE_MINUTES = 5 * 60 * 1000;
    private final CountDownLatch startSignal;
    private final CountDownLatch finishSignal;
    private final boolean uninterruptedWaiting;

    public DoubleLatch()
    {
        this( 1 );
    }

    public DoubleLatch( int numberOfContestants )
    {
        this( numberOfContestants, false );
    }

    public DoubleLatch( int numberOfContestants, boolean uninterruptedWaiting )
    {
        this.startSignal = new CountDownLatch( numberOfContestants );
        this.finishSignal = new CountDownLatch( numberOfContestants );
        this.uninterruptedWaiting = uninterruptedWaiting;
    }

    public void waitForAllToStart()
    {
        awaitLatch( startSignal, uninterruptedWaiting );
    }

    public void startAndWaitForAllToStart()
    {
        startSignal.countDown();
        awaitLatch( startSignal, uninterruptedWaiting );
    }

    public void startAndWaitForAllToStartAndFinish()
    {
        startAndWaitForAllToStart();
        awaitLatch( finishSignal, uninterruptedWaiting );
    }

    public void finish()
    {
        finishSignal.countDown();
    }

    public void finishAndWaitForAllToFinish()
    {
        waitForAllToFinish();
    }

    public void waitForAllToFinish()
    {
        awaitLatch( finishSignal, uninterruptedWaiting );
    }

    public static void awaitLatch( CountDownLatch latch )
    {
        awaitLatch( latch, false );
    }

    public static void awaitLatch( CountDownLatch latch, boolean uninterruptedWaiting )
    {
        long now = System.currentTimeMillis();
        long deadline = now + FIVE_MINUTES;

        while ( now < deadline )
        {
            try
            {

                long waitingTime = Math.min( Math.max(0, deadline - now), 1000L );
                latch.await( waitingTime, TimeUnit.MILLISECONDS );
                Thread.yield();
                return;
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
