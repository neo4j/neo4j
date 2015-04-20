/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
    private final int numberOfContestants;

    public DoubleLatch()
    {
        this( 1 );
    }

    public DoubleLatch( int numberOfContestants )
    {
        this.numberOfContestants = numberOfContestants;
        this.startSignal = new CountDownLatch( numberOfContestants );
        this.finishSignal = new CountDownLatch( numberOfContestants );
    }

    public int getNumberOfContestants()
    {
        return numberOfContestants;
    }

    public void startAndAwaitFinish()
    {
        start();
        awaitLatch( finishSignal );
    }

    public void awaitStart()
    {
        awaitLatch( startSignal );
    }

    public void start()
    {
        startSignal.countDown();
        awaitLatch( startSignal );
    }

    public void finish()
    {
        finishSignal.countDown();
    }

    public void awaitFinish()
    {
        awaitLatch( finishSignal );
    }

    public static void awaitLatch( CountDownLatch latch )
    {
        long deadline = System.currentTimeMillis() + FIVE_MINUTES;
        long remaining;

        while( ( remaining = deadline - System.currentTimeMillis() ) >= 0 )
        {
            try
            {
                latch.await( remaining, TimeUnit.MILLISECONDS  );
                return;
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                new RuntimeException( "Thread interrupted while waiting on latch", e).printStackTrace();
            }
            Thread.yield();
        }
        throw new RuntimeException( "Failed to acquire latch" );
    }

    @Override
    public String toString()
    {
        return super.toString() + "[Start[" + startSignal.getCount() + "], Finish[" + finishSignal.getCount() + "]]";
    }
}
