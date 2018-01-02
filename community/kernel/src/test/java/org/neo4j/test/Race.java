/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

    public void addContestant( Runnable contestant )
    {
        contestants.add( new Contestant( contestant, contestants.size() ) );
    }

    public void go() throws Throwable
    {
        readySet = new CountDownLatch( contestants.size() );
        for ( Contestant contestant : contestants )
        {
            contestant.start();
        }
        readySet.await();
        go.countDown();

        int errorCount = 0;
        for ( Contestant contestant : contestants )
        {
            contestant.join();
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

            randomlyDelaySlightly();

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
