/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha.lock.forseti;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;

import static java.util.Arrays.asList;

/**
 * Kept for future reference. A simple program that concurrently acquires and releases locks and tracks the number of deadlocks that occur.
 */
public class ForsetiDeadlockResolutionExploration
{
    public static final int ITERATIONS = 100_000;

    public static void main(String ... args) throws InterruptedException, ExecutionException
    {
        ForsetiLockManager locks = new ForsetiLockManager( ResourceTypes.NODE );
        ExecutorService pool = Executors.newFixedThreadPool( 4 );

        for ( int i = 0; i < 5; i++ )
        {
            List<Future<Long>> futures = pool.invokeAll( asList( worker( locks.newClient() ), worker( locks.newClient() ), worker( locks.newClient() ) ) );

            long deadlocks = 0;
            for ( Future<Long> future : futures )
            {
                deadlocks += future.get();
            }

            System.out.println("Round["+i+"]: " + deadlocks );
        }

        pool.shutdown();
    }

    private static Callable<Long> worker( final Locks.Client client )
    {
        return new Callable<Long>()
        {
            private final Random rand = new Random();

            @Override
            public Long call() throws Exception
            {
                long deadlocks = 0;
                for ( int i = 0; i < ITERATIONS; i++ )
                {
                    try
                    {
                        for ( int j = 0; j < 3; j++ )
                        {
                            int todo = rand.nextInt( 8 );
                            if( todo <= 3 )
                            {
                                client.acquireExclusive( ResourceTypes.NODE, todo );
                            }
                            else
                            {
                                client.acquireShared( ResourceTypes.NODE, todo - 4 );
                            }
                        }
                    }
                    catch( DeadlockDetectedException e )
                    {
                        deadlocks++;
                    }
                    finally
                    {
                        client.releaseAll();
                    }
                }
                return deadlocks;
            }
        };
    }
}
