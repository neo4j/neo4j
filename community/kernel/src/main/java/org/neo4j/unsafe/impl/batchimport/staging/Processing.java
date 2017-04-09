/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.staging;

import java.util.function.LongPredicate;

import org.neo4j.unsafe.impl.batchimport.executor.ParkStrategy;

import static java.lang.System.nanoTime;

public class Processing
{
    /**
     * Awaits a condition, parking if needed and will abort of health check doesn't pass.
     *
     * @param goalPredicate condition which will end the wait, if returning {@code true}.
     * @param goal to feed into the {@code goalPredicate}.
     * @param healthCheck to check as to not continue waiting if not passing.
     * @param park {@link ParkStrategy} for each tiny little wait.
     * @return how long time was spent in here, in nanos.
     */
    public static long await( LongPredicate goalPredicate, long goal, Runnable healthCheck, ParkStrategy park )
    {
        if ( goalPredicate.test( goal ) )
        {
            return 0;
        }

        long startTime = nanoTime();
        for ( int i = 0; i < 1_000_000 && !goalPredicate.test( goal ); i++ )
        {   // Busy loop a while
        }

        Thread thread = Thread.currentThread();
        while ( !goalPredicate.test( goal ) )
        {
            park.park( thread );
            healthCheck.run();
        }
        return nanoTime() - startTime;
    }
}
