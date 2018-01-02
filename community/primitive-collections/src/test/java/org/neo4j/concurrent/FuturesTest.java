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
package org.neo4j.concurrent;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.*;

public class FuturesTest
{
    private static final Runnable NOOP = new Runnable()
    {
        @Override
        public void run()
        {
        }
    };

    @Test
    public void combinedFutureShouldGetResultsAfterAllComplete() throws Exception
    {
        FutureTask<String> task1 = new FutureTask<>( NOOP, "1" );
        FutureTask<String> task2 = new FutureTask<>( NOOP, "2" );
        FutureTask<String> task3 = new FutureTask<>( NOOP, "3" );

        Future<List<String>> combined = Futures.combine( task1, task2, task3 );

        try
        {
            combined.get( 10, TimeUnit.MILLISECONDS );
            fail( "should have timedout" );
        } catch ( TimeoutException e )
        {
            // continue
        }

        task3.run();
        task2.run();

        try
        {
            combined.get( 10, TimeUnit.MILLISECONDS );
            fail( "should have timedout" );
        } catch ( TimeoutException e )
        {
            // continue
        }

        task1.run();

        List<String> result = combined.get();
        assertThat( result, contains( "1", "2", "3" ) );
    }
}
