/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.enterprise.builtinprocs;

import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

public class BuiltInProceduresIT
{
    private GraphDatabaseService gds = null;

    @Before
    public void init()
    {
        gds = new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase();
    }

    @After
    public void tearDown()
    {
        if ( gds != null )
        {
            gds.shutdown();
        }
    }

    @Test
    public void listQueries() throws Throwable
    {
        // Given
        String q1 = "UNWIND [1,2,3] AS x RETURN x";
        String q2 = "UNWIND [4,5,6] AS x RETURN x";
        ThreadedTestController controller = new ThreadedTestController( q1, q2 );

        controller.waitForAllThreadsToStart();

        // When
        String query = "CALL dbms.listQueries()";
        Result result = gds.execute( query );

        // Then
        List<Map<String,Object>> actual = result.stream().collect( Collectors.toList() );
        assertThat( actual, hasItems(
                hasQuery( q1 ),
                hasQuery( q2 ),
                hasQuery( query ) ) );

        controller.waitForThreadsToFinish();
    }

    private class ThreadedTestController
    {
        private final DoubleLatch latch;
        private final String[] queries;
        private final ArrayList<Exception> failures = new ArrayList<>();

        ThreadedTestController( String... queries )
        {
            this.latch = new DoubleLatch( queries.length + 1, true );
            this.queries = queries;
        }

        void waitForAllThreadsToStart()
        {
            for ( String query : queries )
            {
                Runnable runnable = runQuery( latch, query, failures );
                new Thread( runnable ).start();
            }
        }

        void waitForThreadsToFinish() throws Exception
        {
            latch.finishAndWaitForAllToFinish();
            for ( Exception e : failures )
            {
                throw e;
            }
        }

        private Runnable runQuery( DoubleLatch latch, String query, ArrayList<Exception> failures )
        {
            return () ->
            {
                try
                {
                    Result result = gds.execute( query );
                    latch.startAndWaitForAllToStart();
                    latch.finishAndWaitForAllToFinish();
                    result.close();

                }
                catch ( Exception e )
                {
                    failures.add( e );
                }
            };
        }
    }

    @SuppressWarnings( "unchecked" )
    private Matcher<Map<String,Object>> hasQuery( String query )
    {
        return (Matcher<Map<String,Object>>) (Matcher) hasEntry( equalTo( "query" ), equalTo( query ) );
    }
}
