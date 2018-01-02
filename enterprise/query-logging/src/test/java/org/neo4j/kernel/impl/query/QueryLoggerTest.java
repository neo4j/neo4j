/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.query;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Clock;
import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.impl.query.QueryLoggerKernelExtension.QueryLogger;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;

import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class QueryLoggerTest
{
    public static final String SESSION_1_NAME = "{session one}";
    public static final String SESSION_2_NAME = "{session two}";
    public static final String SESSION_3_NAME = "{session three}";
    public static final String QUERY_1 = "MATCH (n) RETURN n";
    public static final String QUERY_2 = "MATCH (a)--(b) RETURN b.name";
    public static final String QUERY_3 = "MATCH (c)-[:FOO]->(d) RETURN d.size";
    public static final String QUERY_4 = "MATCH (n) WHERE n.age IN {ages} RETURN n";

    @Test
    public void shouldLogQuerySlowerThanThreshold() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        QuerySession session = session( SESSION_1_NAME );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = queryLoggerWithoutParams( logProvider, clock );

        // when
        queryLogger.startQueryExecution( session, QUERY_1, Collections.<String,Object>emptyMap() );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session );

        // then
        logProvider.assertExactly(
                inLog( getClass() ).info( "%d ms: %s - %s", 11L, SESSION_1_NAME, QUERY_1 )
        );
    }

    @Test
    public void shouldNotLogQueryFasterThanThreshold() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        QuerySession session = session( SESSION_1_NAME );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = queryLoggerWithoutParams( logProvider, clock );

        // when
        queryLogger.startQueryExecution( session, QUERY_1, Collections.<String,Object>emptyMap() );
        clock.forward( 9, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session );

        // then
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void shouldKeepTrackOfDifferentSessions() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        QuerySession session1 = session( SESSION_1_NAME );
        QuerySession session2 = session( SESSION_2_NAME );
        QuerySession session3 = session( SESSION_3_NAME );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = queryLoggerWithoutParams( logProvider, clock );

        // when
        queryLogger.startQueryExecution( session1, QUERY_1, Collections.<String,Object>emptyMap() );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.startQueryExecution( session2, QUERY_2, Collections.<String,Object>emptyMap() );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.startQueryExecution( session3, QUERY_3, Collections.<String,Object>emptyMap() );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session3 );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session2 );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session1 );

        // then
        logProvider.assertExactly(
                inLog( getClass() ).info( "%d ms: %s - %s", 15L, SESSION_2_NAME, QUERY_2 ),
                inLog( getClass() ).info( "%d ms: %s - %s", 23L, SESSION_1_NAME, QUERY_1 )
        );
    }

    @Test
    public void shouldLogQueryOnFailureEvenIfFasterThanThreshold() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        QuerySession session = session( SESSION_1_NAME );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = queryLoggerWithoutParams( logProvider, clock );
        RuntimeException failure = new RuntimeException();

        // when
        queryLogger.startQueryExecution( session, QUERY_1, Collections.<String,Object>emptyMap() );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.endFailure( session, failure );

        // then
        logProvider.assertExactly(
                inLog( getClass() ).error( is( "1 ms: {session one} - MATCH (n) RETURN n" ), sameInstance( failure ) )
        );
    }

    @Test
    public void shouldLogQueryParameters() throws Exception
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        QuerySession session = session( SESSION_1_NAME );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = queryLoggerWithParams( logProvider, clock );

        // when
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        queryLogger.startQueryExecution( session, QUERY_4, params );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session );

        // then
        logProvider.assertExactly(
                inLog( getClass() ).info( "%d ms: %s - %s - %s", 11L, SESSION_1_NAME, QUERY_4, "{ages: [41, 42, 43]}" )
        );
    }

    @Test
    public void shouldLogQueryParametersOnFailure() throws Exception
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        QuerySession session = session( SESSION_2_NAME );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = queryLoggerWithParams( logProvider, clock );
        RuntimeException failure = new RuntimeException();

        // when
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        queryLogger.startQueryExecution( session, QUERY_4, params );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.endFailure( session, failure );

        // then
        logProvider.assertExactly(
                inLog( getClass() ).error(
                        is( "1 ms: {session two} - MATCH (n) WHERE n.age IN {ages} RETURN n - {ages: [41, 42, 43]}" ),
                        sameInstance( failure ) )
        );
    }

    private QueryLogger queryLoggerWithoutParams( LogProvider logProvider, Clock clock )
    {
        return new QueryLogger( clock, logProvider.getLog( getClass() ), 10/*ms*/, false );
    }

    private QueryLogger queryLoggerWithParams( LogProvider logProvider, Clock clock )
    {
        return new QueryLogger( clock, logProvider.getLog( getClass() ), 10/*ms*/, true );
    }

    private static QuerySession session( final String data )
    {
        return new QuerySession()
        {
            @Override
            public String toString()
            {
                return data;
            }
        };
    }
}
