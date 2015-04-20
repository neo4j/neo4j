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
package org.neo4j.kernel.impl.query;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.neo4j.helpers.FakeClock;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.kernel.impl.query.QueryLoggerKernelExtension.QueryLogger;

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

    @Test
    public void shouldLogQuerySlowerThanThreshold() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        QuerySession session = session( SESSION_1_NAME );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = new QueryLogger( clock, logProvider.getLog( getClass() ), 10/*ms*/ );

        // when
        queryLogger.startQueryExecution( session, QUERY_1 );
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
        QueryLogger queryLogger = new QueryLogger( clock, logProvider.getLog( getClass() ), 10/*ms*/ );

        // when
        queryLogger.startQueryExecution( session, QUERY_1 );
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
        QueryLogger queryLogger = new QueryLogger( clock, logProvider.getLog( getClass() ), 10/*ms*/ );

        // when
        queryLogger.startQueryExecution( session1, QUERY_1 );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.startQueryExecution( session2, QUERY_2 );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.startQueryExecution( session3, QUERY_3 );
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
        QueryLogger queryLogger = new QueryLogger( clock, logProvider.getLog( getClass() ), 10/*ms*/ );
        RuntimeException failure = new RuntimeException();

        // when
        queryLogger.startQueryExecution( session, QUERY_1 );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.endFailure( session, failure );

        // then
        logProvider.assertExactly(
                inLog( getClass() ).error( is( "1 ms: {session one} - MATCH (n) RETURN n" ), sameInstance( failure ) )
        );
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
