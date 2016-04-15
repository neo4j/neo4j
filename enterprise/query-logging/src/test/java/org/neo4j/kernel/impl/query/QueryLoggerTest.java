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
package org.neo4j.kernel.impl.query;

import org.junit.Test;
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Factory;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.impl.query.QueryLoggerKernelExtension.QueryLogger;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class QueryLoggerTest
{
    @Test
    public void shouldLogQuerySlowerThanThreshold() throws Exception
    {
        // given
        StringLogger logger = mock( StringLogger.class );
        QuerySession session = session( "{the session}" );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = queryLoggerWithoutParams( logger, clock );
        queryLogger.init();

        // when
        queryLogger.startQueryExecution( session, "MATCH (n) RETURN n", Collections.<String,Object>emptyMap() );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session );

        // then
        verify( logger ).info( "SUCCESS 11 ms: {the session} - MATCH (n) RETURN n" );
    }

    @Test
    public void shouldNotLogQueryFasterThanThreshold() throws Exception
    {
        // given
        StringLogger logger = mock( StringLogger.class );
        QuerySession session = session( "{the session}" );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = queryLoggerWithoutParams( logger, clock );
        queryLogger.init();

        // when
        queryLogger.startQueryExecution( session, "MATCH (n) RETURN n", Collections.<String,Object>emptyMap() );
        clock.forward( 9, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session );

        // then
        verifyZeroInteractions( logger );
    }

    @Test
    public void shouldKeepTrackOfDifferentSessions() throws Exception
    {
        // given
        StringLogger logger = mock( StringLogger.class );
        QuerySession session1 = session( "{session one}" );
        QuerySession session2 = session( "{session two}" );
        QuerySession session3 = session( "{session three}" );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = queryLoggerWithoutParams( logger, clock );
        queryLogger.init();

        // when
        queryLogger.startQueryExecution( session1, "MATCH (a) RETURN a", Collections.<String,Object>emptyMap() );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.startQueryExecution( session2, "MATCH (b) RETURN b", Collections.<String,Object>emptyMap() );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.startQueryExecution( session3, "MATCH (c) RETURN c", Collections.<String,Object>emptyMap() );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session3 );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session2 );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session1 );

        // then
        InOrder order = inOrder( logger );
        order.verify( logger ).info( "SUCCESS 15 ms: {session two} - MATCH (b) RETURN b" );
        order.verify( logger ).info( "SUCCESS 23 ms: {session one} - MATCH (a) RETURN a" );
        verifyNoMoreInteractions( logger );
    }

    @Test
    public void shouldLogQueryOnFailureEvenIfFasterThanThreshold() throws Exception
    {
        // given
        StringLogger logger = mock( StringLogger.class );
        QuerySession session = session( "{the session}" );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = queryLoggerWithoutParams( logger, clock );
        queryLogger.init();
        RuntimeException failure = new RuntimeException();

        // when
        queryLogger.startQueryExecution( session, "MATCH (n) RETURN n", Collections.<String,Object>emptyMap() );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.endFailure( session, failure );

        // then
        verify( logger ).error( "FAILURE 1 ms: {the session} - MATCH (n) RETURN n", failure );
    }

    @Test
    public void shouldLogQueryParameters() throws Exception
    {
        // given
        StringLogger logger = mock( StringLogger.class );
        QuerySession session = session( "{the session}" );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = queryLoggerWithParams( logger, clock );
        queryLogger.init();

        // when
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        queryLogger.startQueryExecution( session, "MATCH (n) WHERE n.age IN {ages} RETURN n", params );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session );

        // then
        verify( logger ).info(
                "SUCCESS 11 ms: {the session} - MATCH (n) WHERE n.age IN {ages} RETURN n - {ages: [41, 42, 43]}" );
    }

    @Test
    public void shouldLogQueryParametersOnFailure() throws Exception
    {
        // given
        StringLogger logger = mock( StringLogger.class );
        QuerySession session = session( "{the session}" );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = queryLoggerWithParams( logger, clock );
        queryLogger.init();
        RuntimeException failure = new RuntimeException();

        // when
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        queryLogger.startQueryExecution( session, "MATCH (n) WHERE n.age IN {ages} RETURN n", params );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.endFailure( session, failure );

        // then
        verify( logger ).error(
                "FAILURE 1 ms: {the session} - MATCH (n) WHERE n.age IN {ages} RETURN n - {ages: [41, 42, 43]}",
                failure );
    }

    private static QueryLogger queryLoggerWithoutParams( StringLogger logger, Clock clock )
    {
        return new QueryLogger( clock, new LoggerFactory( logger ), 10/*ms*/, false );
    }

    private static QueryLogger queryLoggerWithParams( StringLogger logger, Clock clock )
    {
        return new QueryLogger( clock, new LoggerFactory( logger ), 10/*ms*/, true );
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

    private static class LoggerFactory implements Factory<StringLogger>
    {
        private final StringLogger logger;

        LoggerFactory( StringLogger logger )
        {
            this.logger = logger;
        }

        @Override
        public StringLogger newInstance()
        {
            return logger;
        }
    }
}
