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
import org.mockito.InOrder;

import org.neo4j.function.Factory;
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
        QueryLogger queryLogger = new QueryLogger( clock, new LoggerFactory( logger ), 10/*ms*/ );
        queryLogger.init();

        // when
        queryLogger.startQueryExecution( session, "MATCH (n) RETURN n" );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session );

        // then
        verify( logger ).logMessage( "SUCCESS 11 ms: {the session} - MATCH (n) RETURN n" );
    }

    @Test
    public void shouldNotLogQueryFasterThanThreshold() throws Exception
    {
        // given
        StringLogger logger = mock( StringLogger.class );
        QuerySession session = session( "{the session}" );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = new QueryLogger( clock, new LoggerFactory( logger ), 10/*ms*/ );
        queryLogger.init();

        // when
        queryLogger.startQueryExecution( session, "MATCH (n) RETURN n" );
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
        QueryLogger queryLogger = new QueryLogger( clock, new LoggerFactory( logger ), 10/*ms*/ );
        queryLogger.init();

        // when
        queryLogger.startQueryExecution( session1, "MATCH (a) RETURN a" );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.startQueryExecution( session2, "MATCH (b) RETURN b" );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.startQueryExecution( session3, "MATCH (c) RETURN c" );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session3 );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session2 );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( session1 );

        // then
        InOrder order = inOrder( logger );
        order.verify( logger ).logMessage( "SUCCESS 15 ms: {session two} - MATCH (b) RETURN b" );
        order.verify( logger ).logMessage( "SUCCESS 23 ms: {session one} - MATCH (a) RETURN a" );
        verifyNoMoreInteractions( logger );
    }

    @Test
    public void shouldLogQueryOnFailureEvenIfFasterThanThreshold() throws Exception
    {
        // given
        StringLogger logger = mock( StringLogger.class );
        QuerySession session = session( "{the session}" );
        FakeClock clock = new FakeClock();
        QueryLogger queryLogger = new QueryLogger( clock, new LoggerFactory( logger ), 10/*ms*/ );
        queryLogger.init();
        RuntimeException failure = new RuntimeException();

        // when
        queryLogger.startQueryExecution( session, "MATCH (n) RETURN n" );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.endFailure( session, failure );

        // then
        verify( logger ).logMessage( "FAILURE 1 ms: {the session} - MATCH (n) RETURN n", failure );
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
