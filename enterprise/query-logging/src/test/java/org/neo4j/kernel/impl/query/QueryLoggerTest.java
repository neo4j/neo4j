/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.ValueUtils;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorCounters;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.kernel.impl.query.clientconnection.ShellConnectionInfo;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.FakeCpuClock;
import org.neo4j.test.FakeHeapAllocation;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.query.QueryLogEntryContent.LOG_ALLOCATED_BYTES;
import static org.neo4j.kernel.impl.query.QueryLogEntryContent.LOG_DETAILED_TIME;
import static org.neo4j.kernel.impl.query.QueryLogEntryContent.LOG_PAGE_DETAILS;
import static org.neo4j.kernel.impl.query.QueryLogEntryContent.LOG_PARAMETERS;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class QueryLoggerTest
{
    private static final ClientConnectionInfo SESSION_1 = new ShellConnectionInfo( "{session one}" );
    private static final ClientConnectionInfo SESSION_2 = new ShellConnectionInfo( "{session two}" );
    private static final ClientConnectionInfo SESSION_3 = new ShellConnectionInfo( "{session three}" );
    private static final String QUERY_1 = "MATCH (n) RETURN n";
    private static final String QUERY_2 = "MATCH (a)--(b) RETURN b.name";
    private static final String QUERY_3 = "MATCH (c)-[:FOO]->(d) RETURN d.size";
    private static final String QUERY_4 = "MATCH (n) WHERE n.age IN {ages} RETURN n";
    private final FakeClock clock = Clocks.fakeClock();
    @Rule
    public final FakeCpuClock cpuClock = new FakeCpuClock();
    @Rule
    public final FakeHeapAllocation heapAllocation = new FakeHeapAllocation();
    private long pageHits;
    private long pageFaults;
    private long thresholdInMillis = 10;

    @Test
    public void shouldLogQuerySlowerThanThreshold() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        QueryLogger queryLogger = queryLogger( logProvider );

        // when
        queryLogger.startQueryExecution( query );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query );

        // then
        String expectedSessionString = sessionConnectionDetails( SESSION_1, "TestUser" );
        logProvider.assertExactly(
            inLog( getClass() ).info( format( "%d ms: %s - %s - {}", 11L, expectedSessionString, QUERY_1 ) )
        );
    }

    @Test
    public void shouldRespectThreshold() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        QueryLogger queryLogger = queryLogger( logProvider );

        // when
        queryLogger.startQueryExecution( query );
        clock.forward( 9, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query );

        // then
        logProvider.assertNoLoggingOccurred();

        // and when
        ExecutingQuery query2 = query( SESSION_2, "TestUser2", QUERY_2 );
        thresholdInMillis = 5;
        queryLogger.startQueryExecution( query2 );
        clock.forward( 9, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query2 );

        // then
        String expectedSessionString = sessionConnectionDetails( SESSION_2, "TestUser2" );
        logProvider.assertExactly(
                inLog( getClass() ).info( format( "%d ms: %s - %s - {}", 9L, expectedSessionString, QUERY_2 ) )
        );
    }

    @Test
    public void shouldKeepTrackOfDifferentSessions() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        ExecutingQuery query1 = query( SESSION_1, "TestUser1", QUERY_1 );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        ExecutingQuery query2 = query( SESSION_2, "TestUser2", QUERY_2 );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        ExecutingQuery query3 = query( SESSION_3, "TestUser3", QUERY_3 );

        QueryLogger queryLogger = queryLogger( logProvider );

        // when
        queryLogger.startQueryExecution( query1 );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.startQueryExecution( query2 );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.startQueryExecution( query3 );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query3 );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query2 );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query1 );

        // then
        String expectedSession1String = sessionConnectionDetails( SESSION_1, "TestUser1" );
        String expectedSession2String = sessionConnectionDetails( SESSION_2, "TestUser2" );
        logProvider.assertExactly(
                inLog( getClass() ).info( format( "%d ms: %s - %s - {}", 17L, expectedSession2String, QUERY_2 ) ),
                inLog( getClass() ).info( format( "%d ms: %s - %s - {}", 25L, expectedSession1String, QUERY_1 ) )
        );
    }

    @Test
    public void shouldLogQueryOnFailureEvenIfFasterThanThreshold() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        QueryLogger queryLogger = queryLogger( logProvider );
        RuntimeException failure = new RuntimeException();

        // when
        queryLogger.startQueryExecution( query );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.endFailure( query, failure );

        // then
        logProvider.assertExactly(
                inLog( getClass() )
                        .error( is( "1 ms: " + sessionConnectionDetails( SESSION_1, "TestUser" )
                                + " - MATCH (n) RETURN n - {}" ), sameInstance( failure ) )
        );
    }

    @Test
    public void shouldLogQueryParameters() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_4, params, emptyMap() );
        QueryLogger queryLogger = queryLogger( logProvider, LOG_PARAMETERS );

        // when
        queryLogger.startQueryExecution( query );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query );

        // then
        String expectedSessionString = sessionConnectionDetails( SESSION_1, "TestUser" );
        logProvider.assertExactly(
            inLog( getClass() ).info( format( "%d ms: %s - %s - %s - {}", 11L, expectedSessionString, QUERY_4,
                    "{ages: " +
                    "[41, 42, 43]}" ) )
        );
    }

    @Test
    public void shouldLogQueryParametersOnFailure() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_4, params, emptyMap() );
        QueryLogger queryLogger = queryLogger( logProvider, LOG_PARAMETERS );
        RuntimeException failure = new RuntimeException();

        // when
        queryLogger.startQueryExecution( query );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.endFailure( query, failure );

        // then
        logProvider.assertExactly(
            inLog( getClass() ).error(
                    is( "1 ms: " + sessionConnectionDetails( SESSION_1, "TestUser" )
                            + " - MATCH (n) WHERE n.age IN {ages} RETURN n - {ages: [41, 42, 43]} - {}" ),
                sameInstance( failure ) )
        );
    }

    @Test
    public void shouldLogUserName() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        QueryLogger queryLogger = queryLogger( logProvider );

        // when
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        queryLogger.startQueryExecution( query );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query );

        ExecutingQuery anotherQuery = query( SESSION_1, "AnotherUser", QUERY_1 );
        queryLogger.startQueryExecution( anotherQuery );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( anotherQuery );

        // then
        logProvider.assertExactly(
                inLog( getClass() ).info( format( "%d ms: %s - %s - {}", 10L,
                        sessionConnectionDetails( SESSION_1, "TestUser" ), QUERY_1 ) ),
                inLog( getClass() ).info( format( "%d ms: %s - %s - {}", 10L,
                        sessionConnectionDetails( SESSION_1, "AnotherUser" ), QUERY_1 ) )
        );
    }

    @Test
    public void shouldLogMetaData() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        QueryLogger queryLogger = queryLogger( logProvider );

        // when
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1, emptyMap(), map( "User", "UltiMate" ) );
        queryLogger.startQueryExecution( query );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query );

        ExecutingQuery anotherQuery =
                query( SESSION_1, "AnotherUser", QUERY_1, emptyMap(), map( "Place", "Town" ) );
        queryLogger.startQueryExecution( anotherQuery );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        Throwable error = new Throwable();
        queryLogger.endFailure( anotherQuery, error );

        // then
        logProvider.assertExactly(
                inLog( getClass() ).info( format( "%d ms: %s - %s - {User: 'UltiMate'}", 10L,
                        sessionConnectionDetails( SESSION_1, "TestUser" ), QUERY_1
                ) ),
                inLog( getClass() ).error(
                        equalTo( format( "%d ms: %s - %s - {Place: 'Town'}", 10L,
                            sessionConnectionDetails( SESSION_1, "AnotherUser" ), QUERY_1 ) ),
                        sameInstance( error ) )
        );
    }

    @Test
    public void shouldNotLogPassword() throws Exception
    {
        String inputQuery = "CALL dbms.security.changePassword('abc123')";
        String outputQuery = "CALL dbms.security.changePassword(******)";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    public void shouldNotLogPasswordNull() throws Exception
    {
        String inputQuery = "CALL dbms.security.changeUserPassword(null, 'password')";
        String outputQuery = "CALL dbms.security.changeUserPassword(null, ******)";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    public void shouldNotLogPasswordWhenMalformedArgument() throws Exception
    {
        String inputQuery = "CALL dbms.security.changeUserPassword('user, 'password')";
        String outputQuery = "CALL dbms.security.changeUserPassword('user, ******)";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    public void shouldNotLogPasswordExplain() throws Exception
    {
        String inputQuery = "EXPLAIN CALL dbms.security.changePassword('abc123')";
        String outputQuery = "EXPLAIN CALL dbms.security.changePassword(******)";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    public void shouldNotLogChangeUserPassword() throws Exception
    {
        String inputQuery = "CALL dbms.security.changeUserPassword('abc123')";
        String outputQuery = "CALL dbms.security.changeUserPassword(******)";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    public void shouldNotLogPasswordEvenIfPasswordIsSilly() throws Exception
    {
        String inputQuery = "CALL dbms.security.changePassword('.changePassword(\\'si\"lly\\')')";
        String outputQuery = "CALL dbms.security.changePassword(******)";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    public void shouldNotLogPasswordEvenIfYouDoTwoThingsAtTheSameTime() throws Exception
    {
        String inputQuery = "CALL dbms.security.changeUserPassword('neo4j','.changePassword(silly)') " +
                "CALL dbms.security.changeUserPassword('smith','other$silly') RETURN 1";
        String outputQuery = "CALL dbms.security.changeUserPassword('neo4j',******) " +
                "CALL dbms.security.changeUserPassword('smith',******) RETURN 1";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    public void shouldNotLogPasswordEvenIfYouDoTwoThingsAtTheSameTimeWithSeveralParms() throws Exception
    {
        String inputQuery = "CALL dbms.security.changeUserPassword('neo4j',$first) " +
                "CALL dbms.security.changeUserPassword('smith',$second) RETURN 1";
        String outputQuery = "CALL dbms.security.changeUserPassword('neo4j',$first) " +
                "CALL dbms.security.changeUserPassword('smith',$second) RETURN 1";

        Map<String,Object> params = new HashMap<>();
        params.put( "first", ".changePassword(silly)" );
        params.put( "second", ".other$silly" );

        runAndCheck( inputQuery, outputQuery, params, "first: ******, second: ******" );
    }

    @Test
    public void shouldNotLogPasswordInParams() throws Exception
    {
        String inputQuery = "CALL dbms.changePassword($password)";
        String outputQuery = "CALL dbms.changePassword($password)";

        runAndCheck( inputQuery, outputQuery, Collections.singletonMap( "password", ".changePassword(silly)" ),
                "password: ******" );
    }

    @Test
    public void shouldNotLogPasswordInDeprecatedParams() throws Exception
    {
        String inputQuery = "CALL dbms.changePassword({password})";
        String outputQuery = "CALL dbms.changePassword({password})";

        runAndCheck( inputQuery, outputQuery, Collections.singletonMap( "password", "abc123" ), "password: ******" );
    }

    @Test
    public void shouldNotLogPasswordDifferentWhitespace() throws Exception
    {
        String inputQuery = "CALL dbms.security.changeUserPassword(%s'abc123'%s)";
        String outputQuery = "CALL dbms.security.changeUserPassword(%s******%s)";

        runAndCheck(
                format( inputQuery, "'user',", "" ),
                format( outputQuery, "'user',", "" ), emptyMap(), "" );
        runAndCheck(
                format( inputQuery, "'user', ", "" ),
                format( outputQuery, "'user', ", "" ), emptyMap(), "" );
        runAndCheck(
                format( inputQuery, "'user' ,", " " ),
                format( outputQuery, "'user' ,", " " ), emptyMap(), "" );
        runAndCheck(
                format( inputQuery, "'user',  ", "  " ),
                format( outputQuery, "'user',  ", "  " ), emptyMap(), "" );
    }

    private void runAndCheck( String inputQuery, String outputQuery, Map<String,Object> params, String paramsString )
    {
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        QueryLogger queryLogger = queryLogger( logProvider, LOG_PARAMETERS );

        // when
        ExecutingQuery query = query( SESSION_1, "neo", inputQuery, params, emptyMap() );
        queryLogger.startQueryExecution( query );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query );

        // then
        logProvider.assertExactly( inLog( getClass() )
                .info( format( "%d ms: %s - %s - {%s} - {}", 10L, sessionConnectionDetails( SESSION_1, "neo" ),
                        outputQuery,
                        paramsString ) ) );
    }

    @Test
    public void shouldBeAbleToLogDetailedTime() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        QueryLogger queryLogger = queryLogger( logProvider, LOG_DETAILED_TIME );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        // when
        queryLogger.startQueryExecution( query );
        clock.forward( 17, TimeUnit.MILLISECONDS );
        cpuClock.add( 12, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query );

        // then
        logProvider.assertExactly( inLog( getClass() ).info(
                containsString( "17 ms: (planning: 17, cpu: 12, waiting: 0) - " ) ) );
    }

    @Test
    public void shouldBeAbleToLogAllocatedBytes() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        QueryLogger queryLogger = queryLogger( logProvider, LOG_ALLOCATED_BYTES );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        // when
        queryLogger.startQueryExecution( query );
        clock.forward( 17, TimeUnit.MILLISECONDS );
        heapAllocation.add( 4096 );
        queryLogger.endSuccess( query );

        // then
        logProvider.assertExactly( inLog( getClass() ).info(
                containsString( "ms: 4096 B - " ) ) );
    }

    @Test
    public void shouldBeAbleToLogPageHitsAndPageFaults() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        QueryLogger queryLogger = queryLogger( logProvider, LOG_PAGE_DETAILS );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        // when
        queryLogger.startQueryExecution( query );
        clock.forward( 12, TimeUnit.MILLISECONDS );
        pageHits = 17;
        pageFaults = 12;
        queryLogger.endSuccess( query );

        // then
        logProvider.assertExactly( inLog( getClass() ).info(
                containsString( " 17 page hits, 12 page faults - " ) ) );
    }

    private QueryLogger queryLogger( LogProvider logProvider, QueryLogEntryContent... flags )
    {
        EnumSet<QueryLogEntryContent> flagSet = EnumSet.noneOf( QueryLogEntryContent.class );
        Collections.addAll( flagSet, flags );
        return new QueryLogger( logProvider.getLog( getClass() ), () -> true, () -> thresholdInMillis, flagSet );
    }

    private ExecutingQuery query(
            ClientConnectionInfo sessionInfo,
            String username,
            String queryText )
    {
        return query( sessionInfo, username, queryText, emptyMap(), emptyMap() );
    }

    private String sessionConnectionDetails( ClientConnectionInfo sessionInfo, String username )
    {
        return sessionInfo.withUsername( username ).asConnectionDetails();
    }

    private int queryId;

    private ExecutingQuery query(
            ClientConnectionInfo sessionInfo,
            String username,
            String queryText,
            Map<String,Object> params,
            Map<String,Object> metaData )
    {
        return new ExecutingQuery( queryId++,
                sessionInfo.withUsername( username ),
                username,
                queryText,
                ValueUtils.asMapValue( params ),
                metaData,
                () -> 0,
                new PageCursorCounters()
                {
                    @Override
                    public long faults()
                    {
                        return pageFaults;
                    }

                    @Override
                    public long hits()
                    {
                        return pageHits;
                    }

                    @Override
                    public long pins()
                    {
                        return 0;
                    }

                    @Override
                    public long unpins()
                    {
                        return 0;
                    }

                    @Override
                    public long bytesRead()
                    {
                        return 0;
                    }

                    @Override
                    public long evictions()
                    {
                        return 0;
                    }

                    @Override
                    public long evictionExceptions()
                    {
                        return 0;
                    }

                    @Override
                    public long bytesWritten()
                    {
                        return 0;
                    }

                    @Override
                    public long flushes()
                    {
                        return 0;
                    }

                    @Override
                    public double hitRatio()
                    {
                        return 0d;
                    }
                }, Thread.currentThread(),
                clock,
                cpuClock,
                heapAllocation );
    }
}
