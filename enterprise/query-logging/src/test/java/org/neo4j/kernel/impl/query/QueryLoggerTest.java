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

import org.junit.Test;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.ExecutingQuery;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import org.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.QueryLoggerKernelExtension.QueryLogger;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class QueryLoggerTest
{
    private static final String SESSION_1_NAME = "{session one}";
    private static final String SESSION_2_NAME = "{session two}";
    private static final String SESSION_3_NAME = "{session three}";
    private static final String QUERY_1 = "MATCH (n) RETURN n";
    private static final String QUERY_2 = "MATCH (a)--(b) RETURN b.name";
    private static final String QUERY_3 = "MATCH (c)-[:FOO]->(d) RETURN d.size";
    private static final String QUERY_4 = "MATCH (n) WHERE n.age IN {ages} RETURN n";

    @Test
    public void shouldLogQuerySlowerThanThreshold() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        ExecutingQuery query = query( 0, SESSION_1_NAME, "TestUser", QUERY_1 );
        FakeClock clock = Clocks.fakeClock();
        QueryLogger queryLogger = queryLoggerWithoutParams( logProvider, clock );

        // when
        queryLogger.startQueryExecution( query );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query );

        // then
        String expectedSessionString = format( "%s [%s]", SESSION_1_NAME, "TestUser" );
        logProvider.assertExactly(
            inLog( getClass() ).info( format( "%d ms: %s - %s - {}", 11L, expectedSessionString, QUERY_1 ) )
        );
    }

    @Test
    public void shouldNotLogQueryFasterThanThreshold() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        ExecutingQuery query = query( 0, SESSION_1_NAME, "TestUser", QUERY_1 );
        FakeClock clock = Clocks.fakeClock();
        QueryLogger queryLogger = queryLoggerWithoutParams( logProvider, clock );

        // when
        queryLogger.startQueryExecution( query );
        clock.forward( 9, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query );

        // then
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void shouldKeepTrackOfDifferentSessions() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        ExecutingQuery query1 = query( 0, SESSION_1_NAME, "TestUser1", QUERY_1 );
        ExecutingQuery query2 = query( 1, SESSION_2_NAME, "TestUser2", QUERY_2 );
        ExecutingQuery query3 = query( 2, SESSION_3_NAME, "TestUser3", QUERY_3 );

        FakeClock clock = Clocks.fakeClock();
        QueryLogger queryLogger = queryLoggerWithoutParams( logProvider, clock );

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
        String expectedSession1String = format( "%s [%s]", SESSION_1_NAME, "TestUser1" );
        String expectedSession2String = format( "%s [%s]", SESSION_2_NAME, "TestUser2" );
        logProvider.assertExactly(
                inLog( getClass() ).info( format( "%d ms: %s - %s - {}", 15L, expectedSession2String, QUERY_2 ) ),
                inLog( getClass() ).info( format( "%d ms: %s - %s - {}", 23L, expectedSession1String, QUERY_1 ) )
        );
    }

    @Test
    public void shouldLogQueryOnFailureEvenIfFasterThanThreshold() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        ExecutingQuery query = query( 0, SESSION_1_NAME, "TestUser", QUERY_1 );

        FakeClock clock = Clocks.fakeClock();
        QueryLogger queryLogger = queryLoggerWithoutParams( logProvider, clock );
        RuntimeException failure = new RuntimeException();

        // when
        queryLogger.startQueryExecution( query );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.endFailure( query, failure );

        // then
        logProvider.assertExactly(
                inLog( getClass() )
                        .error( is( "1 ms: {session one} [TestUser] - MATCH (n) RETURN n - {}" ), sameInstance( failure ) )
        );
    }

    @Test
    public void shouldLogQueryParameters() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        ExecutingQuery query = query( 0, SESSION_1_NAME, "TestUser", QUERY_4, params, emptyMap() );
        FakeClock clock = Clocks.fakeClock();
        QueryLogger queryLogger = queryLoggerWithParams( logProvider, clock );

        // when
        queryLogger.startQueryExecution( query );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query );

        // then
        String expectedSessionString = format( "%s [%s]", SESSION_1_NAME, "TestUser" );
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
        ExecutingQuery query = query( 0, SESSION_1_NAME, "TestUser", QUERY_4, params, emptyMap() );
        FakeClock clock = Clocks.fakeClock();
        QueryLogger queryLogger = queryLoggerWithParams( logProvider, clock );
        RuntimeException failure = new RuntimeException();

        // when
        queryLogger.startQueryExecution( query );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.endFailure( query, failure );

        // then
        logProvider.assertExactly(
            inLog( getClass() ).error(
                is( "1 ms: {session one} [TestUser] - MATCH (n) WHERE n.age IN {ages} RETURN n - {ages: [41, 42, 43]}" +
                        " - {}" ),
                sameInstance( failure ) )
        );
    }

    @Test
    public void shouldLogUserName() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        FakeClock clock = Clocks.fakeClock();
        QueryLogger queryLogger = queryLoggerWithoutParams( logProvider, clock );

        // when
        ExecutingQuery query = query( 0, SESSION_1_NAME, "TestUser", QUERY_1 );
        queryLogger.startQueryExecution( query );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query );

        ExecutingQuery anotherQuery = query( 10, SESSION_1_NAME, "AnotherUser", QUERY_1 );
        queryLogger.startQueryExecution( anotherQuery );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( anotherQuery );

        // then
        logProvider.assertExactly(
                inLog( getClass() ).info( format( "%d ms: %s - %s - {}", 10L, "{session one} [TestUser]", QUERY_1 ) ),
                inLog( getClass() ).info( format( "%d ms: %s - %s - {}", 10L, "{session one} [AnotherUser]", QUERY_1 ) )
        );
    }

    @Test
    public void shouldLogMetaData() throws Exception
    {
        // given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        FakeClock clock = Clocks.fakeClock();
        QueryLogger queryLogger = queryLoggerWithoutParams( logProvider, clock );

        // when
        ExecutingQuery query = query( 0, SESSION_1_NAME, "TestUser", QUERY_1, emptyMap(), map( "User", "UltiMate" ) );
        queryLogger.startQueryExecution( query );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query );

        ExecutingQuery anotherQuery =
                query( 10, SESSION_1_NAME, "AnotherUser", QUERY_1, emptyMap(), map( "Place", "Town" ) );
        queryLogger.startQueryExecution( anotherQuery );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        Throwable error = new Throwable();
        queryLogger.endFailure( anotherQuery, error );

        // then
        logProvider.assertExactly(
                inLog( getClass() ).info( format( "%d ms: %s - %s - {User: 'UltiMate'}", 10L,
                        "{session one} [TestUser]", QUERY_1
                ) ),
                inLog( getClass() ).error(
                        equalTo( format( "%d ms: %s - %s - {Place: 'Town'}", 10L,
                            "{session one} [AnotherUser]", QUERY_1 ) ),
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
        FakeClock clock = Clocks.fakeClock();
        QueryLogger queryLogger = queryLoggerWithParams( logProvider, clock );

        // when
        ExecutingQuery query = query( 0, SESSION_1_NAME, "neo", inputQuery, params, emptyMap() );
        queryLogger.startQueryExecution( query );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.endSuccess( query );

        // then
        logProvider.assertExactly( inLog( getClass() )
                .info( format( "%d ms: %s - %s - {%s} - {}", 10L, "{session one} [neo]", outputQuery,
                        paramsString ) ) );
    }

    private QueryLogger queryLoggerWithoutParams( LogProvider logProvider, Clock clock )
    {
        return new QueryLogger( clock, logProvider.getLog( getClass() ), 10/*ms*/, false );
    }

    private QueryLogger queryLoggerWithParams( LogProvider logProvider, Clock clock )
    {
        return new QueryLogger( clock, logProvider.getLog( getClass() ), 10/*ms*/, true );
    }

    private static ExecutingQuery query( long startTime, String source, String username, String queryText )
    {
        return query( startTime, source, username, queryText, emptyMap(), emptyMap() );
    }

    private static ExecutingQuery query(
        long startTime, String source, String username, String queryText, Map<String,Object> params,
            Map<String,Object> metaData
    )
    {
        ExecutingQuery query = mock( ExecutingQuery.class );
        when( query.querySource() ).thenReturn( new QuerySource( source + " [" + username + "]" ) );
        when( query.queryText() ).thenReturn( queryText );
        when( query.queryParameters() ).thenReturn( params );
        when( query.startTime() ).thenReturn( startTime );
        when( query.username() ).thenReturn( username );
        when( query.metaData() ).thenReturn( metaData );
        return query;
    }
}
