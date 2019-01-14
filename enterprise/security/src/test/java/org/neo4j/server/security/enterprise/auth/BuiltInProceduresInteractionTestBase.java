/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.security.enterprise.auth;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.enterprise.builtinprocs.QueryId;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.newapi.Operations;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.test.Barrier;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static java.lang.String.format;
import static java.time.OffsetDateTime.from;
import static java.time.OffsetDateTime.now;
import static java.time.OffsetDateTime.ofInstant;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.bolt.v1.runtime.integration.TransactionIT.createHttpServer;
import static org.neo4j.concurrent.Runnables.EMPTY_RUNNABLE;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.matchers.CommonMatchers.matchesOneToOneInAnyOrder;

public abstract class BuiltInProceduresInteractionTestBase<S> extends ProcedureInteractionTestBase<S>
{

    //---------- list running transactions -----------

    @Test
    public void shouldListSelfTransaction()
    {
        assertSuccess( adminSubject, "CALL dbms.listTransactions()",
                r -> assertKeyIs( r, "username", "adminSubject" ) );
    }

    @Test
    public void listBlockedTransactions() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (:MyNode {prop: 2})" );
        String firstModifier = "MATCH (n:MyNode) set n.prop=3";
        String secondModifier = "MATCH (n:MyNode) set n.prop=4";
        DoubleLatch latch = new DoubleLatch( 2 );
        DoubleLatch blockedModifierLatch = new DoubleLatch( 2 );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> tx = new ThreadedTransaction<>( neo, latch );
        tx.execute( threading, writeSubject, firstModifier );
        latch.start();
        latch.waitForAllToStart();

        ThreadedTransaction<S> tx2 = new ThreadedTransaction<>( neo, blockedModifierLatch );
        tx2.executeEarly( threading, writeSubject, KernelTransaction.Type.explicit, secondModifier );

        waitTransactionToStartWaitingForTheLock();

        blockedModifierLatch.startAndWaitForAllToStart();
        String query = "CALL dbms.listTransactions()";
        assertSuccess( adminSubject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> listTransaction = listedTransactionOfInteractionLevel( startTime,
                    "adminSubject", query );
            Matcher<Map<String,Object>> blockedQueryMatcher = allOf( anyOf( hasCurrentQuery( secondModifier ),
                    hasCurrentQuery( firstModifier ) ), hasStatus( "Blocked by:" ) );
            Matcher<Map<String,Object>> executedModifier = allOf( hasCurrentQuery(""), hasStatus( "Running" ) );

            assertThat( maps, matchesOneToOneInAnyOrder( listTransaction, blockedQueryMatcher, executedModifier ) );
        } );

        latch.finishAndWaitForAllToFinish();
        tx.closeAndAssertSuccess();
        blockedModifierLatch.finishAndWaitForAllToFinish();
    }

    private void waitTransactionToStartWaitingForTheLock() throws InterruptedException
    {
        while ( Thread.getAllStackTraces().keySet().stream().noneMatch(
                ThreadingRule.waitingWhileIn( Operations.class, "acquireExclusiveNodeLock" ) ) )
        {
            TimeUnit.MILLISECONDS.sleep( 10 );
        }
    }

    @Test
    public void listTransactionWithMetadata() throws Throwable
    {
        String setMetaDataQuery = "CALL dbms.setTXMetaData( { realUser: 'MyMan' } )";
        String matchQuery = "MATCH (n) RETURN n";
        String listTransactionsQuery = "CALL dbms.listTransactions()";

        DoubleLatch latch = new DoubleLatch( 2 );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> tx = new ThreadedTransaction<>( neo, latch );
        tx.execute( threading, writeSubject, setMetaDataQuery, matchQuery );

        latch.startAndWaitForAllToStart();

        assertSuccess( adminSubject, listTransactionsQuery, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );
            Matcher<Map<String,Object>> thisTransaction =
                    listedTransactionOfInteractionLevel( startTime, "adminSubject", listTransactionsQuery );
            Matcher<Map<String,Object>> matchQueryTransactionMatcher =
                    listedTransactionWithMetaData( startTime, "writeSubject", matchQuery,  map( "realUser", "MyMan" ) );

            assertThat( maps, matchesOneToOneInAnyOrder( thisTransaction, matchQueryTransactionMatcher ) );
        } );

        latch.finishAndWaitForAllToFinish();
        tx.closeAndAssertSuccess();
    }

    @Test
    public void listAllTransactionsWhenRunningAsAdmin() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 3, true );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );

        String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        String q2 = read2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listTransactions()";
        assertSuccess( adminSubject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisTransaction = listedTransactionOfInteractionLevel( startTime, "adminSubject", query );
            Matcher<Map<String,Object>> matcher1 = listedTransaction( startTime, "readSubject", q1 );
            Matcher<Map<String,Object>> matcher2 = listedTransaction( startTime, "writeSubject", q2 );

            assertThat( maps, matchesOneToOneInAnyOrder( matcher1, matcher2, thisTransaction ) );
        } );

        latch.finishAndWaitForAllToFinish();

        read1.closeAndAssertSuccess();
        read2.closeAndAssertSuccess();
    }

    @Test
    public void shouldOnlyListOwnTransactionsWhenNotRunningAsAdmin() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 3, true );
        OffsetDateTime startTime = getStartTime();
        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );

        String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        String ignored = read2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listTransactions()";
        assertSuccess( readSubject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisTransaction = listedTransaction( startTime, "readSubject", query );
            Matcher<Map<String,Object>> queryMatcher = listedTransaction( startTime, "readSubject", q1 );

            assertThat( maps, matchesOneToOneInAnyOrder( queryMatcher, thisTransaction ) );
        } );

        latch.finishAndWaitForAllToFinish();

        read1.closeAndAssertSuccess();
        read2.closeAndAssertSuccess();
    }

    @Test
    public void shouldListAllTransactionsWithAuthDisabled() throws Throwable
    {
        neo.tearDown();
        neo = setUpNeoServer( stringMap( GraphDatabaseSettings.auth_enabled.name(), "false" ) );

        DoubleLatch latch = new DoubleLatch( 2, true );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> read = new ThreadedTransaction<>( neo, latch );

        String q = read.execute( threading, neo.login( "user1", "" ), "UNWIND [1,2,3] AS x RETURN x" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listTransactions()";
        try
        {
            assertSuccess( neo.login( "admin", "" ), query, r ->
            {
                List<Map<String,Object>> maps = collectResults( r );

                Matcher<Map<String,Object>> thisQuery = listedTransactionOfInteractionLevel( startTime, "", query ); // admin
                Matcher<Map<String,Object>> matcher1 = listedTransaction( startTime, "", q ); // user1
                assertThat( maps, matchesOneToOneInAnyOrder( matcher1, thisQuery ) );
            } );
        }
        finally
        {
            latch.finishAndWaitForAllToFinish();
        }
        read.closeAndAssertSuccess();
    }

    //---------- list running queries -----------

    @Test
    public void shouldListAllQueryIncludingMetaData() throws Throwable
    {
        String setMetaDataQuery = "CALL dbms.setTXMetaData( { realUser: 'MyMan' } )";
        String matchQuery = "MATCH (n) RETURN n";
        String listQueriesQuery = "CALL dbms.listQueries()";

        DoubleLatch latch = new DoubleLatch( 2 );
        OffsetDateTime startTime = now( ZoneOffset.UTC );

        ThreadedTransaction<S> tx = new ThreadedTransaction<>( neo, latch );
        tx.execute( threading, writeSubject, setMetaDataQuery, matchQuery );

        latch.startAndWaitForAllToStart();

        assertSuccess( adminSubject, listQueriesQuery, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );
            Matcher<Map<String,Object>> thisQuery =
                    listedQueryOfInteractionLevel( startTime, "adminSubject", listQueriesQuery );
            Matcher<Map<String,Object>> matchQueryMatcher =
                    listedQueryWithMetaData( startTime, "writeSubject", matchQuery, map( "realUser", "MyMan" ) );

            assertThat( maps, matchesOneToOneInAnyOrder( thisQuery, matchQueryMatcher ) );
        } );

        latch.finishAndWaitForAllToFinish();
        tx.closeAndAssertSuccess();
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldListAllQueriesWhenRunningAsAdmin() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 3, true );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );

        String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        String q2 = read2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listQueries()";
        assertSuccess( adminSubject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisQuery = listedQueryOfInteractionLevel( startTime, "adminSubject", query );
            Matcher<Map<String,Object>> matcher1 = listedQuery( startTime, "readSubject", q1 );
            Matcher<Map<String,Object>> matcher2 = listedQuery( startTime, "writeSubject", q2 );

            assertThat( maps, matchesOneToOneInAnyOrder( matcher1, matcher2, thisQuery ) );
        } );

        latch.finishAndWaitForAllToFinish();

        read1.closeAndAssertSuccess();
        read2.closeAndAssertSuccess();
    }

    @Test
    public void shouldOnlyListOwnQueriesWhenNotRunningAsAdmin() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 3, true );
        OffsetDateTime startTime = getStartTime();
        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );

        String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        String ignored = read2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listQueries()";
        assertSuccess( readSubject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisQuery = listedQuery( startTime, "readSubject", query );
            Matcher<Map<String,Object>> queryMatcher = listedQuery( startTime, "readSubject", q1 );

            assertThat( maps, matchesOneToOneInAnyOrder( queryMatcher, thisQuery ) );
        } );

        latch.finishAndWaitForAllToFinish();

        read1.closeAndAssertSuccess();
        read2.closeAndAssertSuccess();
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldListQueriesEvenIfUsingPeriodicCommit() throws Throwable
    {
        for ( int i = 8; i <= 11; i++ )
        {
            // Spawns a throttled HTTP server, runs a PERIODIC COMMIT that fetches data from this server,
            // and checks that the query is visible when using listQueries()

            // Given
            final DoubleLatch latch = new DoubleLatch( 3, true );
            final Barrier.Control barrier = new Barrier.Control();

            // Serve CSV via local web server, let Jetty find a random port for us
            Server server = createHttpServer( latch, barrier, i, 50 - i );
            server.start();
            int localPort = getLocalPort( server );

            OffsetDateTime startTime = getStartTime();

            // When
            ThreadedTransaction<S> write = new ThreadedTransaction<>( neo, latch );

            try
            {
                String writeQuery = write.executeEarly( threading, writeSubject, KernelTransaction.Type.implicit,
                        format( "USING PERIODIC COMMIT 10 LOAD CSV FROM 'http://localhost:%d' AS line ", localPort ) +
                        "CREATE (n:A {id: line[0], square: line[1]}) " + "RETURN count(*)" );
                latch.startAndWaitForAllToStart();

                // Then
                String query = "CALL dbms.listQueries()";
                assertSuccess( adminSubject, query, r ->
                {
                    List<Map<String,Object>> maps = collectResults( r );

                    Matcher<Map<String,Object>> thisMatcher = listedQuery( startTime, "adminSubject", query );
                    Matcher<Map<String,Object>> writeMatcher = listedQuery( startTime, "writeSubject", writeQuery );

                    assertThat( maps, hasItem( thisMatcher ) );
                    assertThat( maps, hasItem( writeMatcher ) );
                } );
            }
            finally
            {
                // When
                barrier.release();
                latch.finishAndWaitForAllToFinish();
                server.stop();

                // Then
                write.closeAndAssertSuccess();
            }
        }
    }

    @Test
    public void shouldListAllQueriesWithAuthDisabled() throws Throwable
    {
        neo.tearDown();
        neo = setUpNeoServer( stringMap( GraphDatabaseSettings.auth_enabled.name(), "false" ) );

        DoubleLatch latch = new DoubleLatch( 2, true );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> read = new ThreadedTransaction<>( neo, latch );

        String q = read.execute( threading, neo.login( "user1", "" ), "UNWIND [1,2,3] AS x RETURN x" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listQueries()";
        try
        {
            assertSuccess( neo.login( "admin", "" ), query, r ->
            {
                List<Map<String,Object>> maps = collectResults( r );

                Matcher<Map<String,Object>> thisQuery = listedQueryOfInteractionLevel( startTime, "", query ); // admin
                Matcher<Map<String,Object>> matcher1 = listedQuery( startTime, "", q ); // user1
                assertThat( maps, matchesOneToOneInAnyOrder( matcher1, thisQuery ) );
            } );
        }
        finally
        {
            latch.finishAndWaitForAllToFinish();
        }
        read.closeAndAssertSuccess();
    }

    //---------- Create Tokens query -------

    @Test
    public void shouldCreateLabel()
    {
        assertFail( editorSubject, "CREATE (:MySpecialLabel)", TOKEN_CREATE_OPS_NOT_ALLOWED );
        assertFail( editorSubject, "CALL db.createLabel('MySpecialLabel')", TOKEN_CREATE_OPS_NOT_ALLOWED );
        assertEmpty( writeSubject, "CALL db.createLabel('MySpecialLabel')" );
        assertSuccess( writeSubject, "MATCH (n:MySpecialLabel) RETURN count(n) AS count",
                r -> r.next().get( "count" ).equals( 0 ) );
        assertEmpty( editorSubject, "CREATE (:MySpecialLabel)" );
    }

    @Test
    public void shouldCreateRelationshipType()
    {
        assertEmpty( writeSubject, "CREATE (a:Node {id:0}) CREATE ( b:Node {id:1} )" );
        assertFail( editorSubject,
                "MATCH (a:Node), (b:Node) WHERE a.id = 0 AND b.id = 1 CREATE (a)-[:MySpecialRelationship]->(b)",
                TOKEN_CREATE_OPS_NOT_ALLOWED );
        assertFail( editorSubject, "CALL db.createRelationshipType('MySpecialRelationship')",
                TOKEN_CREATE_OPS_NOT_ALLOWED );
        assertEmpty( writeSubject, "CALL db.createRelationshipType('MySpecialRelationship')" );
        assertSuccess( editorSubject, "MATCH (n)-[c:MySpecialRelationship]-(m) RETURN count(c) AS count",
                r -> r.next().get( "count" ).equals( 0 ) );
        assertEmpty( editorSubject,
                "MATCH (a:Node), (b:Node) WHERE a.id = 0 AND b.id = 1 CREATE (a)-[:MySpecialRelationship]->(b)" );
    }

    @Test
    public void shouldCreateProperty()
    {
        assertFail( editorSubject, "CREATE (a) SET a.MySpecialProperty = 'a'", TOKEN_CREATE_OPS_NOT_ALLOWED );
        assertFail( editorSubject, "CALL db.createProperty('MySpecialProperty')", TOKEN_CREATE_OPS_NOT_ALLOWED );
        assertEmpty( writeSubject, "CALL db.createProperty('MySpecialProperty')" );
        assertSuccess( editorSubject, "MATCH (n) WHERE n.MySpecialProperty IS NULL RETURN count(n) AS count",
                r -> r.next().get( "count" ).equals( 0 ) );
        assertEmpty( editorSubject, "CREATE (a) SET a.MySpecialProperty = 'a'" );
    }

    //---------- terminate query -----------

    /*
     * User starts query1 that takes a lock and runs for a long time.
     * User starts query2 that needs to wait for that lock.
     * query2 is blocked waiting for lock to be released.
     * Admin terminates query2.
     * query2 is immediately terminated, even though locks have not been released.
     */
    @Test
    public void queryWaitingForLocksShouldBeKilledBeforeLocksAreReleased() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (:MyNode {prop: 2})" );

        // create new latch
        ClassWithProcedures.doubleLatch = new DoubleLatch( 2 );

        // start never-ending query
        String query1 = "MATCH (n:MyNode) SET n.prop = 5 WITH * CALL test.neverEnding() RETURN 1";
        ThreadedTransaction<S> tx1 = new ThreadedTransaction<>( neo, new DoubleLatch() );
        tx1.executeEarly( threading, writeSubject, KernelTransaction.Type.explicit, query1 );

        // wait for query1 to be stuck in procedure with its write lock
        ClassWithProcedures.doubleLatch.startAndWaitForAllToStart();

        // start query2
        ThreadedTransaction<S> tx2 = new ThreadedTransaction<>( neo, new DoubleLatch() );
        String query2 = "MATCH (n:MyNode) SET n.prop = 10 RETURN 1";
        tx2.executeEarly( threading, writeSubject, KernelTransaction.Type.explicit, query2 );

        assertQueryIsRunning( query2 );

        // get the query id of query2 and kill it
        assertSuccess( adminSubject,
                "CALL dbms.listQueries() YIELD query, queryId " +
                "WITH query, queryId WHERE query = '" + query2 + "'" +
                "CALL dbms.killQuery(queryId) YIELD queryId AS killedId " +
                "RETURN 1",
                itr -> assertThat( itr.hasNext(), equalTo( true ) ) );

        tx2.closeAndAssertSomeTermination();

        // allow query1 to exit procedure and finish
        ClassWithProcedures.doubleLatch.finish();
        tx1.closeAndAssertSuccess();
    }

    @Test
    public void shouldKillQueryAsAdmin() throws Throwable
    {
        executeTwoQueriesAndKillTheFirst( readSubject, readSubject, adminSubject );
    }

    @Test
    public void shouldKillQueryAsUser() throws Throwable
    {
        executeTwoQueriesAndKillTheFirst( readSubject, writeSubject, readSubject );
    }

    private void executeTwoQueriesAndKillTheFirst( S executor1, S executor2, S killer ) throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 3 );
        ThreadedTransaction<S> tx1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> tx2 = new ThreadedTransaction<>( neo, latch );
        String q1 = tx1.execute( threading, executor1, "UNWIND [1,2,3] AS x RETURN x" );
        tx2.execute( threading, executor2, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String id1 = extractQueryId( q1 );

        assertSuccess(
                killer,
                "CALL dbms.killQuery('" + id1 + "') YIELD username " +
                "RETURN count(username) AS count, username", r ->
                {
                    List<Map<String,Object>> actual = collectResults( r );
                    @SuppressWarnings( "unchecked" )
                    Matcher<Map<String,Object>> mapMatcher = allOf(
                            (Matcher) hasEntry( equalTo( "count" ), anyOf( equalTo( 1 ), equalTo( 1L ) ) ),
                            (Matcher) hasEntry( equalTo( "username" ), equalTo( "readSubject" ) )
                    );
                    assertThat( actual, matchesOneToOneInAnyOrder( mapMatcher ) );
                }
        );

        latch.finishAndWaitForAllToFinish();
        tx1.closeAndAssertExplicitTermination();
        tx2.closeAndAssertSuccess();

        assertEmpty( adminSubject,
                "CALL dbms.listQueries() YIELD query WITH * WHERE NOT query CONTAINS 'listQueries' RETURN *" );
    }

    @Test
    public void shouldSelfKillQuery()
    {
        String result = neo.executeQuery(
                readSubject,
                "WITH 'Hello' AS marker CALL dbms.listQueries() YIELD queryId AS id, query " +
                "WITH * WHERE query CONTAINS 'Hello' CALL dbms.killQuery(id) YIELD username " +
                "RETURN count(username) AS count, username",
                emptyMap(),
                r ->
                {
                }
        );

        assertThat( result, containsString( "Explicitly terminated by the user." ) );

        assertEmpty(
                adminSubject,
                "CALL dbms.listQueries() YIELD query WITH * WHERE NOT query CONTAINS 'listQueries' RETURN *" );
    }

    @Test
    public void shouldFailToTerminateOtherUsersQuery() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 3, true );
        ThreadedTransaction<S> read = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> write = new ThreadedTransaction<>( neo, latch );
        String q1 = read.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        write.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        try
        {
            String id1 = extractQueryId( q1 );
            assertFail(
                    writeSubject,
                    "CALL dbms.killQuery('" + id1 + "') YIELD username RETURN *",
                    PERMISSION_DENIED
            );
            latch.finishAndWaitForAllToFinish();
            read.closeAndAssertSuccess();
            write.closeAndAssertSuccess();
        }
        catch ( Throwable t )
        {
            latch.finishAndWaitForAllToFinish();
            throw t;
        }

        assertEmpty(
                adminSubject,
                "CALL dbms.listQueries() YIELD query WITH * WHERE NOT query CONTAINS 'listQueries' RETURN *" );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldTerminateQueriesEvenIfUsingPeriodicCommit() throws Throwable
    {
        for ( int batchSize = 8; batchSize <= 11; batchSize++ )
        {
            // Spawns a throttled HTTP server, runs a PERIODIC COMMIT that fetches data from this server,
            // and checks that the query is visible when using listQueries()

            // Given
            final DoubleLatch latch = new DoubleLatch( 3, true );
            final Barrier.Control barrier = new Barrier.Control();

            // Serve CSV via local web server, let Jetty find a random port for us
            Server server = createHttpServer( latch, barrier, batchSize, 50 - batchSize );
            server.start();
            int localPort = getLocalPort( server );

            // When
            ThreadedTransaction<S> write = new ThreadedTransaction<>( neo, latch );

            try
            {
                String writeQuery = write.executeEarly( threading, writeSubject, KernelTransaction.Type.implicit,
                        format( "USING PERIODIC COMMIT 10 LOAD CSV FROM 'http://localhost:%d' AS line ", localPort ) +
                        "CREATE (n:A {id: line[0], square: line[1]}) RETURN count(*)" );
                latch.startAndWaitForAllToStart();

                // Then
                String writeQueryId = extractQueryId( writeQuery );

                assertSuccess(
                        adminSubject,
                        "CALL dbms.killQuery('" + writeQueryId + "') YIELD username " +
                        "RETURN count(username) AS count, username", r ->
                        {
                            List<Map<String,Object>> actual = collectResults( r );
                            Matcher<Map<String,Object>> mapMatcher = allOf(
                                    (Matcher) hasEntry( equalTo( "count" ), anyOf( equalTo( 1 ), equalTo( 1L ) ) ),
                                    (Matcher) hasEntry( equalTo( "username" ), equalTo( "writeSubject" ) )
                            );
                            assertThat( actual, matchesOneToOneInAnyOrder( mapMatcher ) );
                        }
                );
            }
            finally
            {
                // When
                barrier.release();
                latch.finishAndWaitForAllToFinish();

                // Then
                // We cannot assert on explicit termination here, because if the termination is detected when trying
                // to lock we will only get the general TransactionTerminatedException
                // (see {@link LockClientStateHolder}).
                write.closeAndAssertSomeTermination();

                // stop server after assertion to avoid other kind of failures due to races (e.g., already closed
                // lock clients )
                server.stop();
            }
        }
    }

    @Test
    public void shouldKillMultipleUserQueries() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 5 );
        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read3 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> write = new ThreadedTransaction<>( neo, latch );
        String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        String q2 = read2.execute( threading, readSubject, "UNWIND [4,5,6] AS y RETURN y" );
        read3.execute( threading, readSubject, "UNWIND [7,8,9] AS z RETURN z" );
        write.execute( threading, writeSubject, "UNWIND [11,12,13] AS q RETURN q" );
        latch.startAndWaitForAllToStart();

        String id1 = extractQueryId( q1 );
        String id2 = extractQueryId( q2 );

        String idParam = "['" + id1 + "', '" + id2 + "']";

        assertSuccess(
                adminSubject,
                "CALL dbms.killQueries(" + idParam + ") YIELD username " +
                "RETURN count(username) AS count, username", r ->
                {
                    List<Map<String,Object>> actual = collectResults( r );
                    Matcher<Map<String,Object>> mapMatcher = allOf(
                            (Matcher) hasEntry( equalTo( "count" ), anyOf( equalTo( 2 ), equalTo( 2L ) ) ),
                            (Matcher) hasEntry( equalTo( "username" ), equalTo( "readSubject" ) )
                    );
                    assertThat( actual, matchesOneToOneInAnyOrder( mapMatcher ) );
                }
        );

        latch.finishAndWaitForAllToFinish();
        read1.closeAndAssertExplicitTermination();
        read2.closeAndAssertExplicitTermination();
        read3.closeAndAssertSuccess();
        write.closeAndAssertSuccess();

        assertEmpty(
                adminSubject,
                "CALL dbms.listQueries() YIELD query WITH * WHERE NOT query CONTAINS 'listQueries' RETURN *" );
    }

    String extractQueryId( String writeQuery )
    {
        return toRawValue( single( collectSuccessResult( adminSubject, "CALL dbms.listQueries()" )
                .stream()
                .filter( m -> m.get( "query" ).equals( valueOf( writeQuery ) ) )
                .collect( toList() ) )
                .get( "queryId" ) )
                .toString();
    }

    //---------- set tx meta data -----------

    @Test
    public void shouldHaveSetTXMetaDataProcedure()
    {
        assertEmpty( writeSubject, "CALL dbms.setTXMetaData( { aKey: 'aValue' } )" );
    }

    @Test
    public void readUpdatedMetadataValue() throws Throwable
    {
        String testValue = "testValue";
        String testKey = "test";
        GraphDatabaseFacade graph = neo.getLocalGraph();
        try ( InternalTransaction transaction = neo
                .beginLocalTransactionAsUser( writeSubject, KernelTransaction.Type.explicit ) )
        {
            graph.execute( "CALL dbms.setTXMetaData({" + testKey + ":'" + testValue + "'})" );
            Map<String,Object> metadata =
                    (Map<String,Object>) graph.execute( "CALL dbms.getTXMetaData " ).next().get( "metadata" );
            assertEquals( testValue, metadata.get( testKey ) );
        }
    }

    @Test
    public void readEmptyMetadataInOtherTransaction()
    {
        String testValue = "testValue";
        String testKey = "test";

        assertEmpty( writeSubject, "CALL dbms.setTXMetaData({" + testKey + ":'" + testValue + "'})" );
        assertSuccess( writeSubject, "CALL dbms.getTXMetaData", mapResourceIterator ->
        {
            Map<String,Object> metadata = mapResourceIterator.next();
            assertNull( metadata.get( testKey ) );
            mapResourceIterator.close();
        } );
    }

    //---------- config manipulation -----------

    @Test
    public void setConfigValueShouldBeAccessibleOnlyToAdmins()
    {
        String call = "CALL dbms.setConfigValue('dbms.logs.query.enabled', 'false')";
        assertFail( writeSubject, call, PERMISSION_DENIED );
        assertFail( schemaSubject, call, PERMISSION_DENIED );
        assertFail( readSubject, call, PERMISSION_DENIED );

        assertEmpty( adminSubject, call );
    }

    //---------- procedure guard -----------

    @Test
    public void shouldTerminateLongRunningProcedureThatChecksTheGuardRegularlyIfKilled() throws Throwable
    {
        final DoubleLatch latch = new DoubleLatch( 2, true );
        ClassWithProcedures.volatileLatch = latch;

        String loopQuery = "CALL test.loop";

        Thread loopQueryThread =
                new Thread( () -> assertFail( readSubject, loopQuery, "Explicitly terminated by the user." ) );
        loopQueryThread.start();
        latch.startAndWaitForAllToStart();

        try
        {
            String loopId = extractQueryId( loopQuery );

            assertSuccess(
                    adminSubject,
                    "CALL dbms.killQuery('" + loopId + "') YIELD username " +
                    "RETURN count(username) AS count, username", r ->
                    {
                        List<Map<String,Object>> actual = collectResults( r );
                        Matcher<Map<String,Object>> mapMatcher = allOf(
                                (Matcher) hasEntry( equalTo( "count" ), anyOf( equalTo( 1 ), equalTo( 1L ) ) ),
                                (Matcher) hasEntry( equalTo( "username" ), equalTo( "readSubject" ) )
                        );
                        assertThat( actual, matchesOneToOneInAnyOrder( mapMatcher ) );
                    }
            );
        }
        finally
        {
            latch.finishAndWaitForAllToFinish();
        }

        // there is a race with "test.loop" procedure - after decrementing latch it may take time to actually exit
        loopQueryThread.join( 10_000 );

        assertEmpty(
                adminSubject,
                "CALL dbms.listQueries() YIELD query WITH * WHERE NOT query CONTAINS 'listQueries' RETURN *" );
    }

    @Test
    public void shouldHandleWriteAfterAllowedReadProcedureForWriteUser() throws Throwable
    {
        userManager = neo.getLocalUserManager();
        userManager.newUser( "role1Subject", "abc", false );
        userManager.newRole( "role1" );
        userManager.addRoleToUser( "role1", "role1Subject" );
        userManager.addRoleToUser( PUBLISHER, "role1Subject" );
        assertEmpty( neo.login( "role1Subject", "abc" ),
                "CALL test.allowedReadProcedure() YIELD value CREATE (:NEWNODE {name:value})" );
    }

    @Test
    public void shouldNotAllowNonWriterToWriteAfterCallingAllowedWriteProc() throws Exception
    {
        userManager = neo.getLocalUserManager();
        userManager.newUser( "nopermission", "abc", false );
        userManager.newRole( "role1" );
        userManager.addRoleToUser( "role1", "nopermission" );
        // should be able to invoke allowed procedure
        assertSuccess( neo.login( "nopermission", "abc" ), "CALL test.allowedWriteProcedure()",
                itr -> assertEquals( itr.stream().collect( toList() ).size(), 2 ) );
        // should not be able to do writes
        assertFail( neo.login( "nopermission", "abc" ),
                "CALL test.allowedWriteProcedure() YIELD value CREATE (:NEWNODE {name:value})", WRITE_OPS_NOT_ALLOWED );
    }

    @Test
    public void shouldNotAllowUnauthorizedAccessToProcedure() throws Exception
    {
        userManager = neo.getLocalUserManager();
        userManager.newUser( "nopermission", "abc", false );
        // should not be able to invoke any procedure
        assertFail( neo.login( "nopermission", "abc" ), "CALL test.staticReadProcedure()", READ_OPS_NOT_ALLOWED );
        assertFail( neo.login( "nopermission", "abc" ), "CALL test.staticWriteProcedure()", WRITE_OPS_NOT_ALLOWED );
        assertFail( neo.login( "nopermission", "abc" ), "CALL test.staticSchemaProcedure()", SCHEMA_OPS_NOT_ALLOWED );
    }

    @Test
    public void shouldNotAllowNonReaderToReadAfterCallingAllowedReadProc() throws Exception
    {
        userManager = neo.getLocalUserManager();
        userManager.newUser( "nopermission", "abc", false );
        userManager.newRole( "role1" );
        userManager.addRoleToUser( "role1", "nopermission" );
        // should not be able to invoke any procedure
        assertSuccess( neo.login( "nopermission", "abc" ), "CALL test.allowedReadProcedure()",
                itr -> assertEquals( itr.stream().collect( toList() ).size(), 1 ) );
        assertFail( neo.login( "nopermission", "abc" ),
                "CALL test.allowedReadProcedure() YIELD value MATCH (n:Secret) RETURN n.pass", READ_OPS_NOT_ALLOWED );
    }

    @Test
    public void shouldHandleNestedReadProcedures() throws Throwable
    {
        userManager = neo.getLocalUserManager();
        userManager.newUser( "role1Subject", "abc", false );
        userManager.newRole( "role1" );
        userManager.addRoleToUser( "role1", "role1Subject" );
        assertSuccess( neo.login( "role1Subject", "abc" ),
                "CALL test.nestedAllowedProcedure('test.allowedReadProcedure') YIELD value",
                r -> assertKeyIs( r, "value", "foo" ) );
    }

    @Test
    public void shouldHandleDoubleNestedReadProcedures() throws Throwable
    {
        userManager = neo.getLocalUserManager();
        userManager.newUser( "role1Subject", "abc", false );
        userManager.newRole( "role1" );
        userManager.addRoleToUser( "role1", "role1Subject" );
        assertSuccess( neo.login( "role1Subject", "abc" ),
                "CALL test.doubleNestedAllowedProcedure YIELD value",
                r -> assertKeyIs( r, "value", "foo" ) );
    }

    @Test
    public void shouldFailNestedAllowedWriteProcedureFromAllowedReadProcedure() throws Throwable
    {
        userManager = neo.getLocalUserManager();
        userManager.newUser( "role1Subject", "abc", false );
        userManager.newRole( "role1" );
        userManager.addRoleToUser( "role1", "role1Subject" );
        assertFail( neo.login( "role1Subject", "abc" ),
                "CALL test.nestedAllowedProcedure('test.allowedWriteProcedure') YIELD value",
                WRITE_OPS_NOT_ALLOWED );
    }

    @Test
    public void shouldFailNestedAllowedWriteProcedureFromAllowedReadProcedureEvenIfAdmin() throws Throwable
    {
        userManager = neo.getLocalUserManager();
        userManager.newUser( "role1Subject", "abc", false );
        userManager.newRole( "role1" );
        userManager.addRoleToUser( "role1", "role1Subject" );
        userManager.addRoleToUser( PredefinedRoles.ADMIN, "role1Subject" );
        assertFail( neo.login( "role1Subject", "abc" ),
                "CALL test.nestedAllowedProcedure('test.allowedWriteProcedure') YIELD value",
                WRITE_OPS_NOT_ALLOWED );
    }

    @Test
    public void shouldRestrictNestedReadProcedureFromAllowedWriteProcedures() throws Throwable
    {
        userManager = neo.getLocalUserManager();
        userManager.newUser( "role1Subject", "abc", false );
        userManager.newRole( "role1" );
        userManager.addRoleToUser( "role1", "role1Subject" );
        assertFail( neo.login( "role1Subject", "abc" ),
                "CALL test.failingNestedAllowedWriteProcedure YIELD value",
                WRITE_OPS_NOT_ALLOWED );
    }

    @Test
    public void shouldHandleNestedReadProcedureWithDifferentAllowedRole() throws Throwable
    {
        userManager = neo.getLocalUserManager();
        userManager.newUser( "role1Subject", "abc", false );
        userManager.newRole( "role1" );
        userManager.addRoleToUser( "role1", "role1Subject" );
        assertSuccess( neo.login( "role1Subject", "abc" ),
                "CALL test.nestedAllowedProcedure('test.otherAllowedReadProcedure') YIELD value",
                r -> assertKeyIs( r, "value", "foo" )
        );
    }

    @Test
    public void shouldFailNestedAllowedWriteProcedureFromNormalReadProcedure() throws Throwable
    {
        userManager = neo.getLocalUserManager();
        userManager.newUser( "role1Subject", "abc", false );
        userManager.newRole( "role1" );
        userManager.addRoleToUser( "role1", "role1Subject" );
        userManager.addRoleToUser( PredefinedRoles.PUBLISHER, "role1Subject" ); // Even if subject has WRITE permission
        // the procedure should restrict to READ
        assertFail( neo.login( "role1Subject", "abc" ),
                "CALL test.nestedReadProcedure('test.allowedWriteProcedure') YIELD value",
                WRITE_OPS_NOT_ALLOWED );
    }

    @Test
    public void shouldHandleFunctionWithAllowed() throws Throwable
    {
        userManager = neo.getLocalUserManager();

        userManager.newUser( "role1Subject", "abc", false );
        userManager.newRole( "role1" );
        userManager.addRoleToUser( "role1", "role1Subject" );
        assertSuccess( neo.login( "role1Subject", "abc" ),
                "RETURN test.allowedFunction1() AS value",
                r -> assertKeyIs( r, "value", "foo" ) );
    }

    @Test
    public void shouldHandleNestedFunctionsWithAllowed() throws Throwable
    {
        userManager = neo.getLocalUserManager();

        userManager.newUser( "role1Subject", "abc", false );
        userManager.newRole( "role1" );
        userManager.addRoleToUser( "role1", "role1Subject" );
        assertSuccess( neo.login( "role1Subject", "abc" ),
                "RETURN test.nestedAllowedFunction('test.allowedFunction1()') AS value",
                r -> assertKeyIs( r, "value", "foo" ) );
    }

    @Test
    public void shouldHandleNestedFunctionWithDifferentAllowedRole() throws Throwable
    {
        userManager = neo.getLocalUserManager();

        userManager.newUser( "role1Subject", "abc", false );
        userManager.newRole( "role1" );
        userManager.addRoleToUser( "role1", "role1Subject" );
        assertSuccess( neo.login( "role1Subject", "abc" ),
                "RETURN test.nestedAllowedFunction('test.allowedFunction2()') AS value",
                r -> assertKeyIs( r, "value", "foo" )
        );
    }

    //---------- clearing query cache -----------

    @Test
    public void shouldNotClearQueryCachesIfNotAdmin()
    {
        assertFail( noneSubject, "CALL dbms.clearQueryCaches()", PERMISSION_DENIED );
        assertFail( readSubject, "CALL dbms.clearQueryCaches()", PERMISSION_DENIED );
        assertFail( writeSubject, "CALL dbms.clearQueryCaches()", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.clearQueryCaches()", PERMISSION_DENIED );
    }

    @Test
    public void shouldClearQueryCachesIfAdmin()
    {
        assertSuccess( adminSubject,"CALL dbms.clearQueryCaches()", ResourceIterator::close );
        // any answer is okay, as long as it isn't denied. That is why we don't care about the actual result here
    }

    /*
    This surface is hidden in 3.1, to possibly be completely removed or reworked later
    ==================================================================================
     */

    //---------- terminate transactions for user -----------

    //@Test
    public void shouldTerminateTransactionForUser() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 2 );
        ThreadedTransaction<S> write = new ThreadedTransaction<>( neo, latch );
        write.executeCreateNode( threading, writeSubject );
        latch.startAndWaitForAllToStart();

        assertSuccess( adminSubject, "CALL dbms.terminateTransactionsForUser( 'writeSubject' )",
                r -> assertKeyIsMap( r, "username", "transactionsTerminated", map( "writeSubject", "1" ) ) );

        assertSuccess( adminSubject, "CALL dbms.listTransactions()",
                r -> assertKeyIsMap( r, "username", "activeTransactions", map( "adminSubject", "1" ) ) );

        latch.finishAndWaitForAllToFinish();

        write.closeAndAssertExplicitTermination();

        assertEmpty( adminSubject, "MATCH (n:Test) RETURN n.name AS name" );
    }

    //@Test
    public void shouldTerminateOnlyGivenUsersTransaction() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 3 );
        ThreadedTransaction<S> schema = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> write = new ThreadedTransaction<>( neo, latch );

        schema.executeCreateNode( threading, schemaSubject );
        write.executeCreateNode( threading, writeSubject );
        latch.startAndWaitForAllToStart();

        assertSuccess( adminSubject, "CALL dbms.terminateTransactionsForUser( 'schemaSubject' )",
                r -> assertKeyIsMap( r, "username", "transactionsTerminated", map( "schemaSubject", "1" ) ) );

        assertSuccess( adminSubject, "CALL dbms.listTransactions()",
                r -> assertKeyIsMap( r, "username", "activeTransactions",
                        map( "adminSubject", "1", "writeSubject", "1" ) ) );

        latch.finishAndWaitForAllToFinish();

        schema.closeAndAssertExplicitTermination();
        write.closeAndAssertSuccess();

        assertSuccess( adminSubject, "MATCH (n:Test) RETURN n.name AS name",
                r -> assertKeyIs( r, "name", "writeSubject-node" ) );
    }

    //@Test
    public void shouldTerminateAllTransactionsForGivenUser() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 3 );
        ThreadedTransaction<S> schema1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> schema2 = new ThreadedTransaction<>( neo, latch );

        schema1.executeCreateNode( threading, schemaSubject );
        schema2.executeCreateNode( threading, schemaSubject );
        latch.startAndWaitForAllToStart();

        assertSuccess( adminSubject, "CALL dbms.terminateTransactionsForUser( 'schemaSubject' )",
                r -> assertKeyIsMap( r, "username", "transactionsTerminated", map( "schemaSubject", "2" ) ) );

        assertSuccess( adminSubject, "CALL dbms.listTransactions()",
                r -> assertKeyIsMap( r, "username", "activeTransactions", map( "adminSubject", "1" ) ) );

        latch.finishAndWaitForAllToFinish();

        schema1.closeAndAssertExplicitTermination();
        schema2.closeAndAssertExplicitTermination();

        assertEmpty( adminSubject, "MATCH (n:Test) RETURN n.name AS name" );
    }

    //@Test
    public void shouldNotTerminateTerminationTransaction()
    {
        assertSuccess( adminSubject, "CALL dbms.terminateTransactionsForUser( 'adminSubject' )",
                r -> assertKeyIsMap( r, "username", "transactionsTerminated", map( "adminSubject", "0" ) ) );
        assertSuccess( readSubject, "CALL dbms.terminateTransactionsForUser( 'readSubject' )",
                r -> assertKeyIsMap( r, "username", "transactionsTerminated", map( "readSubject", "0" ) ) );
    }

    //@Test
    public void shouldTerminateSelfTransactionsExceptTerminationTransactionIfAdmin() throws Throwable
    {
        shouldTerminateSelfTransactionsExceptTerminationTransaction( adminSubject );
    }

    //@Test
    public void shouldTerminateSelfTransactionsExceptTerminationTransactionIfNotAdmin() throws Throwable
    {
        shouldTerminateSelfTransactionsExceptTerminationTransaction( writeSubject );
    }

    private void shouldTerminateSelfTransactionsExceptTerminationTransaction( S subject ) throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 2 );
        ThreadedTransaction<S> create = new ThreadedTransaction<>( neo, latch );
        create.executeCreateNode( threading, subject );

        latch.startAndWaitForAllToStart();

        String subjectName = neo.nameOf( subject );
        assertSuccess( subject, "CALL dbms.terminateTransactionsForUser( '" + subjectName + "' )",
                r -> assertKeyIsMap( r, "username", "transactionsTerminated", map( subjectName, "1" ) ) );

        latch.finishAndWaitForAllToFinish();

        create.closeAndAssertExplicitTermination();

        assertEmpty( adminSubject, "MATCH (n:Test) RETURN n.name AS name" );
    }

    //@Test
    public void shouldNotTerminateTransactionsIfNonExistentUser()
    {
        assertFail( adminSubject, "CALL dbms.terminateTransactionsForUser( 'Petra' )", "User 'Petra' does not exist" );
        assertFail( adminSubject, "CALL dbms.terminateTransactionsForUser( '' )", "User '' does not exist" );
    }

    //@Test
    public void shouldNotTerminateTransactionsIfNotAdmin() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 2 );
        ThreadedTransaction<S> write = new ThreadedTransaction<>( neo, latch );
        write.executeCreateNode( threading, writeSubject );
        latch.startAndWaitForAllToStart();

        assertFail( noneSubject, "CALL dbms.terminateTransactionsForUser( 'writeSubject' )", PERMISSION_DENIED );
        assertFail( pwdSubject, "CALL dbms.terminateTransactionsForUser( 'writeSubject' )", CHANGE_PWD_ERR_MSG );
        assertFail( readSubject, "CALL dbms.terminateTransactionsForUser( 'writeSubject' )", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.terminateTransactionsForUser( 'writeSubject' )", PERMISSION_DENIED );

        assertSuccess( adminSubject, "CALL dbms.listTransactions()",
                r -> assertKeyIs( r, "username", "adminSubject", "writeSubject" ) );

        latch.finishAndWaitForAllToFinish();

        write.closeAndAssertSuccess();

        assertSuccess( adminSubject, "MATCH (n:Test) RETURN n.name AS name",
                r -> assertKeyIs( r, "name", "writeSubject-node" ) );
    }

    //@Test
    public void shouldTerminateRestrictedTransaction()
    {
        final DoubleLatch doubleLatch = new DoubleLatch( 2 );

        ClassWithProcedures.setTestLatch(
                new ClassWithProcedures.LatchedRunnables( doubleLatch, EMPTY_RUNNABLE, EMPTY_RUNNABLE ) );

        new Thread( () -> assertFail( writeSubject, "CALL test.waitForLatch()", "Explicitly terminated by the user." ) )
                .start();

        doubleLatch.startAndWaitForAllToStart();
        try
        {
            assertSuccess( adminSubject, "CALL dbms.terminateTransactionsForUser( 'writeSubject' )",
                    r -> assertKeyIsMap( r, "username", "transactionsTerminated", map( "writeSubject", "1" ) ) );
        }
        finally
        {
            doubleLatch.finishAndWaitForAllToFinish();
        }
    }

    private void assertQueryIsRunning( String query ) throws InterruptedException
    {
        assertEventually( "Query did not appear in dbms.listQueries output",
                () -> queryIsRunning( query ),
                equalTo( true ),
                1, TimeUnit.MINUTES );
    }

    private boolean queryIsRunning( String targetQuery )
    {
        String query = "CALL dbms.listQueries() YIELD query WITH query WHERE query = '" + targetQuery + "' RETURN 1";
        MutableBoolean resultIsNotEmpty = new MutableBoolean();
        neo.executeQuery( adminSubject, query, emptyMap(), itr -> resultIsNotEmpty.setValue( itr.hasNext() ) );
        return resultIsNotEmpty.booleanValue();
    }

    /*
    ==================================================================================
     */

    //---------- jetty helpers for serving CSV files -----------

    private int getLocalPort( Server server )
    {
        return ((ServerConnector) (server.getConnectors()[0])).getLocalPort();

    }

    //---------- matchers-----------

    private Matcher<Map<String,Object>> listedTransactionOfInteractionLevel( OffsetDateTime startTime, String
            username, String currentQuery )
    {
        return allOf(
                hasCurrentQuery( currentQuery ),
                hasUsername( username ),
                hasTransactionId(),
                hasStartTimeAfter( startTime ),
                hasProtocol( neo.getConnectionProtocol() )
        );
    }

    private Matcher<Map<String,Object>> listedQuery( OffsetDateTime startTime, String username, String query )
    {
        return allOf(
                hasQuery( query ),
                hasUsername( username ),
                hasQueryId(),
                hasStartTimeAfter( startTime ),
                hasNoParameters()
        );
    }

    private Matcher<Map<String,Object>> listedTransaction( OffsetDateTime startTime, String username, String currentQuery )
    {
        return allOf(
                hasCurrentQuery( currentQuery ),
                hasUsername( username ),
                hasTransactionId(),
                hasStartTimeAfter( startTime )
        );
    }

    /**
     * Executes a query through the NeoInteractionLevel required
     */
    private Matcher<Map<String,Object>> listedQueryOfInteractionLevel( OffsetDateTime startTime, String username,
            String query )
    {
        return allOf(
                hasQuery( query ),
                hasUsername( username ),
                hasQueryId(),
                hasStartTimeAfter( startTime ),
                hasNoParameters(),
                hasProtocol( neo.getConnectionProtocol() )
        );
    }

    private Matcher<Map<String,Object>> listedQueryWithMetaData( OffsetDateTime startTime, String username,
            String query, Map<String,Object> metaData )
    {
        return allOf(
                hasQuery( query ),
                hasUsername( username ),
                hasQueryId(),
                hasStartTimeAfter( startTime ),
                hasNoParameters(),
                hasMetaData( metaData )
        );
    }

    private Matcher<Map<String,Object>> listedTransactionWithMetaData( OffsetDateTime startTime, String username,
            String currentQuery, Map<String,Object> metaData )
    {
        return allOf(
                hasCurrentQuery( currentQuery ),
                hasUsername( username ),
                hasTransactionId(),
                hasStartTimeAfter( startTime ),
                hasMetaData( metaData )
        );
    }

    @SuppressWarnings( "unchecked" )
    private Matcher<Map<String,Object>> hasQuery( String query )
    {
        return (Matcher) hasEntry( equalTo( "query" ), equalTo( query ) );
    }

    private Matcher<Map<String,Object>> hasCurrentQuery( String currentQuery )
    {
        return (Matcher) hasEntry( equalTo( "currentQuery" ), equalTo( currentQuery ) );
    }

    private Matcher<Map<String,Object>> hasStatus( String statusPrefix )
    {
        return (Matcher) hasEntry( equalTo( "status" ), startsWith( statusPrefix ) );
    }

    @SuppressWarnings( "unchecked" )
    private Matcher<Map<String,Object>> hasUsername( String username )
    {
        return (Matcher) hasEntry( equalTo( "username" ), equalTo( username ) );
    }

    @SuppressWarnings( "unchecked" )
    private Matcher<Map<String,Object>> hasQueryId()
    {
        Matcher<String> queryId = equalTo( "queryId" );
        Matcher valueMatcher =
                allOf( (Matcher) isA( String.class ), (Matcher) containsString( QueryId.QUERY_ID_PREFIX ) );
        return hasEntry( queryId, valueMatcher );
    }

    private Matcher<Map<String,Object>> hasTransactionId()
    {
        Matcher<String> transactionId = equalTo( "transactionId" );
        Matcher valueMatcher =
                allOf( (Matcher) isA( String.class ), (Matcher) containsString( "transaction-" ) );
        return hasEntry( transactionId, valueMatcher );
    }

    @SuppressWarnings( "unchecked" )
    private Matcher<Map<String,Object>> hasStartTimeAfter( OffsetDateTime startTime )
    {
        return (Matcher) hasEntry( equalTo( "startTime" ), new BaseMatcher<String>()
        {
            @Override
            public void describeTo( Description description )
            {
                description.appendText( "should be after " + startTime.toString() );
            }

            @Override
            public boolean matches( Object item )
            {
                OffsetDateTime otherTime = from( ISO_OFFSET_DATE_TIME.parse( item.toString() ) );
                return startTime.compareTo( otherTime ) <= 0;
            }
        } );
    }

    @SuppressWarnings( "unchecked" )
    private Matcher<Map<String,Object>> hasNoParameters()
    {
        return (Matcher) hasEntry( equalTo( "parameters" ), equalTo( emptyMap() ) );
    }

    @SuppressWarnings( "unchecked" )
    private Matcher<Map<String,Object>> hasProtocol( String expected )
    {
        return (Matcher) hasEntry( "protocol", expected );
    }

    @SuppressWarnings( "unchecked" )
    private Matcher<Map<String,Object>> hasMetaData( Map<String,Object> expected )
    {
        return (Matcher) hasEntry( equalTo( "metaData" ), allOf(
                expected.entrySet().stream().map(
                        entryMapper()
                ).collect( Collectors.toList() )
        ) );
    }

    @SuppressWarnings( {"rawtypes", "unchecked"} )
    private Function<Entry<String,Object>,Matcher<Entry<String,Object>>> entryMapper()
    {
        return entry ->
        {
            Matcher keyMatcher = equalTo( entry.getKey() );
            Matcher valueMatcher = equalTo( entry.getValue() );
            return hasEntry( keyMatcher, valueMatcher );
        };
    }

    private List<Map<String,Object>> collectResults( ResourceIterator<Map<String,Object>> results )
    {
        List<Map<String,Object>> maps = results.stream().collect( Collectors.toList() );
        List<Map<String,Object>> transformed = new ArrayList<>( maps.size() );
        for ( Map<String,Object> map : maps )
        {
            Map<String,Object> transformedMap = new HashMap<>( map.size() );
            for ( Entry<String,Object> entry : map.entrySet() )
            {
                transformedMap.put( entry.getKey(), toRawValue( entry.getValue() ) );
            }
            transformed.add( transformedMap );
        }
        return transformed;
    }

    private static OffsetDateTime getStartTime()
    {
        return ofInstant( Instant.ofEpochMilli( now().toEpochSecond() ), ZoneOffset.UTC );
    }
}
