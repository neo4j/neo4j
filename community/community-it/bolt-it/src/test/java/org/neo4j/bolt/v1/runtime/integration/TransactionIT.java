/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.v1.runtime.integration;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.testing.BoltTestUtil;
import org.neo4j.bolt.v1.messaging.request.AckFailureMessage;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.messaging.request.RunMessage;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.test.Barrier;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.util.concurrent.BinaryLatch;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.bolt.testing.BoltMatchers.containsRecord;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.succeededWithMetadata;
import static org.neo4j.bolt.testing.BoltMatchers.succeededWithRecord;
import static org.neo4j.bolt.testing.BoltMatchers.wasIgnored;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;


public class TransactionIT
{
    private static final String USER_AGENT = "TransactionIT/0.0";
    private static final Pattern BOOKMARK_PATTERN = Pattern.compile( "neo4j:bookmark:v1:tx[0-9]+" );
    private static final BoltChannel BOLT_CHANNEL = BoltTestUtil.newTestBoltChannel();

    @Rule
    public SessionRule env = new SessionRule();
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldHandleBeginCommit() throws Throwable
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        BoltStateMachine machine = env.newMachine( BOLT_CHANNEL );
        machine.process( new InitMessage( USER_AGENT, emptyMap() ), nullResponseHandler() );

        // When
        machine.process( new RunMessage( "BEGIN", EMPTY_MAP ), recorder );
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );

        machine.process( new RunMessage( "CREATE (n:InTx)", EMPTY_MAP ), recorder );
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );

        machine.process( new RunMessage( "COMMIT", EMPTY_MAP ), recorder );
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    public void shouldHandleBeginRollback() throws Throwable
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        BoltStateMachine machine = env.newMachine( BOLT_CHANNEL );
        machine.process( new InitMessage( USER_AGENT, emptyMap() ), nullResponseHandler() );

        // When
        machine.process( new RunMessage( "BEGIN", EMPTY_MAP ), recorder );
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );

        machine.process( new RunMessage( "CREATE (n:InTx)", EMPTY_MAP ), recorder );
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );

        machine.process( new RunMessage( "ROLLBACK", EMPTY_MAP ), recorder );
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    public void shouldNotFailWhenOutOfOrderRollbackInAutoCommitMode() throws Throwable
    {
        // Given
        BoltResponseRecorder runRecorder = new BoltResponseRecorder();
        BoltResponseRecorder pullAllRecorder = new BoltResponseRecorder();
        BoltStateMachine machine = env.newMachine( BOLT_CHANNEL );
        machine.process( new InitMessage( USER_AGENT, emptyMap() ), nullResponseHandler() );

        // When
        machine.process( new RunMessage( "ROLLBACK", EMPTY_MAP ), runRecorder );
        machine.process( PullAllMessage.INSTANCE, pullAllRecorder );

        // Then
        assertThat( runRecorder.nextResponse(), succeeded() );
        assertThat( pullAllRecorder.nextResponse(), succeeded() );
    }

    @Test
    public void shouldReceiveBookmarkOnCommitAndDiscardAll() throws Throwable
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        BoltStateMachine machine = env.newMachine( BOLT_CHANNEL );
        machine.process( new InitMessage( USER_AGENT, emptyMap() ), nullResponseHandler() );

        // When
        machine.process( new RunMessage( "BEGIN", EMPTY_MAP ), recorder );
        machine.process( DiscardAllMessage.INSTANCE, recorder );

        machine.process( new RunMessage( "CREATE (a:Person)", EMPTY_MAP ), recorder );
        machine.process( DiscardAllMessage.INSTANCE, recorder );

        machine.process( new RunMessage( "COMMIT", EMPTY_MAP ), recorder );
        machine.process( DiscardAllMessage.INSTANCE, recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeededWithMetadata( "bookmark", BOOKMARK_PATTERN ) );
    }

    @Test
    public void shouldReceiveBookmarkOnCommitAndPullAll() throws Throwable
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        BoltStateMachine machine = env.newMachine( BOLT_CHANNEL );
        machine.process( new InitMessage( USER_AGENT, emptyMap() ), nullResponseHandler() );

        // When
        machine.process( new RunMessage( "BEGIN", EMPTY_MAP ), recorder );
        machine.process( DiscardAllMessage.INSTANCE, recorder );

        machine.process( new RunMessage( "CREATE (a:Person)", EMPTY_MAP ), recorder );
        machine.process( DiscardAllMessage.INSTANCE, recorder );

        machine.process( new RunMessage( "COMMIT", EMPTY_MAP ), recorder );
        machine.process( PullAllMessage.INSTANCE, recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeededWithMetadata( "bookmark", BOOKMARK_PATTERN ) );
    }

    @Test
    public void shouldReadYourOwnWrites() throws Exception
    {
        try ( Transaction tx = env.graph().beginTx() )
        {
            Node node = env.graph().createNode( Label.label( "A" ) );
            node.setProperty( "prop", "one" );
            tx.success();
        }

        BinaryLatch latch = new BinaryLatch();

        long dbVersion = env.lastClosedTxId();
        Thread thread = new Thread( () -> {
            try ( BoltStateMachine machine = env.newMachine( BOLT_CHANNEL ) )
            {
                machine.process( new InitMessage( USER_AGENT, emptyMap() ), nullResponseHandler() );
                latch.await();
                machine.process( new RunMessage( "MATCH (n:A) SET n.prop = 'two'", EMPTY_MAP ), nullResponseHandler() );
                machine.process( PullAllMessage.INSTANCE, nullResponseHandler() );
            }
            catch ( BoltConnectionFatality connectionFatality )
            {
                throw new RuntimeException( connectionFatality );
            }
        } );
        thread.start();

        long dbVersionAfterWrite = dbVersion + 1;
        try ( BoltStateMachine machine = env.newMachine( BOLT_CHANNEL ) )
        {
            BoltResponseRecorder recorder = new BoltResponseRecorder();
            machine.process( new InitMessage( USER_AGENT, emptyMap() ), nullResponseHandler() );
            latch.release();
            final String bookmark = "neo4j:bookmark:v1:tx" + dbVersionAfterWrite;
            machine.process( new RunMessage( "BEGIN", ValueUtils.asMapValue( singletonMap( "bookmark", bookmark ) ) ), nullResponseHandler() );
            machine.process( PullAllMessage.INSTANCE, recorder );
            machine.process( new RunMessage( "MATCH (n:A) RETURN n.prop", EMPTY_MAP ), nullResponseHandler() );
            machine.process( PullAllMessage.INSTANCE, recorder );
            machine.process( new RunMessage( "COMMIT", EMPTY_MAP ), nullResponseHandler() );
            machine.process( PullAllMessage.INSTANCE, recorder );

            assertThat( recorder.nextResponse(), succeeded() );
            assertThat( recorder.nextResponse(), succeededWithRecord( "two" ) );
            assertThat( recorder.nextResponse(), succeededWithMetadata( "bookmark", BOOKMARK_PATTERN ) );
        }

        thread.join();
    }

    @Test
    public void shouldTerminateQueriesEvenIfUsingPeriodicCommit() throws Exception
    {
        // Spawns a throttled HTTP server, runs a PERIODIC COMMIT that fetches data from this server,
        // and checks that the query able to be terminated

        // We start with 3, because that is how many actors we have -
        // 1. the http server
        // 2. the running query
        // 3. the one terminating 2
        final DoubleLatch latch = new DoubleLatch( 3, true );

        // This is used to block the http server between the first and second batch
        final Barrier.Control barrier = new Barrier.Control();

        // Serve CSV via local web server, let Jetty find a random port for us
        Server server = createHttpServer( latch, barrier, 20, 30 );
        server.start();
        int localPort = getLocalPort( server );

        final BoltStateMachine[] machine = {null};

        Thread thread = new Thread( () -> {
            try ( BoltStateMachine stateMachine = env.newMachine( BOLT_CHANNEL ) )
            {
                machine[0] = stateMachine;
                stateMachine.process( new InitMessage( USER_AGENT, emptyMap() ), nullResponseHandler() );
                String query = format( "USING PERIODIC COMMIT 10 LOAD CSV FROM 'http://localhost:%d' AS line " +
                                       "CREATE (n:A {id: line[0], square: line[1]}) " +
                                       "WITH count(*) as number " +
                                       "CREATE (n:ShouldNotExist)",
                                       localPort );
                try
                {
                    latch.start();
                    stateMachine.process( new RunMessage( query, EMPTY_MAP ), nullResponseHandler() );
                    stateMachine.process( PullAllMessage.INSTANCE, nullResponseHandler() );
                }
                finally
                {
                    latch.finish();
                }
            }
            catch ( BoltConnectionFatality connectionFatality )
            {
                throw new RuntimeException( connectionFatality );
            }
        } );
        thread.setName( "query runner" );
        thread.start();

        // We block this thread here, waiting for the http server to spin up and the running query to get started
        latch.startAndWaitForAllToStart();
        Thread.sleep( 1000 );

        // This is the call that RESETs the Bolt connection and will terminate the running query
        machine[0].process( ResetMessage.INSTANCE, nullResponseHandler() );

        barrier.release();

        // We block again here, waiting for the running query to have been terminated, and for the server to have
        // wrapped up and finished streaming http results
        latch.finishAndWaitForAllToFinish();

        // And now we check that the last node did not get created
        try ( Transaction ignored = env.graph().beginTx() )
        {
            assertFalse( "Query was not terminated in time - nodes were created!",
                         env.graph().findNodes( Label.label( "ShouldNotExist" ) ).hasNext() );
        }
    }

    @Test
    public void shouldInterpretEmptyStatementAsReuseLastStatementInAutocommitTransaction() throws Throwable
    {
        // Given
        final BoltStateMachine machine = env.newMachine( BOLT_CHANNEL );
        machine.process( new InitMessage( USER_AGENT, emptyMap() ), nullResponseHandler() );
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // When
        machine.process( new RunMessage( "RETURN 1", EMPTY_MAP ), nullResponseHandler() );
        machine.process( PullAllMessage.INSTANCE, recorder );
        machine.process( new RunMessage( "", EMPTY_MAP ), nullResponseHandler() );
        machine.process( PullAllMessage.INSTANCE, recorder );

        // Then
        assertThat( recorder.nextResponse(), succeededWithRecord( 1L ) );
        assertThat( recorder.nextResponse(), succeededWithRecord( 1L ) );
    }

    @Test
    public void shouldInterpretEmptyStatementAsReuseLastStatementInExplicitTransaction() throws Throwable
    {
        // Given
        final BoltStateMachine machine = env.newMachine( BOLT_CHANNEL );
        machine.process( new InitMessage( USER_AGENT, emptyMap() ), nullResponseHandler() );
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // When
        machine.process( new RunMessage( "BEGIN", EMPTY_MAP ), nullResponseHandler() );
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );
        machine.process( new RunMessage( "RETURN 1", EMPTY_MAP ), nullResponseHandler() );
        machine.process( PullAllMessage.INSTANCE, recorder );
        machine.process( new RunMessage( "", EMPTY_MAP ), nullResponseHandler() );
        machine.process( PullAllMessage.INSTANCE, recorder );
        machine.process( new RunMessage( "COMMIT", EMPTY_MAP ), nullResponseHandler() );
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );

        // Then
        assertThat( recorder.nextResponse(), succeededWithRecord( 1L ) );
        assertThat( recorder.nextResponse(), succeededWithRecord( 1L ) );
    }

    @Test
    public void beginShouldNotOverwriteLastStatement() throws Throwable
    {
        // Given
        final BoltStateMachine machine = env.newMachine( BOLT_CHANNEL );
        machine.process( new InitMessage( USER_AGENT, emptyMap() ), nullResponseHandler() );
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // When
        machine.process( new RunMessage( "RETURN 1", EMPTY_MAP ), nullResponseHandler() );
        machine.process( PullAllMessage.INSTANCE, recorder );
        machine.process( new RunMessage( "BEGIN", EMPTY_MAP ), nullResponseHandler() );
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );
        machine.process( new RunMessage( "", EMPTY_MAP ), nullResponseHandler() );
        machine.process( PullAllMessage.INSTANCE, recorder );
        machine.process( new RunMessage( "COMMIT", EMPTY_MAP ), nullResponseHandler() );
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );

        // Then
        assertThat( recorder.nextResponse(), succeededWithRecord( 1L ) );
        assertThat( recorder.nextResponse(), succeededWithRecord( 1L ) );
    }

    @Test
    public void shouldCloseAutoCommitTransactionAndRespondToNextStatementWhenRunFails() throws Throwable
    {
        // Given
        final BoltStateMachine machine = env.newMachine( BOLT_CHANNEL );
        machine.process( new InitMessage( USER_AGENT, emptyMap() ), nullResponseHandler() );
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // When
        machine.process( new RunMessage( "INVALID QUERY", EMPTY_MAP ), recorder );
        machine.process( PullAllMessage.INSTANCE, recorder );
        machine.process( AckFailureMessage.INSTANCE, recorder );
        machine.process( new RunMessage( "RETURN 2", EMPTY_MAP ), recorder );
        machine.process( PullAllMessage.INSTANCE, recorder );

        // Then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Statement.SyntaxError ) );
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeededWithRecord( 2L ) );
        assertEquals( 0, recorder.responseCount() );
    }

    @Test
    public void shouldCloseAutoCommitTransactionAndRespondToNextStatementWhenStreamingFails() throws Throwable
    {
        // Given
        final BoltStateMachine machine = env.newMachine( BOLT_CHANNEL );
        machine.process( new InitMessage( USER_AGENT, emptyMap() ), nullResponseHandler() );
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // When
        machine.process( new RunMessage( "UNWIND [1, 0] AS x RETURN 1 / x", EMPTY_MAP ), recorder );
        machine.process( PullAllMessage.INSTANCE, recorder );
        machine.process( AckFailureMessage.INSTANCE, recorder );
        machine.process( new RunMessage( "RETURN 2", EMPTY_MAP ), recorder );
        machine.process( PullAllMessage.INSTANCE, recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), allOf( containsRecord( 1L ), failedWithStatus( Status.Statement.ArithmeticError ) ) );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeededWithRecord( 2L ) );
        assertEquals( 0, recorder.responseCount() );
    }

    public static Server createHttpServer(
            DoubleLatch latch, Barrier.Control innerBarrier, int firstBatchSize, int otherBatchSize )
    {
        Server server = new Server( 0 );
        server.setHandler( new AbstractHandler()
        {
            @Override
            public void handle(
                    String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response )
                    throws IOException
            {
                response.setContentType( "text/plain; charset=utf-8" );
                response.setStatus( HttpServletResponse.SC_OK );
                PrintWriter out = response.getWriter();

                writeBatch( out, firstBatchSize );
                out.flush();
                latch.start();
                innerBarrier.reached();

                latch.finish();
                writeBatch( out, otherBatchSize );
                baseRequest.setHandled(true);
            }

            private void writeBatch( PrintWriter out, int batchSize )
            {
                for ( int i = 0; i < batchSize; i++ )
                {
                    out.write( format( "%d %d\n", i, i * i ) );
                    i++;
                }
            }
        } );
        return server;
    }

    private int getLocalPort( Server server )
    {
        return ((ServerConnector) (server.getConnectors()[0])).getLocalPort();
    }

}
