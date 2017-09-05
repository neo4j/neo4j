/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.integration;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.regex.Pattern;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.BoltConnectionDescriptor;
import org.neo4j.bolt.v1.runtime.BoltConnectionFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.ValueUtils;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.Barrier;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.rule.SuppressOutput;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
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
    private static final BoltChannel boltChannel = mock( BoltChannel.class );
    @Rule
    public SessionRule env = new SessionRule();
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldHandleBeginCommit() throws Throwable
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // When
        machine.run( "BEGIN", EMPTY_MAP, recorder );
        machine.discardAll( nullResponseHandler() );

        machine.run( "CREATE (n:InTx)", EMPTY_MAP, recorder );
        machine.discardAll( nullResponseHandler() );

        machine.run( "COMMIT", EMPTY_MAP, recorder );
        machine.discardAll( nullResponseHandler() );

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
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // When
        machine.run( "BEGIN", EMPTY_MAP, recorder );
        machine.discardAll( nullResponseHandler() );

        machine.run( "CREATE (n:InTx)", EMPTY_MAP, recorder );
        machine.discardAll( nullResponseHandler() );

        machine.run( "ROLLBACK", EMPTY_MAP, recorder );
        machine.discardAll( nullResponseHandler() );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    public void shouldFailNicelyWhenOutOfOrderRollback() throws Throwable
    {
        // Given
        BoltResponseRecorder runRecorder = new BoltResponseRecorder();
        BoltResponseRecorder pullAllRecorder = new BoltResponseRecorder();
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // When
        machine.run( "ROLLBACK", EMPTY_MAP, runRecorder );
        machine.pullAll( pullAllRecorder );

        // Then
        assertThat( runRecorder.nextResponse(), failedWithStatus( Status.Statement.SemanticError ) );
        assertThat( pullAllRecorder.nextResponse(), wasIgnored() );
    }

    @Test
    public void shouldReceiveBookmarkOnCommitAndDiscardAll() throws Throwable
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // When
        machine.run( "BEGIN", EMPTY_MAP, recorder );
        machine.discardAll( recorder );

        machine.run( "CREATE (a:Person)", EMPTY_MAP, recorder );
        machine.discardAll( recorder );

        machine.run( "COMMIT", EMPTY_MAP, recorder );
        machine.discardAll( recorder );

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
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // When
        machine.run( "BEGIN", EMPTY_MAP, recorder );
        machine.discardAll( recorder );

        machine.run( "CREATE (a:Person)", EMPTY_MAP, recorder );
        machine.discardAll( recorder );

        machine.run( "COMMIT", EMPTY_MAP, recorder );
        machine.pullAll( recorder );

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
        Thread thread = new Thread()
        {
            @Override
            public void run()
            {
                try ( BoltStateMachine machine = env.newMachine( boltChannel ) )
                {
                    machine.init( USER_AGENT, emptyMap(), null );
                    latch.await();
                    machine.run( "MATCH (n:A) SET n.prop = 'two'", EMPTY_MAP, nullResponseHandler() );
                    machine.pullAll( nullResponseHandler() );
                }
                catch ( BoltConnectionFatality connectionFatality )
                {
                    throw new RuntimeException( connectionFatality );
                }
            }
        };
        thread.start();

        long dbVersionAfterWrite = dbVersion + 1;
        try ( BoltStateMachine machine = env.newMachine( boltChannel ) )
        {
            BoltResponseRecorder recorder = new BoltResponseRecorder();
            machine.init( USER_AGENT, emptyMap(), null );
            latch.release();
            final String bookmark = "neo4j:bookmark:v1:tx" + Long.toString( dbVersionAfterWrite );
            machine.run( "BEGIN", ValueUtils.asMapValue( singletonMap( "bookmark", bookmark ) ), nullResponseHandler() );
            machine.pullAll( recorder );
            machine.run( "MATCH (n:A) RETURN n.prop", EMPTY_MAP, nullResponseHandler() );
            machine.pullAll( recorder );
            machine.run( "COMMIT", EMPTY_MAP, nullResponseHandler() );
            machine.pullAll( recorder );

            assertThat( recorder.nextResponse(), succeededWithMetadata( "bookmark", BOOKMARK_PATTERN ) );
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

        Thread thread = new Thread()
        {
            @Override
            public void run()
            {
                try ( BoltStateMachine stateMachine = env.newMachine( mock( BoltChannel.class ) ) )
                {
                    machine[0] = stateMachine;
                    stateMachine.init( USER_AGENT, emptyMap(), null );
                    String query = format( "USING PERIODIC COMMIT 10 LOAD CSV FROM 'http://localhost:%d' AS line " +
                                           "CREATE (n:A {id: line[0], square: line[1]}) " +
                                           "WITH count(*) as number " +
                                           "CREATE (n:ShouldNotExist)",
                                           localPort );
                    try
                    {
                        latch.start();
                        stateMachine.run( query, EMPTY_MAP, nullResponseHandler() );
                        stateMachine.pullAll( nullResponseHandler() );
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
            }
        };
        thread.setName( "query runner" );
        thread.start();

        // We block this thread here, waiting for the http server to spin up and the running query to get started
        latch.startAndWaitForAllToStart();
        Thread.sleep( 1000 );

        // This is the call that RESETs the Bolt connection and will terminate the running query
        machine[0].reset( nullResponseHandler() );

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
        final BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // When
        machine.run( "RETURN 1", EMPTY_MAP, nullResponseHandler() );
        machine.pullAll( recorder );
        machine.run( "", EMPTY_MAP, nullResponseHandler() );
        machine.pullAll( recorder );

        // Then
        assertThat( recorder.nextResponse(), succeededWithRecord( 1L ) );
        assertThat( recorder.nextResponse(), succeededWithRecord( 1L ) );
    }

    @Test
    public void shouldInterpretEmptyStatementAsReuseLastStatementInExplicitTransaction() throws Throwable
    {
        // Given
        final BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // When
        machine.run( "BEGIN", EMPTY_MAP, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );
        machine.run( "RETURN 1", EMPTY_MAP, nullResponseHandler() );
        machine.pullAll( recorder );
        machine.run( "", EMPTY_MAP, nullResponseHandler() );
        machine.pullAll( recorder );
        machine.run( "COMMIT", EMPTY_MAP, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );

        // Then
        assertThat( recorder.nextResponse(), succeededWithRecord( 1L ) );
        assertThat( recorder.nextResponse(), succeededWithRecord( 1L ) );
    }

    @Test
    public void beginShouldNotOverwriteLastStatement() throws Throwable
    {
        // Given
        final BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // When
        machine.run( "RETURN 1", EMPTY_MAP, nullResponseHandler() );
        machine.pullAll( recorder );
        machine.run( "BEGIN", EMPTY_MAP, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );
        machine.run( "", EMPTY_MAP, nullResponseHandler() );
        machine.pullAll( recorder );
        machine.run( "COMMIT", EMPTY_MAP, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );

        // Then
        assertThat( recorder.nextResponse(), succeededWithRecord( 1L ) );
        assertThat( recorder.nextResponse(), succeededWithRecord( 1L ) );
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
                    throws IOException, ServletException
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
