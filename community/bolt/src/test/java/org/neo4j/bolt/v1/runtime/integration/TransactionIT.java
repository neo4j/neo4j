/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.v1.runtime.BoltConnectionFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.Status;

import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.succeededWithMetadata;
import static org.neo4j.bolt.testing.BoltMatchers.succeededWithRecord;
import static org.neo4j.bolt.testing.BoltMatchers.wasIgnored;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;

public class TransactionIT
{
    private static final String USER_AGENT = "TransactionIT/0.0";
    private static final Pattern BOOKMARK_PATTERN = Pattern.compile( "neo4j:bookmark:v1:tx[0-9]+" );

    @Rule
    public SessionRule env = new SessionRule();

    @Test
    public void shouldHandleBeginCommit() throws Throwable
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        BoltStateMachine machine = env.newMachine( "<test>" );
        machine.init( USER_AGENT, emptyMap(), null );

        // When
        machine.run( "BEGIN", emptyMap(), recorder );
        machine.discardAll( nullResponseHandler() );

        machine.run( "CREATE (n:InTx)", emptyMap(), recorder );
        machine.discardAll( nullResponseHandler() );

        machine.run( "COMMIT", emptyMap(), recorder );
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
        BoltStateMachine machine = env.newMachine( "<test>" );
        machine.init( USER_AGENT, emptyMap(), null );

        // When
        machine.run( "BEGIN", emptyMap(), recorder );
        machine.discardAll( nullResponseHandler() );

        machine.run( "CREATE (n:InTx)", emptyMap(), recorder );
        machine.discardAll( nullResponseHandler() );

        machine.run( "ROLLBACK", emptyMap(), recorder );
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
        BoltStateMachine machine = env.newMachine( "<test>" );
        machine.init( USER_AGENT, emptyMap(), null );

        // When
        machine.run( "ROLLBACK", emptyMap(), runRecorder );
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
        BoltStateMachine machine = env.newMachine( "<test>" );
        machine.init( USER_AGENT, emptyMap(), null );

        // When
        machine.run( "BEGIN", emptyMap(), recorder );
        machine.discardAll( recorder );

        machine.run( "CREATE (a:Person)", emptyMap(), recorder );
        machine.discardAll( recorder );

        machine.run( "COMMIT", emptyMap(), recorder );
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
        BoltStateMachine machine = env.newMachine( "<test>" );
        machine.init( USER_AGENT, emptyMap(), null );

        // When
        machine.run( "BEGIN", emptyMap(), recorder );
        machine.discardAll( recorder );

        machine.run( "CREATE (a:Person)", emptyMap(), recorder );
        machine.discardAll( recorder );

        machine.run( "COMMIT", emptyMap(), recorder );
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
                try ( BoltStateMachine machine = env.newMachine( "<write>" ) )
                {
                    machine.init( USER_AGENT, emptyMap(), null );
                    latch.await();
                    machine.run( "MATCH (n:A) SET n.prop = 'two'", emptyMap(), nullResponseHandler() );
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
        try ( BoltStateMachine machine = env.newMachine( "<read>" ) )
        {
            BoltResponseRecorder recorder = new BoltResponseRecorder();
            machine.init( USER_AGENT, emptyMap(), null );
            latch.release();
            final String bookmark = "neo4j:bookmark:v1:tx" + Long.toString( dbVersionAfterWrite );
            machine.run( "BEGIN", singletonMap( "bookmark", bookmark ), nullResponseHandler() );
            machine.pullAll( recorder );
            machine.run( "MATCH (n:A) RETURN n.prop", emptyMap(), nullResponseHandler() );
            machine.pullAll( recorder );
            machine.run( "COMMIT", emptyMap(), nullResponseHandler() );
            machine.pullAll( recorder );

            assertThat( recorder.nextResponse(), succeededWithMetadata( "bookmark", BOOKMARK_PATTERN ) );
            assertThat( recorder.nextResponse(), succeededWithRecord( "two" ) );
            assertThat( recorder.nextResponse(), succeededWithMetadata( "bookmark", BOOKMARK_PATTERN ) );
        }

        thread.join();
    }

}
