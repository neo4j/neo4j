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
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.testing.NullResponseHandler;
import org.neo4j.kernel.api.exceptions.Status;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.wasIgnored;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.IGNORED;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.SUCCESS;

public class TransactionIT
{
    @Rule
    public SessionRule env = new SessionRule();

    @Test
    public void shouldHandleBeginCommit() throws Throwable
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        BoltStateMachine machine = env.newMachine( "<test>" );
        machine.init( "TestClient", emptyMap(), null );

        // When
        machine.run( "BEGIN", emptyMap(), recorder );
        machine.discardAll( new NullResponseHandler() );

        machine.run( "CREATE (n:InTx)", emptyMap(), recorder );
        machine.discardAll( new NullResponseHandler() );

        machine.run( "COMMIT", emptyMap(), recorder );
        machine.discardAll( new NullResponseHandler() );

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
        machine.init( "TestClient", emptyMap(), null );

        // When
        machine.run( "BEGIN", emptyMap(), recorder );
        machine.discardAll( new NullResponseHandler() );

        machine.run( "CREATE (n:InTx)", emptyMap(), recorder );
        machine.discardAll( new NullResponseHandler() );

        machine.run( "ROLLBACK", emptyMap(), recorder );
        machine.discardAll( new NullResponseHandler() );

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
        machine.init( "TestClient", emptyMap(), null );

        // When
        machine.run( "ROLLBACK", emptyMap(), runRecorder );
        machine.pullAll( pullAllRecorder );

        // Then
        assertThat( runRecorder.nextResponse(), failedWithStatus( Status.Statement.SemanticError ) );
        assertThat( pullAllRecorder.nextResponse(), wasIgnored() );
    }

    // TODO: re-enable when bookmarks implemented
//    @Test
//    public void shouldReadYourOwnWrites() throws Exception
//    {
//        try ( Transaction tx = env.graph().beginTx() )
//        {
//            Node node = env.graph().createNode( Label.label( "A" ) );
//            node.setProperty( "prop", "one" );
//            tx.success();
//        }
//
//        BinaryLatch latch = new BinaryLatch();
//
//        long dbVersion = env.lastClosedTxId();
//        Thread thread = new Thread()
//        {
//            @Override
//            public void run()
//            {
//                try ( Session session = env.newSession( "<write>" ) )
//                {
//                    session.init( "TestClient", emptyMap(), -1, null );
//                    latch.await();
//                    session.run( "MATCH (n:A) SET n.prop = 'two'", emptyMap(), Session.Callbacks.noop() );
//                    session.pullAll( Session.Callbacks.noop() );
//                }
//                catch ( ConnectionFatality connectionFatality )
//                {
//                    throw new RuntimeException( connectionFatality );
//                }
//            }
//        };
//        thread.start();
//
//        long dbVersionAfterWrite = dbVersion + 1;
//        try ( Session session = env.newSession( "<read>" ) )
//        {
//            session.init( "TestClient", emptyMap(), dbVersionAfterWrite, null );
//            latch.release();
//            session.run( "MATCH (n:A) RETURN n.prop", emptyMap(), Session.Callbacks.noop() );
//            RecordingCallback<RecordStream,Object> pullResponse = new RecordingCallback<>();
//            session.pullAll( pullResponse );
//
//            Record[] records = ((RecordingCallback.Result) pullResponse.next()).records();
//
//            assertEquals( 1, records.length );
//            assertThat( records[0], eqRecord( equalTo( "two" ) ) );
//        }
//
//        thread.join();
//    }
}
