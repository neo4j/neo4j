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

import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.StatementMetadata;
import org.neo4j.bolt.v1.runtime.spi.Record;
import org.neo4j.bolt.v1.runtime.spi.RecordStream;
import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.neo4j.bolt.v1.runtime.integration.SessionMatchers.success;
import static org.neo4j.bolt.v1.runtime.spi.StreamMatchers.eqRecord;

public class TransactionIT
{
    @Rule
    public SessionRule env = new SessionRule();

    @Test
    public void shouldHandleBeginCommit() throws Throwable
    {
        // Given
        RecordingCallback<StatementMetadata,?> responses = new RecordingCallback<>();
        Session session = env.newSession( "<test>" );
        session.init( "TestClient", emptyMap(), -1, null );

        // When
        session.run( "BEGIN", emptyMap(), responses );
        session.discardAll( Session.Callbacks.noop() );

        session.run( "CREATE (n:InTx)", emptyMap(), responses );
        session.discardAll( Session.Callbacks.noop() );

        session.run( "COMMIT", emptyMap(), responses );
        session.discardAll( Session.Callbacks.noop() );

        // Then
        assertThat( responses.next(), success() );
        assertThat( responses.next(), success() );
        assertThat( responses.next(), success() );
    }

    @Test
    public void shouldHandleBeginRollback() throws Throwable
    {
        // Given
        RecordingCallback<StatementMetadata,?> responses = new RecordingCallback<>();
        Session session = env.newSession( "<test>" );
        session.init( "TestClient", emptyMap(), -1, null );

        // When
        session.run( "BEGIN", emptyMap(), responses );
        session.discardAll( Session.Callbacks.noop() );

        session.run( "CREATE (n:InTx)", emptyMap(), responses );
        session.discardAll( Session.Callbacks.noop() );

        session.run( "ROLLBACK", emptyMap(), responses );
        session.discardAll( Session.Callbacks.noop() );

        // Then
        assertThat( responses.next(), success() );
        assertThat( responses.next(), success() );
        assertThat( responses.next(), success() );
    }

    @Test
    public void shouldFailNicelyWhenOutOfOrderRollback() throws Throwable
    {
        // Given
        RecordingCallback<StatementMetadata,?> runResponse = new RecordingCallback<>();
        RecordingCallback<RecordStream,Object> pullResponse = new RecordingCallback<>();
        Session session = env.newSession( "<test>" );
        session.init( "TestClient", emptyMap(), -1, null );

        // When
        session.run( "ROLLBACK", emptyMap(), runResponse );
        session.pullAll( pullResponse );

        // Then
        assertThat( runResponse.next(), SessionMatchers
                .failedWith( "rollback cannot be done when there is no open transaction in the session." ) );
        assertThat( pullResponse.next(), SessionMatchers.ignored() );
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
                try ( Session session = env.newSession( "<write>" ) )
                {
                    session.init( "TestClient", emptyMap(), -1, null );
                    latch.await();
                    session.run( "MATCH (n:A) SET n.prop = 'two'", emptyMap(), Session.Callbacks.noop() );
                    session.pullAll( Session.Callbacks.noop() );
                }
            }
        };
        thread.start();

        long dbVersionAfterWrite = dbVersion + 1;
        try ( Session session = env.newSession( "<read>" ) )
        {
            session.init( "TestClient", emptyMap(), dbVersionAfterWrite, null );
            latch.release();
            session.run( "MATCH (n:A) RETURN n.prop", emptyMap(), Session.Callbacks.noop() );
            RecordingCallback<RecordStream,Object> pullResponse = new RecordingCallback<>();
            session.pullAll( pullResponse );

            Record[] records = ((RecordingCallback.Result) pullResponse.next()).records();

            assertEquals( 1, records.length );
            assertThat( records[0], eqRecord( equalTo( "two" ) ) );
        }

        thread.join();
    }
}
