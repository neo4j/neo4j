/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.v4.runtime;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.util.concurrent.BinaryLatch;
import org.neo4j.values.storable.TextValue;

import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.bolt.testing.BoltMatchers.containsRecord;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.succeededWithMetadata;
import static org.neo4j.bolt.testing.BoltMatchers.succeededWithRecord;
import static org.neo4j.bolt.testing.BoltMatchers.succeededWithoutMetadata;
import static org.neo4j.bolt.testing.BoltMatchers.verifyKillsConnection;
import static org.neo4j.bolt.testing.BoltMatchers.wasIgnored;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.v4.messaging.BoltV4Messages.begin;
import static org.neo4j.bolt.v4.messaging.BoltV4Messages.commit;
import static org.neo4j.bolt.v4.messaging.BoltV4Messages.discardAll;
import static org.neo4j.bolt.v4.messaging.BoltV4Messages.pullAll;
import static org.neo4j.bolt.v4.messaging.BoltV4Messages.reset;
import static org.neo4j.bolt.v4.messaging.BoltV4Messages.rollback;
import static org.neo4j.bolt.v4.messaging.BoltV4Messages.run;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;


class TransactionIT extends BoltStateMachineV4StateTestBase
{
    private static final Pattern BOOKMARK_PATTERN = Pattern.compile( "\\b[0-9a-f]{8}\\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\\b[0-9a-f]{12}\\b:[0-9]+" );

    @Test
    void shouldHandleBeginCommit() throws Throwable
    {
        // Given
        var recorder = new BoltResponseRecorder();
        var machine = newStateMachineAfterAuth();

        // When
        machine.process( begin(), recorder );

        machine.process( run( "CREATE (n:InTx)" ), recorder );
        machine.process( discardAll(), nullResponseHandler() );

        machine.process( commit(), recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    void shouldHandleBeginRollback() throws Throwable
    {
        // Given
        var recorder = new BoltResponseRecorder();
        var machine = newStateMachineAfterAuth();

        // When
        machine.process( begin(), recorder );

        machine.process( run( "CREATE (n:InTx)" ), recorder );
        machine.process( discardAll(), nullResponseHandler() );
        machine.process( rollback(), recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    void shouldFailWhenOutOfOrderRollbackInAutoCommitMode() throws Throwable
    {
        // Given
        var machine = newStateMachineAfterAuth();

        // When & Then
        assertThrows( BoltProtocolBreachFatality.class, () -> machine.process( rollback(), nullResponseHandler() ) );
    }

    @Test
    void shouldFailWhenOutOfOrderCommitInAutoCommitMode() throws Throwable
    {
        // Given
        var machine = newStateMachineAfterAuth();

        // When & Then
        assertThrows( BoltProtocolBreachFatality.class, () -> machine.process( commit(), nullResponseHandler() ) );
    }

    @Test
    void shouldReceiveBookmarkOnCommit() throws Throwable
    {
        // Given
        var recorder = new BoltResponseRecorder();
        var machine = newStateMachineAfterAuth();

        // When
        machine.process( begin(), nullResponseHandler() );

        machine.process( run( "CREATE (a:Person)" ), nullResponseHandler() );
        machine.process( discardAll(), nullResponseHandler() );

        machine.process( commit(), recorder );

        // Then
        assertThat( recorder.nextResponse(), succeededWithMetadata( "bookmark", BOOKMARK_PATTERN ) );
    }

    @Test
    void shouldNotReceiveBookmarkOnRollback() throws Throwable
    {
        // Given
        var recorder = new BoltResponseRecorder();
        var machine = newStateMachineAfterAuth();

        // When
        machine.process( begin(), nullResponseHandler() );

        machine.process( run( "CREATE (a:Person)" ), nullResponseHandler() );
        machine.process( discardAll(), nullResponseHandler() );

        machine.process( rollback(), recorder );

        // Then
        assertThat( recorder.nextResponse(), succeededWithoutMetadata( "bookmark" ) );
    }

    @Test
    void shouldReadYourOwnWrites() throws Exception
    {
        var latch = new BinaryLatch();

        String bookmarkPrefix = null;
        try ( var machine = newStateMachineAfterAuth() )
        {
            var recorder = new BoltResponseRecorder();
            machine.process( run( "CREATE (n:A {prop:'one'})" ), nullResponseHandler() );
            machine.process( pullAll(), recorder );

            var bookmark = ((TextValue) recorder.nextResponse().metadata( "bookmark" )).stringValue();
            bookmarkPrefix = bookmark.split( ":" )[0];
        }

        var dbVersion = env.lastClosedTxId();
        var thread = new Thread( () -> {
            try ( BoltStateMachine machine = newStateMachineAfterAuth() )
            {
                latch.await();
                var recorder = new BoltResponseRecorder();
                machine.process( run( "MATCH (n:A) SET n.prop = 'two'", EMPTY_MAP ), nullResponseHandler() );
                machine.process( pullAll(), recorder );
            }
            catch ( Throwable connectionFatality )
            {
                throw new RuntimeException( connectionFatality );
            }
        } );
        thread.start();

        var dbVersionAfterWrite = dbVersion + 1;
        try ( var machine = newStateMachineAfterAuth() )
        {
            var recorder = new BoltResponseRecorder();
            latch.release();
            var bookmark = bookmarkPrefix + ":" + dbVersionAfterWrite;

            machine.process( begin( env.databaseIdRepository(), asMapValue( singletonMap( "bookmarks", List.of( bookmark ) ) ) ), recorder );
            machine.process( run( "MATCH (n:A) RETURN n.prop" ), recorder );
            machine.process( pullAll(), recorder );
            machine.process( commit(), recorder );

            assertThat( recorder.nextResponse(), succeeded() );
            assertThat( recorder.nextResponse(), succeeded() );

            assertThat( recorder.nextResponse(), succeededWithRecord( "two" ) );
            assertThat( recorder.nextResponse(), succeededWithMetadata( "bookmark", BOOKMARK_PATTERN ) );
        }

        thread.join();
    }

    @Test
    void shouldAllowNewRunAfterRunFailure() throws Throwable
    {
        // Given
        var machine = newStateMachineAfterAuth();
        var recorder = new BoltResponseRecorder();

        // When
        machine.process( run( "INVALID QUERY" ), recorder );
        machine.process( pullAll(), recorder );
        resetReceived( machine, recorder );
        machine.process( run( "RETURN 2", EMPTY_MAP ), recorder );
        machine.process( pullAll(), recorder );

        // Then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Statement.SyntaxError ) );
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeededWithRecord( 2L ) );
        assertEquals( 0, recorder.responseCount() );
    }

    @Test
    void shouldAllowNewRunAfterStreamingFailure() throws Throwable
    {
        // Given
        var machine = newStateMachineAfterAuth();
        var recorder = new BoltResponseRecorder();

        // When
        machine.process( run( "UNWIND [1, 0] AS x RETURN 1 / x" ), recorder );
        machine.process( pullAll(), recorder );
        resetReceived( machine, recorder );
        machine.process( run( "RETURN 2" ), recorder );
        machine.process( pullAll(), recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), allOf( containsRecord( 1L ), failedWithStatus( Status.Statement.ArithmeticError ) ) );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeededWithRecord( 2L ) );
        assertEquals( 0, recorder.responseCount() );
    }
    @Test
    void shouldNotAllowNewRunAfterRunFailure() throws Throwable
    {
        // Given
        var machine = newStateMachineAfterAuth();
        var recorder = new BoltResponseRecorder();

        // When
        machine.process( run( "INVALID QUERY" ), nullResponseHandler() );
        machine.process( pullAll(), nullResponseHandler() );

        // If I do not ack failure, then I shall not be able to do anything
        machine.process( run(), recorder );
        machine.process( pullAll(), recorder );

        // Then
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertEquals( 0, recorder.responseCount() );
    }

    @Test
    void shouldNotAllowNewRunAfterStreamingFailure() throws Throwable
    {
        // Given
        var machine = newStateMachineAfterAuth();
        var recorder = new BoltResponseRecorder();

        // When
        machine.process( run( "UNWIND [1, 0] AS x RETURN 1 / x" ), recorder );
        machine.process( pullAll(), recorder );

        // If I do not ack failure, then I shall not be able to do anything
        machine.process( run(), recorder );
        machine.process( pullAll(), recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Statement.ArithmeticError ) );
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertEquals( 0, recorder.responseCount() );
    }

    @Test
    void shouldNotAllowNewTransactionAfterProtocolFailure() throws Throwable
    {
        // You cannot recover from Protocol error.
        // Given
        var machine = newStateMachineAfterAuth();

        // When
        verifyKillsConnection( () -> machine.process( commit(), nullResponseHandler() ) );
        assertFalse( machine.hasOpenStatement() );
        assertNull( machine.state() );
    }

    private static void resetReceived( BoltStateMachine machine, BoltResponseRecorder recorder ) throws BoltConnectionFatality
    {
        machine.interrupt();
        machine.process( reset(), recorder );
    }
}
