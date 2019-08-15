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
package org.neo4j.bolt.runtime;

import org.junit.jupiter.api.Test;

import org.neo4j.bolt.runtime.statemachine.MutableConnectionState;
import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.graphdb.TransactionTerminatedException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.v4.messaging.AbstractStreamingMessage.STREAM_LIMIT_UNLIMITED;
import static org.neo4j.kernel.api.exceptions.Status.Request.Invalid;
import static org.neo4j.values.storable.Values.stringValue;

class MutableConnectionStateTest
{
    private final MutableConnectionState state = new MutableConnectionState();

    private final BoltResult result = mock( BoltResult.class );
    private final BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );

    @Test
    void shouldHandleOnPullRecordsWithoutResponseHandler() throws Throwable
    {
        state.setResponseHandler( null );

        state.onPullRecords( result, STREAM_LIMIT_UNLIMITED );

        assertNull( state.getPendingError() );
        assertFalse( state.hasPendingIgnore() );
    }

    @Test
    void shouldHandleOnPullRecordsWithResponseHandler() throws Throwable
    {
        state.setResponseHandler( responseHandler );

        state.onPullRecords( result, STREAM_LIMIT_UNLIMITED );

        verify( responseHandler ).onPullRecords( result, STREAM_LIMIT_UNLIMITED );
    }

    @Test
    void shouldHandleOnDiscardRecordsWithoutResponseHandler() throws Throwable
    {
        state.setResponseHandler( null );

        state.onDiscardRecords( result, STREAM_LIMIT_UNLIMITED );

        assertNull( state.getPendingError() );
        assertFalse( state.hasPendingIgnore() );
    }

    @Test
    void shouldHandleOnDiscardRecordsWithResponseHandler() throws Throwable
    {
        state.setResponseHandler( responseHandler );

        state.onDiscardRecords( result, STREAM_LIMIT_UNLIMITED );

        verify( responseHandler ).onDiscardRecords( result, STREAM_LIMIT_UNLIMITED );
    }

    @Test
    void shouldHandleOnMetadataWithoutResponseHandler()
    {
        state.setResponseHandler( null );

        state.onMetadata( "key", stringValue( "value" ) );

        assertNull( state.getPendingError() );
        assertFalse( state.hasPendingIgnore() );
    }

    @Test
    void shouldHandleOnMetadataWitResponseHandler()
    {
        state.setResponseHandler( responseHandler );

        state.onMetadata( "key", stringValue( "value" ) );

        verify( responseHandler ).onMetadata( "key", stringValue( "value" ) );
    }

    @Test
    void shouldHandleMarkIgnoredWithoutResponseHandler()
    {
        state.setResponseHandler( null );

        state.markIgnored();

        assertNull( state.getPendingError() );
        assertTrue( state.hasPendingIgnore() );
    }

    @Test
    void shouldHandleMarkIgnoredWitResponseHandler()
    {
        state.setResponseHandler( responseHandler );

        state.markIgnored();

        verify( responseHandler ).markIgnored();
        assertNull( state.getPendingError() );
        assertFalse( state.hasPendingIgnore() );
    }

    @Test
    void shouldHandleMarkFailedWithoutResponseHandler()
    {
        state.setResponseHandler( null );

        Neo4jError error = Neo4jError.from( new RuntimeException() );
        state.markFailed( error );

        assertEquals( error, state.getPendingError() );
        assertFalse( state.hasPendingIgnore() );
    }

    @Test
    void shouldHandleMarkFailedWitResponseHandler()
    {
        state.setResponseHandler( responseHandler );

        Neo4jError error = Neo4jError.from( new RuntimeException() );
        state.markFailed( error );

        verify( responseHandler ).markFailed( error );
        assertNull( state.getPendingError() );
        assertFalse( state.hasPendingIgnore() );
    }

    @Test
    void shouldHandleOnFinishWithoutResponseHandler()
    {
        state.setResponseHandler( null );

        state.onFinish();

        assertNull( state.getPendingError() );
        assertFalse( state.hasPendingIgnore() );
    }

    @Test
    void shouldHandleOnFinishWitResponseHandler()
    {
        state.setResponseHandler( responseHandler );

        state.onFinish();

        verify( responseHandler ).onFinish();
    }

    @Test
    void shouldResetPendingFailureAndIgnored()
    {
        state.setResponseHandler( null );

        Neo4jError error = Neo4jError.from( new RuntimeException() );
        state.markIgnored();
        state.markFailed( error );

        assertEquals( error, state.getPendingError() );
        assertTrue( state.hasPendingIgnore() );

        state.resetPendingFailedAndIgnored();

        assertNull( state.getPendingError() );
        assertFalse( state.hasPendingIgnore() );
    }

    @Test
    void shouldNotProcessMessageWhenClosed()
    {
        state.setResponseHandler( null );

        state.markClosed();

        assertFalse( state.canProcessMessage() );
    }

    @Test
    void shouldNotProcessMessageWithPendingError()
    {
        state.setResponseHandler( null );

        state.markFailed( Neo4jError.from( new RuntimeException() ) );

        assertFalse( state.canProcessMessage() );
    }

    @Test
    void shouldNotProcessMessageWithPendingIgnore()
    {
        state.setResponseHandler( null );

        state.markIgnored();

        assertFalse( state.canProcessMessage() );
    }

    @Test
    void shouldInterrupt()
    {
        assertFalse( state.isInterrupted() );

        assertEquals( 1, state.incrementInterruptCounter() );
        assertTrue( state.isInterrupted() );

        assertEquals( 2, state.incrementInterruptCounter() );
        assertTrue( state.isInterrupted() );

        assertEquals( 3, state.incrementInterruptCounter() );
        assertTrue( state.isInterrupted() );

        assertEquals( 2, state.decrementInterruptCounter() );
        assertTrue( state.isInterrupted() );

        assertEquals( 1, state.decrementInterruptCounter() );
        assertTrue( state.isInterrupted() );

        assertEquals( 0, state.decrementInterruptCounter() );
        assertFalse( state.isInterrupted() );
    }

    @Test
    void shouldSetAndGetStatementProcessor() throws Throwable
    {
        StatementProcessor processor = mock( StatementProcessor.class );
        state.setStatementProcessor( processor );

        assertThat( state.getStatementProcessor(), equalTo( processor ) );
    }

    @Test
    void shouldThrowErrorWhenSetStatementProcessorAndThereIsPendingTerminationError() throws Throwable
    {
        state.setPendingTerminationNotice( Invalid );
        StatementProcessor processor = mock( StatementProcessor.class );
        TransactionTerminatedException error =
                assertThrows( TransactionTerminatedException.class, () -> state.setStatementProcessor( processor ) );
        assertThat( error.status(), equalTo( Invalid ) );

        // The second set shall be fine
        state.setStatementProcessor( processor );
        assertThat( state.getStatementProcessor(), equalTo( processor ) );
    }

    @Test
    void shouldThrowErrorWhenGetStatementProcessorAndThereIsPendingTerminationError() throws Throwable
    {
        state.setPendingTerminationNotice( Invalid );
        TransactionTerminatedException error = assertThrows( TransactionTerminatedException.class, state::getStatementProcessor );
        assertThat( error.status(), equalTo( Invalid ) );

        // The second get shall be fine
        assertThat( state.getStatementProcessor(), equalTo( StatementProcessor.EMPTY ) );
    }

    @Test
    void shouldClearResetStatmentProcessorToEmpty() throws Throwable
    {
        // Given
        StatementProcessor processor = mock( StatementProcessor.class );
        state.setStatementProcessor( processor );
        assertThat( state.getStatementProcessor(), equalTo( processor ) );

        // When
        state.clearStatementProcessor();
        // Then
        assertThat( state.getStatementProcessor(), equalTo( StatementProcessor.EMPTY ) );
    }
}
