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
package org.neo4j.bolt.runtime;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.values.storable.Values.stringValue;

class MutableConnectionStateTest
{
    private final MutableConnectionState state = new MutableConnectionState();

    private final BoltResult result = mock( BoltResult.class );
    private final BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );

    @Test
    void shouldHandleOnRecordsWithoutResponseHandler() throws Exception
    {
        state.setResponseHandler( null );

        state.onRecords( result, true );

        assertNull( state.getPendingError() );
        assertFalse( state.hasPendingIgnore() );
    }

    @Test
    void shouldHandleOnRecordsWitResponseHandler() throws Exception
    {
        state.setResponseHandler( responseHandler );

        state.onRecords( result, true );

        verify( responseHandler ).onRecords( result, true );
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
}
