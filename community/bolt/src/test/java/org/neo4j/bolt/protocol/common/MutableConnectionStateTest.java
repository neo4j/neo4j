/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.values.storable.Values.stringValue;

import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.connector.connection.MutableConnectionState;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.Error;

class MutableConnectionStateTest {
    private final MutableConnectionState state = new MutableConnectionState();

    private final ResponseHandler responseHandler = mock(ResponseHandler.class);

    @Test
    void shouldHandleOnMetadataWithoutResponseHandler() {
        state.setResponseHandler(null);

        state.onMetadata("key", stringValue("value"));

        assertNull(state.getPendingError());
        assertFalse(state.hasPendingIgnore());
    }

    @Test
    void shouldHandleOnMetadataWitResponseHandler() {
        state.setResponseHandler(responseHandler);

        state.onMetadata("key", stringValue("value"));

        verify(responseHandler).onMetadata("key", stringValue("value"));
    }

    @Test
    void shouldHandleMarkIgnored() {
        state.setResponseHandler(null);

        state.markIgnored();

        assertNull(state.getPendingError());
        assertTrue(state.hasPendingIgnore());
    }

    @Test
    void shouldHandleMarkFailed() {
        state.setResponseHandler(null);

        Error error = Error.from(new RuntimeException());
        state.markFailed(error);

        assertEquals(error, state.getPendingError());
        assertFalse(state.hasPendingIgnore());
    }

    @Test
    void shouldResetPendingFailureAndIgnored() {
        state.setResponseHandler(null);

        Error error = Error.from(new RuntimeException());
        state.markIgnored();
        state.markFailed(error);

        assertEquals(error, state.getPendingError());
        assertTrue(state.hasPendingIgnore());

        state.resetPendingFailedAndIgnored();

        assertNull(state.getPendingError());
        assertFalse(state.hasPendingIgnore());
    }

    @Test
    void shouldNotProcessMessageWithPendingError() {
        state.setResponseHandler(null);

        state.markFailed(Error.from(new RuntimeException()));

        assertFalse(state.canProcessMessage());
    }

    @Test
    void shouldNotProcessMessageWithPendingIgnore() {
        state.setResponseHandler(null);

        state.markIgnored();

        assertFalse(state.canProcessMessage());
    }
}
