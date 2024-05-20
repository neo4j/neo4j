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
package org.neo4j.bolt.protocol.common.connector.listener;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.listener.AuthenticationSecurityConnectionListener;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;

class AuthenticationTimeoutConnectorListenerTest {

    @Test
    void shouldRegisterAuthenticationTimeoutConnectionListenerOnConnectionCreated() {
        var connection = Mockito.mock(Connection.class, Mockito.RETURNS_MOCKS);
        var memoryTracker = Mockito.mock(MemoryTracker.class);

        Mockito.doReturn(memoryTracker).when(connection).memoryTracker();

        new AuthenticationTimeoutConnectorListener(Duration.ofSeconds(2), NullLogProvider.getInstance())
                .onConnectionCreated(connection);

        var inOrder = Mockito.inOrder(connection, memoryTracker);

        inOrder.verify(connection).memoryTracker();
        inOrder.verify(memoryTracker).allocateHeap(AuthenticationSecurityConnectionListener.SHALLOW_SIZE);
        inOrder.verify(connection)
                .registerListener(ArgumentMatchers.any(AuthenticationSecurityConnectionListener.class));
        inOrder.verifyNoMoreInteractions();
    }
}
