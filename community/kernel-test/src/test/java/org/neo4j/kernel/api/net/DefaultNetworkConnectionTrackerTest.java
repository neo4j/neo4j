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
package org.neo4j.kernel.api.net;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

class DefaultNetworkConnectionTrackerTest {
    private final TrackedNetworkConnection connection1 = connectionMock("1");
    private final TrackedNetworkConnection connection2 = connectionMock("2");
    private final TrackedNetworkConnection connection3 = connectionMock("3");

    private final NetworkConnectionTracker tracker = new DefaultNetworkConnectionTracker();

    @Test
    void shouldCreateIds() {
        assertEquals("bolt-0", tracker.newConnectionId("bolt"));
        assertEquals("bolt-1", tracker.newConnectionId("bolt"));
        assertEquals("bolt-2", tracker.newConnectionId("bolt"));

        assertEquals("http-3", tracker.newConnectionId("http"));
        assertEquals("http-4", tracker.newConnectionId("http"));

        assertEquals("https-5", tracker.newConnectionId("https"));
        assertEquals("https-6", tracker.newConnectionId("https"));
        assertEquals("https-7", tracker.newConnectionId("https"));
    }

    @Test
    void shouldAllowAddingOfConnections() {
        tracker.add(connection1);
        tracker.add(connection2);
        tracker.add(connection3);

        assertThat(tracker.activeConnections()).contains(connection1, connection2, connection3);
    }

    @Test
    void shouldFailWhenAddingConnectionWithSameId() {
        tracker.add(connection1);

        assertThrows(IllegalArgumentException.class, () -> tracker.add(connection1));
    }

    @Test
    void shouldRemoveConnections() {
        tracker.add(connection1);
        tracker.add(connection2);
        tracker.add(connection3);

        tracker.remove(connection2);
        assertThat(tracker.activeConnections()).contains(connection1, connection3);

        tracker.remove(connection1);
        assertThat(tracker.activeConnections()).contains(connection3);
    }

    @Test
    void shouldDoNothingWhenRemovingUnknownConnection() {
        tracker.add(connection1);
        tracker.add(connection3);
        assertThat(tracker.activeConnections()).contains(connection1, connection3);

        tracker.remove(connection2);

        assertThat(tracker.activeConnections()).contains(connection1, connection3);
    }

    @Test
    void shouldGetKnownConnectionById() {
        tracker.add(connection1);
        tracker.add(connection2);

        assertEquals(connection1, tracker.get(connection1.id()));
        assertEquals(connection2, tracker.get(connection2.id()));
    }

    @Test
    void shouldReturnNullForUnknownId() {
        tracker.add(connection1);

        assertNotNull(tracker.get(connection1.id()));
        assertNull(tracker.get(connection2.id()));
        assertNull(tracker.get(connection3.id()));
    }

    @Test
    void shouldListActiveConnectionsWhenEmpty() {
        assertThat(tracker.activeConnections()).isEmpty();
    }

    private static TrackedNetworkConnection connectionMock(String id) {
        TrackedNetworkConnection connection = mock(TrackedNetworkConnection.class);
        when(connection.id()).thenReturn(id);
        return connection;
    }
}
