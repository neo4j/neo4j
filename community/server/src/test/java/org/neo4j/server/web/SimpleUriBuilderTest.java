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
package org.neo4j.server.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.helpers.SocketAddress;

class SimpleUriBuilderTest {

    @Test
    void shouldIncludeScheme() {
        var uri1 = SimpleUriBuilder.buildURI(new SocketAddress("neo4j.example.org", 443), false);
        var uri2 = SimpleUriBuilder.buildURI(new SocketAddress("neo4j.example.org", 80), true);

        assertEquals("http", uri1.getScheme());
        assertEquals("https", uri2.getScheme());
    }

    @Test
    void shouldIncludePortWhenNecessary() {
        var uri1 = SimpleUriBuilder.buildURI(new SocketAddress("neo4j.example.org", 80), false);
        var uri2 = SimpleUriBuilder.buildURI(new SocketAddress("neo4j.example.org", 443), true);
        var uri3 = SimpleUriBuilder.buildURI(new SocketAddress("neo4j.example.org", 443), false);
        var uri4 = SimpleUriBuilder.buildURI(new SocketAddress("neo4j.example.org", 80), true);
        var uri5 = SimpleUriBuilder.buildURI(new SocketAddress("neo4j.example.org", 7690), false);

        assertEquals(-1, uri1.getPort());
        assertEquals(-1, uri2.getPort());
        assertEquals(443, uri3.getPort());
        assertEquals(80, uri4.getPort());
        assertEquals(7690, uri5.getPort());
    }

    @Test
    void shouldRejectInvalidHostnames() {
        var ex = assertThrows(
                RuntimeException.class, () -> SimpleUriBuilder.buildURI(new SocketAddress("$core_1", 443), false));

        var cause = ex.getCause();
        assertNotNull(cause);
        assertTrue(cause instanceof URISyntaxException);
    }
}
