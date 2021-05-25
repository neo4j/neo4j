/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.server.web;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;

import org.neo4j.helpers.AdvertisedSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimpleUriBuilderTest
{

    @Test
    void shouldIncludeScheme()
    {
        SimpleUriBuilder builder = new SimpleUriBuilder();

        URI uri1 = builder.buildURI( new AdvertisedSocketAddress( "neo4j.example.org", 443 ), false );
        URI uri2 = builder.buildURI( new AdvertisedSocketAddress( "neo4j.example.org", 80 ), true );

        assertEquals( "http", uri1.getScheme() );
        assertEquals( "https", uri2.getScheme() );
    }

    @Test
    void shouldIncludePortWhenNecessary()
    {
        SimpleUriBuilder builder = new SimpleUriBuilder();

        URI uri1 = builder.buildURI( new AdvertisedSocketAddress( "neo4j.example.org", 80 ), false );
        URI uri2 = builder.buildURI( new AdvertisedSocketAddress( "neo4j.example.org", 443 ), true );
        URI uri3 = builder.buildURI( new AdvertisedSocketAddress( "neo4j.example.org", 443 ), false );
        URI uri4 = builder.buildURI( new AdvertisedSocketAddress( "neo4j.example.org", 80 ), true );
        URI uri5 = builder.buildURI( new AdvertisedSocketAddress( "neo4j.example.org", 7690 ), false );

        assertEquals( -1, uri1.getPort() );
        assertEquals( -1, uri2.getPort() );
        assertEquals( 443, uri3.getPort() );
        assertEquals( 80, uri4.getPort() );
        assertEquals( 7690, uri5.getPort() );
    }

    @Test
    void shouldRejectInvalidHostnames()
    {
        SimpleUriBuilder builder = new SimpleUriBuilder();

        RuntimeException ex = assertThrows(
                RuntimeException.class,
                () -> builder.buildURI( new AdvertisedSocketAddress( "$core_1", 443 ), false ) );

        Throwable cause = ex.getCause();
        assertNotNull( cause );
        assertTrue( cause instanceof URISyntaxException );
    }
}
