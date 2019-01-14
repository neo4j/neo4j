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
package org.neo4j.server.rest.dbms;

import org.junit.Test;

import java.util.Base64;

import org.neo4j.test.server.HTTP;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.neo4j.server.rest.dbms.AuthorizationHeaders.decode;

public class AuthorizationHeadersTest
{
    @Test
    public void shouldParseHappyPath()
    {
        // Given
        String username = "jake";
        String password = "qwerty123456";
        String header = HTTP.basicAuthHeader( username, password );

        // When
        String[] parsed = decode( header );

        // Then
        assertEquals( username, parsed[0] );
        assertEquals( password, parsed[1] );
    }

    @Test
    public void shouldHandleSadPaths()
    {
        // When & then
        assertNull( decode( "" ) );
        assertNull( decode( "Basic" ) );
        assertNull( decode( "Basic not valid value" ) );
        assertNull( decode( "Basic " + Base64.getEncoder().encodeToString( "".getBytes() ) ) );
    }
}
