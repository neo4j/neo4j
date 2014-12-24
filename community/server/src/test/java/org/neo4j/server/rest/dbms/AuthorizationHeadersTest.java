/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.rest.dbms;

import java.nio.charset.Charset;

import com.sun.jersey.core.util.Base64;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.neo4j.server.rest.dbms.AuthorizationHeaders.extractToken;

public class AuthorizationHeadersTest
{
    @Test
    public void shouldParseHappyPath() throws Exception
    {
        // Given
        String token = "12345";
        String header = "Basic realm=\"Neo4j\" " + base64(":" + token);

        // When
        String parsed = extractToken( header );

        // Then
        assertEquals(token, parsed);
    }

    @Test
    public void shouldHandleSadPaths() throws Exception
    {
        // When & then
        assertEquals("",  extractToken( "" ));
        assertEquals("",  extractToken( null ));
        assertEquals("",  extractToken( "Basic" ));
        assertEquals("",  extractToken( "Basic realm=\"Neo4j\" not valid value" ));
        assertEquals("",  extractToken( "Basic realm=\"Neo4j\" " + base64("") ));
        assertEquals("",  extractToken( "Basic realm=\"Neo4j\" " + base64(":") ));
    }

    private String base64(String value)
    {
        return new String( Base64.encode( value ), Charset
                .forName( "UTF-8" ));
    }
}
