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

import com.sun.jersey.core.util.Base64;

public class AuthorizationHeaders
{
    /**
     * Extract the encoded authorization token from a HTTP Authorization header value.
     */
    public static String extractToken(String authorizationHeader)
    {
        if(authorizationHeader == null)
        {
            return "";
        }

        String[] parts = authorizationHeader.trim().split( " " );
        String tokenSegment = parts[parts.length-1];

        if(tokenSegment.trim().length() == 0)
        {
            return "";
        }

        String decoded = Base64.base64Decode( tokenSegment );
        if(decoded.length() < 1)
        {
            return "";
        }

        String[] blankAndToken = decoded.split( ":" );
        if(blankAndToken.length != 2)
        {
            return "";
        }

        return blankAndToken[1];
    }
}
