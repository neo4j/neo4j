/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.web;

import java.net.URI;
import java.net.URISyntaxException;

public class SimpleUriBuilder {

    public URI buildURI(String address, int port, boolean isSsl)
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "http" );
        
        if ( isSsl )
        {
            sb.append( "s" );

        }
        sb.append( "://" );

        sb.append( address );

        if ( port != 80 && port != 443)
        {
            sb.append( ":" );
            sb.append( port );
        }
        sb.append( "/" );

        try
        {
            return new URI( sb.toString() );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }

}
