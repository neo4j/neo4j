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
package org.neo4j.helpers;

import java.net.URI;

/**
 * Functions for working with URIs
 */
public final class Uris
{
    /**
     * Extract a named parameter from the query of a URI. If a parameter is set but no value defined,
     * then "true" is returned
     *
     * @param name of the parameter
     * @return value of named parameter or null if missing
     */
    public static Function<URI, String> parameter( final String name )
    {
        return new Function<URI, String>()
        {
            @Override
            public String apply( URI uri )
            {
                if ( uri == null )
                {
                    return null;
                }

                String query = uri.getQuery();
                if (query != null)
                {
                    for ( String param : query.split( "&" ) )
                    {
                        String[] keyValue = param.split( "=" );

                        if ( keyValue[0].equalsIgnoreCase( name ) )
                        {
                            if ( keyValue.length == 2 )
                            {
                                return keyValue[1];
                            }
                            else
                            {
                                return "true";
                            }
                        }
                    }
                }
                return null;
            }
        };
    }

    private Uris()
    {
    }
}
