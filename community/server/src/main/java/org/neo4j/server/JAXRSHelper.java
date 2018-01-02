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
package org.neo4j.server;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @deprecated This class is for internal use only and will be moved to an internal package in a future release.
 */
@Deprecated
public class JAXRSHelper
{
    public static List<String> listFrom( String... strings )
    {
        ArrayList<String> al = new ArrayList<String>();

        if ( strings != null )
        {
            al.addAll( Arrays.asList( strings ) );
        }

        return al;
    }

    public static URI generateUriFor( URI baseUri, String serviceName )
    {
        if ( serviceName.startsWith( "/" ) )
        {
            serviceName = serviceName.substring( 1 );
        }
        StringBuilder sb = new StringBuilder();
        try
        {
            String baseUriString = baseUri.toString();
            sb.append( baseUriString );
            if ( !baseUriString.endsWith( "/" ) )
            {
                sb.append( "/" );
            }
            sb.append( serviceName );

            return new URI( sb.toString() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
