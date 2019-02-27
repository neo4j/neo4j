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
package org.neo4j.server.rest.repr;

import java.net.URI;

abstract class Serializer
{
    private final URI baseUri;

    Serializer( URI baseUri )
    {
        this.baseUri = baseUri;
    }

    final void serialize( MappingWriter mapping, MappingRepresentation value )
    {
        value.serialize( new MappingSerializer( mapping, baseUri ) );
        mapping.done();
    }

    final void serialize( ListWriter list, ListRepresentation value )
    {
        value.serialize( new ListSerializer( list, baseUri ) );
        list.done();
    }

    final String relativeUri( String path )
    {
        return joinBaseWithRelativePath(baseUri, path);
    }

    final String relativeTemplate( String path )
    {
        return joinBaseWithRelativePath( baseUri, path );
    }

    static String joinBaseWithRelativePath( URI baseUri, String path )
    {
        String base = baseUri.toString();
        final StringBuilder result = new StringBuilder( base.length() + path.length() + 1 ).append( base );
        if ( base.endsWith( "/" ) )
        {
            if ( path.startsWith( "/" ) )
            {
                return result.append(path.substring(1)).toString();
            }
        }
        else if ( !path.startsWith( "/" ) )
        {
            return result.append('/').append(path).toString();
        }
        return result.append(path).toString();
    }

    void checkThatItIsBuiltInType( Object value )
    {
        if ( !"java.lang".equals( value.getClass().getPackage().getName() ) )
        {
            throw new IllegalArgumentException( "Unsupported number type: " + value.getClass() );
        }
    }
}
