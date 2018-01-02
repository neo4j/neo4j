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
package org.neo4j.server.rest.repr;

import java.net.URI;

public class ListSerializer extends Serializer
{
    final ListWriter writer;

    ListSerializer( ListWriter writer, URI baseUri, ExtensionInjector extensions )
    {
        super( baseUri, extensions );
        this.writer = writer;
    }

    public void addUri( String path )
    {
        writer.writeValue( RepresentationType.URI, relativeUri( path ) );
    }

    public void addUriTemplate( String template )
    {
        writer.writeValue( RepresentationType.TEMPLATE, relativeTemplate( template ) );
    }

    public void addString( String value )
    {
        writer.writeString( value );
    }

    public void addMapping( MappingRepresentation value )
    {
        serialize( writer.newMapping( value.type ), value );
    }

    public void addList( ListRepresentation value )
    {
        serialize( writer.newList( value.type ), value );
    }

    public final void addNumber( Number value )
    {
        if ( value instanceof Double || value instanceof Float )
        {
            writer.writeFloatingPointNumber( RepresentationType.valueOf( value.getClass() ), value.doubleValue() );
        }
        else
        {
            checkThatItIsBuiltInType( value );
            writer.writeInteger( RepresentationType.valueOf( value.getClass() ), value.longValue() );
        }
    }
}
