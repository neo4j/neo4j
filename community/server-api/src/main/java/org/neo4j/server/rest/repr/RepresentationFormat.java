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

import javax.ws.rs.core.MediaType;

import org.neo4j.annotations.service.Service;

/**
 * Implementations of this class must be stateless. Implementations of this
 * class must have a public no arguments constructor.
 */
@Service
public abstract class RepresentationFormat implements InputFormat
{
    final MediaType mediaType;

    public RepresentationFormat( MediaType mediaType )
    {
        this.mediaType = mediaType;
    }

    @Override
    public String toString()
    {
        return String.format( "%s[%s]", getClass().getSimpleName(), mediaType );
    }

    String serializeValue( RepresentationType type, Object value )
    {
        return serializeValue( type.valueName, value );
    }

    protected abstract String serializeValue( String type, Object value );

    ListWriter serializeList( RepresentationType type )
    {
        if ( type.listName == null )
        {
            throw new IllegalStateException( "Invalid list type: " + type );
        }
        return serializeList( type.listName );
    }

    protected abstract ListWriter serializeList( String type );

    MappingWriter serializeMapping( RepresentationType type )
    {
        return serializeMapping( type.valueName );
    }

    protected abstract MappingWriter serializeMapping( String type );

    /**
     * Will be invoked (when serialization is done) with the result retrieved
     * from invoking {@link #serializeList(String)}, it is therefore safe for
     * this method to convert the {@link ListWriter} argument to the
     * implementation class returned by {@link #serializeList(String)}.
     */
    protected abstract String complete( ListWriter serializer ) ;

    /**
     * Will be invoked (when serialization is done) with the result retrieved
     * from invoking {@link #serializeMapping(String)}, it is therefore safe for
     * this method to convert the {@link MappingWriter} argument to the
     * implementation class returned by {@link #serializeMapping(String)}.
     */
    protected abstract String complete( MappingWriter serializer ) ;

    public void complete()
    {
    }
}
