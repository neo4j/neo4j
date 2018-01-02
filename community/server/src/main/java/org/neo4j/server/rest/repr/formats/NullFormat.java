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
package org.neo4j.server.rest.repr.formats;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.InvalidArgumentsException;
import org.neo4j.server.rest.repr.ListWriter;
import org.neo4j.server.rest.repr.MappingWriter;
import org.neo4j.server.rest.repr.MediaTypeNotSupportedException;
import org.neo4j.server.rest.repr.RepresentationFormat;

public class NullFormat extends RepresentationFormat
{
    private final Collection<MediaType> supported;
    private final MediaType[] requested;

    public NullFormat( Collection<MediaType> supported, MediaType... requested )
    {
        super( null );
        this.supported = supported;
        this.requested = requested;
    }

    @Override
    public Object readValue( String input )
    {
        if ( empty( input ) )
        {
            return null;
        }
        throw new MediaTypeNotSupportedException( Response.Status.UNSUPPORTED_MEDIA_TYPE, supported, requested );
    }

    @Override
    public URI readUri( String input )
    {
        if ( empty( input ) )
        {
            return null;
        }
        throw new MediaTypeNotSupportedException( Response.Status.UNSUPPORTED_MEDIA_TYPE, supported, requested );
    }

    @Override
    public Map<String, Object> readMap( String input, String... requiredKeys ) throws BadInputException
    {
        if ( empty( input ) )
        {
            if ( requiredKeys.length != 0 )
            {
                String missingKeys = Arrays.toString( requiredKeys );
                throw new InvalidArgumentsException( "Missing required keys: " + missingKeys );
            }
            return Collections.emptyMap();
        }
        throw new MediaTypeNotSupportedException( Response.Status.UNSUPPORTED_MEDIA_TYPE, supported, requested );
    }

    @Override
    public List<Object> readList( String input )
    {
        if ( empty( input ) )
        {
            return Collections.emptyList();
        }
        throw new MediaTypeNotSupportedException( Response.Status.UNSUPPORTED_MEDIA_TYPE, supported, requested );
    }

    private boolean empty( String input )
    {
        return input == null || "".equals( input.trim() );
    }

    @Override
    protected String serializeValue( final String type, final Object value )
    {
        throw new MediaTypeNotSupportedException( Response.Status.NOT_ACCEPTABLE, supported, requested );
    }

    @Override
    protected ListWriter serializeList( final String type )
    {
        throw new MediaTypeNotSupportedException( Response.Status.NOT_ACCEPTABLE, supported, requested );
    }

    @Override
    protected MappingWriter serializeMapping( final String type )
    {
        throw new MediaTypeNotSupportedException( Response.Status.NOT_ACCEPTABLE, supported, requested );
    }

    @Override
    protected String complete( final ListWriter serializer )
    {
        throw new MediaTypeNotSupportedException( Response.Status.NOT_ACCEPTABLE, supported, requested );
    }

    @Override
    protected String complete( final MappingWriter serializer )
    {
        throw new MediaTypeNotSupportedException( Response.Status.NOT_ACCEPTABLE, supported, requested );
    }
}
