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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class decorates another RepresentationFormat (called inner here), and
 * tries to use inner to parse stuff. If it fails, it will throw an appropriate
 * exception, and not just blow up with an exception that leads to HTTP STATUS
 * 500
 */
public class DefaultFormat extends RepresentationFormat
{
    private final RepresentationFormat inner;
    private final Collection<MediaType> supported;
    private final MediaType[] requested;

    public DefaultFormat( RepresentationFormat inner, Collection<MediaType> supported, MediaType... requested )
    {
        super( MediaType.APPLICATION_JSON_TYPE );

        this.inner = inner;
        this.supported = supported;
        this.requested = requested;
    }

    @Override
    protected String serializeValue( String type, Object value )
    {
        return inner.serializeValue( type, value );
    }

    @Override
    protected ListWriter serializeList( String type )
    {
        return inner.serializeList( type );
    }

    @Override
    protected MappingWriter serializeMapping( String type )
    {
        return inner.serializeMapping( type );
    }

    @Override
    protected String complete( ListWriter serializer )
    {
        return inner.complete( serializer );
    }

    @Override
    protected String complete( MappingWriter serializer )
    {
        return inner.complete( serializer );
    }

    @Override
    public Object readValue( String input )
    {
        try
        {
            return inner.readValue( input );
        }
        catch ( BadInputException e )
        {
            throw newMediaTypeNotSupportedException();
        }
    }

    private MediaTypeNotSupportedException newMediaTypeNotSupportedException()
    {
        return new MediaTypeNotSupportedException( Response.Status.UNSUPPORTED_MEDIA_TYPE, supported, requested );
    }

    @Override
    public Map<String, Object> readMap( String input, String... requiredKeys ) throws BadInputException
    {
        Map<String, Object> result;
        try
        {
            result = inner.readMap( input );
        }
        catch ( BadInputException e )
        {
            throw newMediaTypeNotSupportedException();
        }
        return validateKeys( result, requiredKeys );
    }

    @Override
    public List<Object> readList( String input ) throws BadInputException
    {
        try
        {
            return inner.readList( input );
        }
        catch ( BadInputException e )
        {
            throw newMediaTypeNotSupportedException();
        }
    }

    @Override
    public URI readUri( String input ) throws BadInputException
    {
        try
        {
            return inner.readUri( input );
        }
        catch ( BadInputException e )
        {
            throw newMediaTypeNotSupportedException();
        }
    }

    public static <T> Map<String, T> validateKeys( Map<String, T> map, String... requiredKeys ) throws BadInputException
    {
        Set<String> missing = null;
        for ( String key : requiredKeys )
        {
            if ( !map.containsKey( key ) ) ( missing == null ? ( missing = new HashSet<String>() ) : missing ).add( key );
        }
        if ( missing != null )
        {
            if ( missing.size() == 1 )
            {
                throw new InvalidArgumentsException( "Missing required key: \"" + missing.iterator().next() + "\"" );
            }
            else
            {
                throw new InvalidArgumentsException( "Missing required keys: " + missing );
            }
        }
        return map;
    }
}
