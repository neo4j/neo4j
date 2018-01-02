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

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.DefaultFormat;
import org.neo4j.server.rest.repr.ListWriter;
import org.neo4j.server.rest.repr.MappingWriter;
import org.neo4j.server.rest.repr.RepresentationFormat;

public class UrlFormFormat extends RepresentationFormat
{
    public UrlFormFormat()
    {
        super( MediaType.APPLICATION_FORM_URLENCODED_TYPE );
    }

    @Override
    protected String serializeValue( final String type, final Object value )
    {
        throw new RuntimeException( "Not implemented!" );
    }

    @Override
    protected ListWriter serializeList( final String type )
    {
        throw new RuntimeException( "Not implemented!" );
    }

    @Override
    protected MappingWriter serializeMapping( final String type )
    {
        throw new RuntimeException( "Not implemented!" );
    }

    @Override
    protected String complete( final ListWriter serializer )
    {
        throw new RuntimeException( "Not implemented!" );
    }

    @Override
    protected String complete( final MappingWriter serializer )
    {
        throw new RuntimeException( "Not implemented!" );
    }

    @Override
    public Object readValue( final String input ) throws BadInputException
    {
        throw new RuntimeException( "Not implemented!" );
    }

    @Override
    public Map<String, Object> readMap( final String input, String... requiredKeys ) throws BadInputException
    {
        HashMap<String, Object> result = new HashMap<String, Object>();
        if ( input.isEmpty() )
        {
            return result;
        }

        for ( String pair : input.split( "\\&" ) )
        {
            String[] fields = pair.split( "=" );
            String key;
            String value;

            try
            {
                key = ensureThatKeyDoesNotHavePhPStyleParenthesesAtTheEnd( URLDecoder.decode( fields[0], "UTF-8" ) );
                value = URLDecoder.decode( fields[1], "UTF-8" );
            }
            catch ( UnsupportedEncodingException e )
            {
                throw new BadInputException( e );
            }

            Object old = result.get( key );
            if ( old == null )
            {
                result.put( key, value );
            }
            else
            {
                List<Object> list;
                if ( old instanceof List<?> )
                {
                    list = (List<Object>) old;
                }
                else
                {
                    list = new ArrayList<Object>();
                    result.put( key, list );
                    list.add( old );
                }
                list.add( value );
            }
        }

        return DefaultFormat.validateKeys( result, requiredKeys );
    }

    private String ensureThatKeyDoesNotHavePhPStyleParenthesesAtTheEnd( String key )
    {
        if ( key.endsWith( "[]" ) )
        {
            return key.substring( 0, key.length() - 2 );
        }
        return key;
    }

    @Override
    public List<Object> readList( final String input ) throws BadInputException
    {
        throw new RuntimeException( "Not implemented!" );
    }

    @Override
    public URI readUri( final String input ) throws BadInputException
    {
        throw new RuntimeException( "Not implemented!" );
    }
}
