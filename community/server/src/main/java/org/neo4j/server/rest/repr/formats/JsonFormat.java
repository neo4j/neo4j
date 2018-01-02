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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.DefaultFormat;
import org.neo4j.server.rest.repr.ListWriter;
import org.neo4j.server.rest.repr.MappingWriter;
import org.neo4j.server.rest.repr.RepresentationFormat;

import static org.neo4j.server.rest.domain.JsonHelper.assertSupportedPropertyValue;
import static org.neo4j.server.rest.domain.JsonHelper.readJson;

public class JsonFormat extends RepresentationFormat
{
    public JsonFormat()
    {
        super( MediaType.APPLICATION_JSON_TYPE );
    }

    @Override
    protected ListWriter serializeList( String type )
    {
        return new ListWrappingWriter( new ArrayList<Object>() );
    }

    @Override
    protected String complete( ListWriter serializer )
    {
        return JsonHelper.createJsonFrom( ( (ListWrappingWriter) serializer ).data );
    }

    @Override
    protected MappingWriter serializeMapping( String type )
    {
        return new MapWrappingWriter( new LinkedHashMap<String, Object>() );
    }

    @Override
    protected String complete( MappingWriter serializer )
    {
        return JsonHelper.createJsonFrom( ( (MapWrappingWriter) serializer ).data );
    }

    @Override
    protected String serializeValue( String type, Object value )
    {
        return JsonHelper.createJsonFrom( value );
    }

    private boolean empty( String input )
    {
        return input == null || "".equals( input.trim() );
    }

    @Override
    public Map<String, Object> readMap( String input, String... requiredKeys ) throws BadInputException
    {
        if ( empty( input ) ) return DefaultFormat.validateKeys( Collections.<String,Object>emptyMap(), requiredKeys );
        try
        {
            return DefaultFormat.validateKeys( JsonHelper.jsonToMap( stripByteOrderMark( input ) ), requiredKeys );
        }
        catch ( Exception ex )
        {
            throw new BadInputException( ex );
        }
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public List<Object> readList( String input ) throws BadInputException
    {
        try
        {
            return (List<Object>) JsonHelper.readJson( input );
        }
        catch ( ClassCastException ex )
        {
            throw new BadInputException( ex );
        }
        catch ( JsonParseException ex )
        {
            throw new BadInputException( ex );
        }
    }

    @Override
    public Object readValue( String input ) throws BadInputException
    {
        if ( empty( input ) ) return Collections.emptyMap();
        try
        {
            return assertSupportedPropertyValue( readJson( stripByteOrderMark( input ) ) );
        }
        catch ( JsonParseException ex )
        {
            throw new BadInputException( ex );
        }
    }

    @Override
    public URI readUri( String input ) throws BadInputException
    {
        try
        {
            return new URI( readValue( input ).toString() );
        }
        catch ( URISyntaxException e )
        {
            throw new BadInputException( e );
        }
    }

    private String stripByteOrderMark( String string )
    {
        if ( string != null && string.length() > 0 && string.charAt( 0 ) == 0xfeff )
        {
            return string.substring( 1 );
        }
        return string;
    }
}
