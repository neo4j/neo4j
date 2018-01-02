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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.neo4j.server.rest.repr.formats.ListWrappingWriter;
import org.neo4j.server.rest.repr.formats.MapWrappingWriter;

public class RepresentationTestAccess
{
    private static final URI BASE_URI = URI.create( "http://neo4j.org/" );

    public static Object serialize( Representation repr )
    {
        if ( repr instanceof ValueRepresentation )
        {
            return serialize( (ValueRepresentation) repr );
        }
        else if ( repr instanceof MappingRepresentation )
        {
            return serialize( (MappingRepresentation) repr );
        }
        else if ( repr instanceof ListRepresentation )
        {
            return serialize( (ListRepresentation) repr );
        }
        else
        {
            throw new IllegalArgumentException( repr.getClass().toString() );
        }
    }

    public static String serialize( ValueRepresentation repr )
    {
        return serialize( BASE_URI, repr );
    }

    public static String serialize( URI baseUri, ValueRepresentation repr )
    {
        return repr.serialize( new StringFormat(), baseUri, null );
    }

    public static Map<String, Object> serialize( MappingRepresentation repr )
    {
        return serialize( BASE_URI, repr );
    }

    public static Map<String, Object> serialize( URI baseUri, MappingRepresentation repr )
    {
        Map<String, Object> result = new HashMap<String, Object>();
        repr.serialize( new MappingSerializer( new MapWrappingWriter( result ), baseUri, null ) );
        return result;
    }

    public static List<Object> serialize( ListRepresentation repr )
    {
        return serialize( BASE_URI, repr );
    }

    public static List<Object> serialize( URI baseUri, ListRepresentation repr )
    {
        List<Object> result = new ArrayList<Object>();
        repr.serialize( new ListSerializer( new ListWrappingWriter( result ), baseUri, null ) );
        return result;
    }
    
    public static long nodeUriToId( String nodeUri )
    {
        int lastSlash = nodeUri.lastIndexOf( '/' );
        if ( lastSlash == -1 )
            throw new IllegalArgumentException( "'" + nodeUri + "' isn't a node URI" );
        return Long.parseLong( nodeUri.substring( lastSlash+1 ) );
    }
    
    private static class StringFormat extends RepresentationFormat
    {
        StringFormat()
        {
            super( MediaType.WILDCARD_TYPE );
        }

        @Override
        protected String serializeValue( String type, Object value )
        {
            return value.toString();
        }

        @Override
        protected String complete( ListWriter serializer )
        {
            throw new UnsupportedOperationException( "StringFormat.complete(ListWriter)" );
        }

        @Override
        protected String complete( MappingWriter serializer )
        {
            throw new UnsupportedOperationException( "StringFormat.complete(MappingWriter)" );
        }

        @Override
        protected ListWriter serializeList( String type )
        {
            throw new UnsupportedOperationException( "StringFormat.serializeList()" );
        }

        @Override
        protected MappingWriter serializeMapping( String type )
        {
            throw new UnsupportedOperationException( "StringFormat.serializeMapping()" );
        }

        @Override
        public List<Object> readList( String input )
        {
            throw new UnsupportedOperationException( "StringFormat.readList()" );
        }

        @Override
        public Map<String, Object> readMap( String input, String... requiredKeys )
        {
            throw new UnsupportedOperationException( "StringFormat.readMap()" );
        }

        @Override
        public Object readValue( String input )
        {
            throw new UnsupportedOperationException( "StringFormat.readValue()" );
        }

        @Override
        public URI readUri( String input )
        {
            throw new UnsupportedOperationException( "StringFormat.readUri()" );
        }
    }
}
