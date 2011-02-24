/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.repr;

import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.neo4j.server.rest.repr.formats.ListWrappingWriter;
import org.neo4j.server.rest.repr.formats.MapWrappingWriter;

public abstract class RepresentationTestBase
{
    public static final URI BASE_URI = URI.create( "http://neo4j.org/" );
    static final String NODE_URI_PATTERN = "http://.*/node/[0-9]+";
    static final String RELATIONSHIP_URI_PATTERN = "http://.*/relationship/[0-9]+";

    static void assertUriMatches( String expectedRegex, ValueRepresentation uriRepr ) throws BadInputException
    {
        assertUriMatches( expectedRegex, serialize( uriRepr ) );
    }

    static void assertUriMatches( String expectedRegex, URI actualUri )
    {
        assertUriMatches( expectedRegex, actualUri.toString() );
    }

    public static Object serialize( Representation repr ) throws BadInputException
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

    public static String serialize( ValueRepresentation repr ) throws BadInputException
    {
        return repr.serialize( new StringFormat(), BASE_URI, null );
    }

    static void assertUriMatches( String expectedRegex, String actualUri )
    {
        assertTrue( "expected <" + expectedRegex + "> got <" + actualUri + ">",
                actualUri.matches( expectedRegex ) );
    }

    static String uriPattern( String subPath )
    {
        return "http://.*/[0-9]+" + subPath;
    }

    public static Map<String, Object> serialize( MappingRepresentation repr )
    {
        Map<String, Object> result = new HashMap<String, Object>();
        repr.serialize( new MappingSerializer( new MapWrappingWriter( result ), BASE_URI, null ) );
        return result;
    }

    public static List<Object> serialize( ListRepresentation repr )
    {
        List<Object> result = new ArrayList<Object>();
        repr.serialize( new ListSerializer( new ListWrappingWriter( result ), BASE_URI, null ) );
        return result;
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
        public Map<String, Object> readMap( String input )
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
