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
package org.neo4j.server.rest.transactional;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;

public class Neo4jJsonCodec extends ObjectMapper
{
    public Neo4jJsonCodec()
    {
        getSerializationConfig().without( SerializationConfig.Feature.FLUSH_AFTER_WRITE_VALUE );
    }

    @Override
    public void writeValue( JsonGenerator out, Object value ) throws IOException
    {
        if ( value instanceof PropertyContainer )
        {
            writePropertyContainer( out, (PropertyContainer) value );
        }
        else if ( value instanceof Path )
        {
            writePath( out, ((Path) value).iterator() );
        }
        else if (value instanceof Iterable)
        {
            writeIterator( out, ((Iterable) value).iterator() );
        }
        else if ( value instanceof byte[] )
        {
            writeByteArray( out, (byte[]) value );
        }
        else if ( value instanceof Map )
        {
            writeMap(out, (Map) value );
        }
        else
        {
            super.writeValue( out, value );
        }
    }

    private void writeMap( JsonGenerator out, Map value ) throws IOException
    {
        out.writeStartObject();
        try
        {
            Set<Map.Entry> set = value.entrySet();
            for ( Map.Entry e : set )
            {
                Object key = e.getKey();
                out.writeFieldName( key == null ? "null" : key.toString() );
                writeValue( out, e.getValue() );
            }
        }
        finally
        {
            out.writeEndObject();
        }
    }

    private void writeIterator( JsonGenerator out, Iterator value ) throws IOException
    {
        out.writeStartArray();
        try
        {
            while ( value.hasNext() )
            {
                writeValue( out, value.next() );
            }
        }
        finally
        {
            out.writeEndArray();
        }
    }

    private void writePath( JsonGenerator out, Iterator<PropertyContainer> value ) throws IOException
    {
        out.writeStartArray();
        try
        {
            while ( value.hasNext() )
            {
                writePropertyContainer( out, value.next() );
            }
        }
        finally
        {
            out.writeEndArray();
        }
    }

    private void writePropertyContainer( JsonGenerator out, PropertyContainer value ) throws IOException
    {
        out.writeStartObject();
        try
        {
            for ( Map.Entry<String, Object> property : value.getAllProperties().entrySet() )
            {
                out.writeObjectField( property.getKey(), property.getValue() );
            }
        }
        finally
        {
            out.writeEndObject();
        }
    }

    private void writeByteArray( JsonGenerator out, byte[] bytes ) throws IOException
    {
        out.writeStartArray();
        try
        {
            for ( byte b : bytes )
            {
                out.writeNumber( (int) b );
            }
        }
        finally
        {
            out.writeEndArray();
        }
    }
}
