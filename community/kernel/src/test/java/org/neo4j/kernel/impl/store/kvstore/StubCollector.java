/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.kvstore;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class StubCollector extends MetadataCollector<Map<String, byte[]>>
{
    private final HeaderField<Map<String, byte[]>, byte[]>[] headerFields;

    StubCollector( int entriesPerPage, String... header )
    {
        this( entriesPerPage, headerFields( header ) );
    }

    private StubCollector( int entriesPerPage, HeaderField<Map<String, byte[]>, byte[]>[] headerFields )
    {
        super( entriesPerPage, headerFields );
        this.headerFields = headerFields;
    }

    @Override
    boolean verifyFormatSpecifier( ReadableBuffer value )
    {
        return true;
    }

    @Override
    Map<String, byte[]> metadata()
    {
        return metadata( headerFields, this );
    }

    static Map<String, byte[]> metadata( HeaderField<Map<String, byte[]>, byte[]>[] headerFields,
                                         CollectedMetadata metadata )
    {
        Map<String, byte[]> result = new HashMap<>();
        for ( HeaderField<Map<String, byte[]>, byte[]> field : headerFields )
        {
            result.put( field.toString(), metadata.getMetadata( field ) );
        }
        return Collections.unmodifiableMap( result );
    }

    static HeaderField<Map<String, byte[]>, byte[]>[] headerFields( String[] keys )
    {
        @SuppressWarnings("unchecked")
        HeaderField<Map<String, byte[]>, byte[]>[] fields = new HeaderField[keys.length];
        for ( int i = 0; i < keys.length; i++ )
        {
            fields[i] = headerField( keys[i] );
        }
        return fields;
    }

    private static HeaderField<Map<String, byte[]>, byte[]> headerField( final String key )
    {
        return new HeaderField<Map<String, byte[]>, byte[]>()
        {
            @Override
            public byte[] read( ReadableBuffer header )
            {
                return header.get( 0, new byte[header.size()] );
            }

            @Override
            public void write( Map<String, byte[]> headers, WritableBuffer header )
            {
                header.put( 0, headers.get( key ) );
            }

            @Override
            public String toString()
            {
                return key;
            }
        };
    }
}
