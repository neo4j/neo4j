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
package org.neo4j.kernel.impl.store.kvstore;

class StubCollector extends MetadataCollector
{
    StubCollector( int entriesPerPage, String... header )
    {
        super( entriesPerPage, headerFields( header ) );
    }

    @Override
    boolean verifyFormatSpecifier( ReadableBuffer value )
    {
        return true;
    }

    static HeaderField<byte[]>[] headerFields( String[] keys )
    {
        @SuppressWarnings("unchecked")
        HeaderField<byte[]>[] fields = new HeaderField[keys.length];
        for ( int i = 0; i < keys.length; i++ )
        {
            fields[i] = headerField( keys[i] );
        }
        return fields;
    }

    private static HeaderField<byte[]> headerField( final String key )
    {
        return new HeaderField<byte[]>()
        {
            @Override
            public byte[] read( ReadableBuffer header )
            {
                return header.get( 0, new byte[header.size()] );
            }

            @Override
            public void write( byte[] bytes, WritableBuffer header )
            {
                header.put( 0, bytes );
            }

            @Override
            public String toString()
            {
                return key;
            }
        };
    }
}
