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
package org.neo4j.kernel.impl.proc;

import java.util.List;
import java.util.function.Function;

import org.neo4j.kernel.api.proc.FieldSignature;

import static org.neo4j.kernel.impl.proc.Neo4jValue.ntByteArray;
import static org.neo4j.kernel.impl.proc.ParseUtil.parseList;

public class ByteArrayConverter implements Function<String,Neo4jValue>, FieldSignature.InputMapper
{
    @Override
    public Neo4jValue apply( String s )
    {
        String value = s.trim();
        if ( value.equalsIgnoreCase( "null" ) )
        {
            return ntByteArray( null );
        }
        else
        {
            List<Long> values = parseList( value, Long.class );
            byte[] bytes = new byte[values.size()];
            for ( int i = 0; i < bytes.length; i++ )
            {
                bytes[i] = values.get( i ).byteValue();
            }
            return ntByteArray( bytes );
        }
    }

    @Override
    public Object map( Object input )
    {
        if ( input instanceof byte[] )
        {
            return input;
        }
        if ( input instanceof List<?> )
        {
            List list = (List) input;
            byte[] bytes = new byte[list.size()];
            for ( int a = 0; a < bytes.length; a++ )
            {
                Object value = list.get( a );
                if ( value instanceof Byte )
                {
                    bytes[a] = (Byte) value;
                }
                else
                {
                    throw new IllegalArgumentException( "Cannot convert " + value + " to byte for input to procedure" );
                }
            }
            return bytes;
        }
        else
        {
            throw new IllegalArgumentException( "Cannot convert " + input.getClass().getSimpleName() + " to byte[] for input to procedure" );
        }
    }
}
