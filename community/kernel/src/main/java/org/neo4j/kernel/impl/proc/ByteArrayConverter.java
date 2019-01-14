/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.ByteValue;
import org.neo4j.values.storable.Values;

import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntByteArray;
import static org.neo4j.kernel.impl.proc.ParseUtil.parseList;
import static org.neo4j.values.SequenceValue.IterationPreference.RANDOM_ACCESS;

public class ByteArrayConverter implements Function<String,DefaultParameterValue>, FieldSignature.InputMapper
{

    @Override
    public DefaultParameterValue apply( String s )
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
            throw new IllegalArgumentException(
                    "Cannot convert " + input.getClass().getSimpleName() + " to byte[] for input to procedure" );
        }
    }

    @Override
    public AnyValue map( AnyValue input )
    {

        if ( input instanceof ByteArray )
        {
            return input;
        }
        if ( input instanceof SequenceValue )
        {
            SequenceValue list = (SequenceValue) input;
            if ( list.iterationPreference() == RANDOM_ACCESS )
            {
                byte[] bytes = new byte[list.length()];
                for ( int a = 0; a < bytes.length; a++ )
                {
                    bytes[a] = asByte( list.value( a ) );
                }
                return Values.byteArray( bytes );
            }
            else
            {
                //this may have linear complexity, still worth doing it upfront
                byte[] bytes = new byte[list.length()];
                int i = 0;
                for ( AnyValue anyValue : list )
                {
                    bytes[i++] = asByte( anyValue );
                }

                return Values.byteArray( bytes );
            }
        }
        else
        {
            throw new IllegalArgumentException(
                    "Cannot convert " + input.getClass().getSimpleName() + " to byte[] for input to procedure" );
        }
    }

    private byte asByte( AnyValue value )
    {
        if ( value instanceof ByteValue )
        {
            return ((ByteValue) value).value();
        }
        else
        {
            throw new IllegalArgumentException(
                    "Cannot convert " + value.map( new DefaultValueMapper( null ) ) + " to byte for input to procedure" );
        }
    }
}
