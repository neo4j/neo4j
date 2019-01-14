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
package org.neo4j.kernel.impl.api.store;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.neo4j.kernel.impl.store.GeometryType;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.ShortArray;
import org.neo4j.kernel.impl.store.TemporalType;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.Values;

public class PropertyUtil
{
    public static ArrayValue readArrayFromBuffer( ByteBuffer buffer )
    {
        if ( buffer.limit() <= 0 )
        {
            throw new IllegalStateException( "Given buffer is empty" );
        }

        byte typeId = buffer.get();
        buffer.order( ByteOrder.BIG_ENDIAN );
        try
        {
            if ( typeId == PropertyType.STRING.intValue() )
            {
                int arrayLength = buffer.getInt();
                String[] result = new String[arrayLength];

                for ( int i = 0; i < arrayLength; i++ )
                {
                    int byteLength = buffer.getInt();
                    result[i] = UTF8.decode( buffer.array(), buffer.position(), byteLength );
                    buffer.position( buffer.position() + byteLength );
                }
                return Values.stringArray( result );
            }
            else if ( typeId == PropertyType.GEOMETRY.intValue() )
            {
                GeometryType.GeometryHeader header = GeometryType.GeometryHeader.fromArrayHeaderByteBuffer( buffer );
                byte[] byteArray = new byte[buffer.limit() - buffer.position()];
                buffer.get( byteArray );
                return GeometryType.decodeGeometryArray( header, byteArray );
            }
            else if ( typeId == PropertyType.TEMPORAL.intValue() )
            {
                TemporalType.TemporalHeader header = TemporalType.TemporalHeader.fromArrayHeaderByteBuffer( buffer );
                byte[] byteArray = new byte[buffer.limit() - buffer.position()];
                buffer.get( byteArray );
                return TemporalType.decodeTemporalArray( header, byteArray );
            }
            else
            {
                ShortArray type = ShortArray.typeOf( typeId );
                int bitsUsedInLastByte = buffer.get();
                int requiredBits = buffer.get();
                if ( requiredBits == 0 )
                {
                    return type.createEmptyArray();
                }
                if ( type == ShortArray.BYTE && requiredBits == Byte.SIZE )
                {   // Optimization for byte arrays (probably large ones)
                    byte[] byteArray = new byte[buffer.limit() - buffer.position()];
                    buffer.get( byteArray );
                    return Values.byteArray( byteArray );
                }
                else
                {   // Fallback to the generic approach, which is a slower
                    Bits bits = Bits.bitsFromBytes( buffer.array(), buffer.position() );
                    int length = ((buffer.limit() - buffer.position()) * 8 - (8 - bitsUsedInLastByte)) / requiredBits;
                    return type.createArray( length, bits, requiredBits );
                }
            }
        }
        finally
        {
            buffer.order( ByteOrder.LITTLE_ENDIAN );
        }
    }
}
