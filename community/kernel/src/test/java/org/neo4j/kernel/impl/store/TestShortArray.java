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
package org.neo4j.kernel.impl.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Array;

import org.neo4j.kernel.impl.store.record.PropertyBlock;

import org.junit.Test;

public class TestShortArray
{
    private static final int DEFAULT_PAYLOAD_SIZE = PropertyType.getPayloadSize();

    @Test
    public void canEncodeSomeSampleArraysWithDefaultPayloadSize() throws Exception
    {
        assertCanEncodeAndDecodeToSameValue( new boolean[] { true, false, true,
                true, true, true, true, true, true, true, false, true } );
        assertCanEncodeAndDecodeToSameValue( new byte[] { -1, -10, 43, 127, 0, 4, 2, 3, 56, 47, 67, 43 } );
        assertCanEncodeAndDecodeToSameValue( new short[] { 1,2,3,45,5,6,7 } );
        assertCanEncodeAndDecodeToSameValue( new int[] { 1,2,3,4,5,6,7 } );
        assertCanEncodeAndDecodeToSameValue( new long[] { 1,2,3,4,5,6,7 } );
        assertCanEncodeAndDecodeToSameValue( new float[] { 0.34f, 0.21f } );
        assertCanEncodeAndDecodeToSameValue( new long[] { 1 << 63, 1 << 63 } );
        assertCanEncodeAndDecodeToSameValue( new long[] { 1 << 63, 1 << 63,
                1 << 63 } );
        assertCanEncodeAndDecodeToSameValue( new byte[] { 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0 } );
        assertCanEncodeAndDecodeToSameValue( new long[] { 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0 } );
    }

    @Test
    public void testCannotEncodeMarginal() throws Exception
    {
        assertCanNotEncode( new long[] { 1l << 15, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1 } );
    }

    @Test
    public void canEncodeBiggerArraysWithBiggerPayloadSize() throws Exception
    {
        int[] intArray = intArray( 10, 2600 );
        assertCanEncodeAndDecodeToSameValue( intArray, 32 );
    }

    private void assertCanNotEncode( Object intArray )
    {
        assertCanNotEncode( intArray, DEFAULT_PAYLOAD_SIZE );
    }

    private void assertCanNotEncode( Object intArray, int payloadSize )
    {
        assertFalse( ShortArray.encode( 0, intArray, new PropertyBlock(),
                payloadSize ) );
    }

    private int[] intArray( int count, int stride )
    {
        int[] result = new int[count];
        for ( int i = 0; i < count; i++ )
        {
            result[i] = i*stride;
        }
        return result;
    }

    private void assertCanEncodeAndDecodeToSameValue( Object value )
    {
        assertCanEncodeAndDecodeToSameValue( value, PropertyType.getPayloadSize() );
    }

    private void assertCanEncodeAndDecodeToSameValue( Object value, int payloadSize )
    {
        PropertyBlock target = new PropertyBlock();
        boolean encoded = ShortArray.encode( 0, value, target, payloadSize );
        assertTrue( encoded );
        assertArraysEquals( value, ShortArray.decode( target ) );
    }

    private void assertArraysEquals( Object value1, Object value2 )
    {
        assertEquals( value1.getClass().getComponentType(), value2.getClass().getComponentType() );
        int length1 = Array.getLength( value1 );
        int length2 = Array.getLength( value2 );
        assertEquals( length1, length2 );

        for ( int i = 0; i < length1; i++ )
        {
            Object item1 = Array.get( value1, i );
            Object item2 = Array.get( value2, i );
            assertEquals( item1, item2 );
        }
    }
}
