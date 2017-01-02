/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

@RunWith( Parameterized.class )
public class ByteArrayTest
{
    private static final byte[] DEFAULT = new byte[25];

    @Parameters
    public static Collection<Supplier<ByteArray>> data()
    {
        return Arrays.asList(
                () -> NumberArrayFactory.HEAP.newByteArray( 1_000, DEFAULT ),
                () -> NumberArrayFactory.HEAP.newDynamicByteArray( 100, DEFAULT ),
                () -> NumberArrayFactory.OFF_HEAP.newByteArray( 1_000, DEFAULT ),
                () -> NumberArrayFactory.OFF_HEAP.newDynamicByteArray( 100, DEFAULT ),
                () -> NumberArrayFactory.AUTO.newByteArray( 1_000, DEFAULT ),
                () -> NumberArrayFactory.AUTO.newDynamicByteArray( 100, DEFAULT ) );
    }

    @Parameter
    public Supplier<ByteArray> factory;
    private ByteArray array;

    @Before
    public void before()
    {
        array = factory.get();
    }

    @After
    public void after()
    {
        array.close();
    }

    @Test
    public void shouldSetAndGetBasicTypes() throws Exception
    {
        // WHEN
        array.setByte( 0, 0, (byte) 123 );
        array.setShort( 0, 1, (short) 1234 );
        array.setInt( 0, 5, 12345 );
        array.setLong( 0, 9, Long.MAX_VALUE - 100 );
        array.set3ByteInt( 0, 17, 76767 );

        // THEN
        assertEquals( (byte) 123, array.getByte( 0, 0 ) );
        assertEquals( (short) 1234, array.getShort( 0, 1 ) );
        assertEquals( 12345, array.getInt( 0, 5 ) );
        assertEquals( Long.MAX_VALUE - 100, array.getLong( 0, 9 ) );
        assertEquals( 76767, array.get3ByteInt( 0, 17 ) );
    }

    @Test
    public void shouldDetectMinusOne() throws Exception
    {
        // WHEN
        array.set6ByteLong( 10, 2, -1 );
        array.set6ByteLong( 10, 8, -1 );

        // THEN
        assertEquals( -1L, array.get6ByteLong( 10, 2 ) );
        assertEquals( -1L, array.get6ByteLong( 10, 8 ) );
    }
}
