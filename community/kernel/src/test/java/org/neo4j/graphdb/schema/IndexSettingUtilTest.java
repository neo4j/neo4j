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
package org.neo4j.graphdb.schema;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.Value;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexSettingUtilTest
{
    @Test
    void shouldParseBoolean()
    {
        final IndexSetting setting = IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT;
        final Class<?> type = setting.getType();
        assertEquals( Boolean.class, type );

        // Boolean
        Object object = true;
        assertBoolean( setting, object, true );
    }

    @Test
    void shouldParseString()
    {
        final IndexSetting setting = IndexSettingImpl.FULLTEXT_ANALYZER;
        final Class<?> type = setting.getType();
        assertEquals( String.class, type );

        // String
        Object object = "analyser";
        assertString( setting, object, "analyser" );
    }

    @Test
    void shouldParseDoubleArray()
    {
        final IndexSetting setting = IndexSettingImpl.SPATIAL_CARTESIAN_MAX;
        final Class<?> type = setting.getType();
        assertEquals( double[].class, type );

        double[] expectedResult = new double[]{-45.0, -40.0};

        // Primitive arrays
        {
            final byte[] object = new byte[]{-45, -40};
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final short[] object = new short[]{-45, -40};
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final int[] object = new int[]{-45, -40};
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final long[] object = new long[]{-45, -40};
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final float[] object = new float[]{-45.0f, -40.0f};
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final double[] object = new double[]{-45.0, -40.0};
            assertDoubleArray( setting, object, expectedResult );
        }

        // Non primitive arrays
        {
            final Byte[] object = new Byte[]{-45, -40};
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final Short[] object = new Short[]{-45, -40};
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final Integer[] object = new Integer[]{-45, -40};
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final Long[] object = new Long[]{-45L, -40L};
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final Float[] object = new Float[]{-45.0f, -40.0f};
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final Double[] object = new Double[]{-45.0, -40.0};
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final Number[] object = new Number[]{(byte) 1, (short) 2, 3, 4L, 5f, 6.0};
            assertDoubleArray( setting, object, new double[]{1, 2, 3, 4, 5, 6} );
        }

        // Collection
        {
            final List<Byte> object = Arrays.asList( (byte) -45, (byte) -40 );
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final List<Short> object = Arrays.asList( (short) -45, (short) -40 );
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final List<Integer> object = Arrays.asList( -45, -40 );
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final List<Long> object = Arrays.asList( -45L, -40L );
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final List<Float> object = Arrays.asList( -45.0f, -40.0f );
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final List<Double> object = Arrays.asList( -45.0, -40.0 );
            assertDoubleArray( setting, object, expectedResult );
        }
        {
            final List<Number> object = Arrays.asList( (byte) 1, (short) 2, 3, 4L, 5f, 6.0 );
            assertDoubleArray( setting, object, new double[]{1, 2, 3, 4, 5, 6} );
        }
    }

    @Test
    void shouldNotParseDoubleArray()
    {
        final IndexSetting setting = IndexSettingImpl.SPATIAL_CARTESIAN_MAX;
        final Class<?> type = setting.getType();
        assertEquals( double[].class, type );
        {
            Byte[] object = new Byte[]{-45, null};
            assertThrows( NullPointerException.class, () -> IndexSettingUtil.asIndexSettingValue( setting, object ) );
        }
        {
            String[] object = new String[]{"45", "40"};
            assertThrows( IllegalArgumentException.class, () -> IndexSettingUtil.asIndexSettingValue( setting, object ) );
        }
        {
            List<String> object = Arrays.asList( "45", "40" );
            assertThrows( IllegalArgumentException.class, () -> IndexSettingUtil.asIndexSettingValue( setting, object ) );
        }
    }

    private void assertBoolean( IndexSetting setting, Object object, boolean expectedResult )
    {
        Value result = IndexSettingUtil.asIndexSettingValue( setting, object );
        assertTrue( result instanceof BooleanValue );
        assertEquals( expectedResult, ((BooleanValue) result).booleanValue() );
    }

    private void assertString( IndexSetting setting, Object object, String expectedResult )
    {
        Value result = IndexSettingUtil.asIndexSettingValue( setting, object );
        assertTrue( result instanceof StringValue );
        assertEquals( expectedResult, ((StringValue) result).stringValue() );
    }

    private void assertDoubleArray( IndexSetting setting, Object object, double[] expectedResult )
    {
        Value result = IndexSettingUtil.asIndexSettingValue( setting, object );
        assertTrue( result instanceof DoubleArray );
        assertArrayEquals( expectedResult, ((DoubleArray) result).asObjectCopy() );
    }
}
