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
package org.neo4j.kernel.api.properties;

import java.lang.reflect.Array;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PropertyConversionTest
{
    private static Random random;

    @Test
    public void shouldConvertStringProperty() throws Exception
    {
        assertConverts( "a value" );
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void shouldConvertSmallLongProperty() throws Exception
    {
        assertConverts( Long.valueOf( 1 + random.nextInt( 1024 ) ) );
    }

    @Test
    public void shouldConvertBigLongProperty() throws Exception
    {
        assertConverts( (1l + random.nextInt( 1024 )) << 32 );
    }

    @Test
    public void shouldConvertIntegerProperty() throws Exception
    {
        assertConverts( 1 + random.nextInt( 1024 ) );
    }

    @Test
    public void shouldConvertCharProperty() throws Exception
    {
        assertConverts( randomChar() );
    }

    @Test
    public void shouldConvertShortProperty() throws Exception
    {
        assertConverts( randomShort() );
    }

    @Test
    public void shouldConvertByteProperty() throws Exception
    {
        assertConverts( randomByte() );
    }

    @Test
    public void shouldConvertBooleanProperty() throws Exception
    {
        assertConverts( random.nextBoolean() );
    }

    @Test
    public void shouldConvertFloatProperty() throws Exception
    {
        assertConverts( 1f + random.nextFloat() );
    }

    @Test
    public void shouldConvertDoubleProperty() throws Exception
    {
        assertConverts( 1.0 + random.nextDouble() );
    }

    // Arrays

    @Test
    public void shouldConvertStringArrayProperty() throws Exception
    {
        assertConverts( new String[]{"foo", "bar", "baz"} );
    }

    @Test
    public void shouldNotSupportNullInArrays() throws Exception
    {
        // given
        for ( Object[] array : new Object[][]{new String[]{null},
                                              new Byte[]{null},
                                              new Long[]{null},
                                              new Integer[]{null},
                                              new Double[]{null},
                                              new Float[]{null},
                                              new Boolean[]{null},
                                              new Character[]{null},
                                              new Short[]{null}} )
        {
            // when
            try
            {
                PropertyConversion.convertProperty( 17, array );
                fail( "Should not support nulls in " + array.getClass() );
            }
            // then
            catch ( IllegalArgumentException e )
            {
                assertEquals( "Property array value elements may not be null.", e.getMessage() );
            }
        }
    }

    @Test
    public void shouldConvertLongArrayProperty() throws Exception
    {
        assertConverts( new long[]{random.nextLong(), random.nextLong(), random.nextLong()} );
        assertConverts( new Long[]{random.nextLong(), random.nextLong(), random.nextLong()} );
    }

    @Test
    public void shouldConvertIntegerArrayProperty() throws Exception
    {
        assertConverts( new int[]{random.nextInt(), random.nextInt(), random.nextInt()} );
        assertConverts( new Integer[]{random.nextInt(), random.nextInt(), random.nextInt()} );
    }

    @Test
    public void shouldConvertCharArrayProperty() throws Exception
    {
        assertConverts( new char[]{randomChar(), randomChar(), randomChar()} );
        assertConverts( new Character[]{randomChar(), randomChar(), randomChar()} );
    }

    @Test
    public void shouldConvertShortArrayProperty() throws Exception
    {
        assertConverts( new short[]{randomShort(), randomShort(), randomShort()} );
        assertConverts( new Short[]{randomShort(), randomShort(), randomShort()} );
    }

    @Test
    public void shouldConvertByteArrayProperty() throws Exception
    {
        assertConverts( new byte[]{randomByte(), randomByte(), randomByte()} );
        assertConverts( new Byte[]{randomByte(), randomByte(), randomByte()} );
    }

    @Test
    public void shouldConvertBooleanArrayProperty() throws Exception
    {
        assertConverts( new boolean[]{random.nextBoolean(), random.nextBoolean(), random.nextBoolean()} );
        assertConverts( new Boolean[]{random.nextBoolean(), random.nextBoolean(), random.nextBoolean()} );
    }

    @Test
    public void shouldConvertFloatArrayProperty() throws Exception
    {
        assertConverts( new float[]{random.nextFloat(), random.nextFloat(), random.nextFloat()} );
        assertConverts( new Float[]{random.nextFloat(), random.nextFloat(), random.nextFloat()} );
    }

    @Test
    public void shouldConvertDoubleArrayProperty() throws Exception
    {
        assertConverts( new double[]{random.nextDouble(), random.nextDouble(), random.nextDouble()} );
        assertConverts( new Double[]{random.nextDouble(), random.nextDouble(), random.nextDouble()} );
    }

    private static char randomChar()
    {
        return (char) (1 + random.nextInt( Character.MAX_VALUE - 1 ));
    }

    private static short randomShort()
    {
        return (short) (1 + random.nextInt( Short.MAX_VALUE - 1 ));
    }

    private static byte randomByte()
    {
        return (byte) (1 + random.nextInt( Byte.MAX_VALUE - 1 ));
    }

    @BeforeClass
    public static void randomize()
    {
        random = new Random();
    }

    private static void assertConverts( Object value )
    {
        DefinedProperty property = PropertyConversion.convertProperty( 17, value );
        assertDeepEquals( value, property.value() );
        assertTrue( "valueEquals:" + value.getClass(), property.valueEquals( value ) );
        assertTrue( "two conversions are equal", property.equals( PropertyConversion.convertProperty( 17, value ) ) );
        assertEquals( "hashCode()", property.hashCode(), PropertyConversion.convertProperty( 17, value ).hashCode() );
        assertFalse( "properties with different keys should not be equal",
                     property.equals( PropertyConversion.convertProperty( 666, value ) ) );
        // this needs to be last, because another() will mutate arrays in place for extra nastyness
        assertFalse( "properties with different values should not be equal",
                     property.equals( PropertyConversion.convertProperty( 17, another( value ) ) ) );
    }

    private static void assertDeepEquals( Object expected, Object actual )
    {
        if ( expected.getClass().isArray() )
        {
            assertNotNull( actual );
            assertTrue( "is array", actual.getClass().isArray() );
            int length = Array.getLength( expected );
            if ( expected instanceof Object[] )
            {
                if ( expected instanceof String[] )
                {
                    assertEquals( expected.getClass(), actual.getClass() );
                }
                else
                {
                    assertTrue( "can only compare boxed arrays of non-zero length", length > 0 );
                }
            }
            else
            {
                assertEquals( "component type", expected.getClass(), actual.getClass() );
            }
            assertEquals( "array length", length, Array.getLength( actual ) );
            for ( int i = 0; i < length; i++ )
            {
                assertEquals( Array.get( expected, i ), Array.get( actual, i ) );
            }
        }
        else
        {
            assertEquals( expected, actual );
        }
    }

    private static Object another( Object value )
    {
        if ( value instanceof Long )
        {
            return -((Long) value);
        }
        if ( value instanceof Integer )
        {
            return -((Integer) value);
        }
        if ( value instanceof Short )
        {
            return (short) -((Short) value);
        }
        if ( value instanceof Byte )
        {
            return (byte) -((Byte) value);
        }
        if ( value instanceof Character )
        {
            return (char) -((Character) value);
        }
        if ( value instanceof Boolean )
        {
            return !(Boolean) value;
        }
        if ( value instanceof Float )
        {
            return -(Float) value;
        }
        if ( value instanceof Double )
        {
            return -(Double) value;
        }
        if ( value instanceof String )
        {
            return "not " + value;
        }
        if ( value.getClass().isArray() )
        {
            for ( int i = 0, len = Array.getLength( value ); i < len; i++ )
            {
                Array.set( value, i, another( Array.get( value, i ) ) );
            }
            return value;
        }
        throw new AssertionError( "unexpected type: " + value.getClass().getName() );
    }
}
