/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.values.storable;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValueAsObjectCopyTest
{
    private Iterable<AsObjectCopyTest> scalars = Arrays.asList(
            shouldGivePublic( Values.byteValue( (byte)1 ), (byte)1 ),
            shouldGivePublic( Values.shortValue( (short)2 ), (short)2 ),
            shouldGivePublic( Values.intValue( 3 ), 3 ),
            shouldGivePublic( Values.longValue( 4L ), 4L ),
            shouldGivePublic( Values.floatValue( 5.0f ), 5.0f ),
            shouldGivePublic( Values.doubleValue( 6.0 ), 6.0 ),
            shouldGivePublic( Values.booleanValue( false ), false ),
            shouldGivePublic( Values.charValue( 'a' ), 'a' ),
            shouldGivePublic( Values.stringValue( "b" ), "b" )
        );

    @Test
    void shouldProvideScalarValueAsPublic()
    {
        for ( AsObjectCopyTest test : scalars )
        {
            test.assertGeneratesPublic();
        }
    }

    // DIRECT ARRAYS

    @Test
    void shouldProvideDirectByteArrayAsPublic()
    {
        byte[] inStore = {1};
        Value value = Values.byteArray( inStore );
        Object asObject = value.asObjectCopy();
        assertNotNull( asObject, "should return byte[]" );

        byte[] arr = (byte[]) asObject;
        assertTrue( Arrays.equals( inStore, arr ), "should have same values" );

        arr[0] = -1;
        assertFalse( Arrays.equals( inStore, arr ), "should not modify inStore array" );
        assertTrue( Arrays.equals( inStore, (byte[]) value.asObjectCopy() ), "should still generate inStore array" );
    }

    @Test
    void shouldProvideDirectShortArrayAsPublic()
    {
        short[] inStore = {1};
        Value value = Values.shortArray( inStore );
        Object asObject = value.asObjectCopy();
        assertNotNull( asObject, "should return short[]" );

        short[] arr = (short[]) asObject;
        assertTrue( Arrays.equals( inStore, arr ), "should have same values" );

        arr[0] = -1;
        assertFalse( Arrays.equals( inStore, arr ), "should not modify inStore array" );
        assertTrue( Arrays.equals( inStore, (short[])value.asObjectCopy() ), "should still generate inStore array" );
    }

    @Test
    void shouldProvideDirectIntArrayAsPublic()
    {
        int[] inStore = {1};
        Value value = Values.intArray( inStore );
        Object asObject = value.asObjectCopy();
        assertNotNull( asObject, "should return int[]" );

        int[] arr = (int[]) asObject;
        assertTrue( Arrays.equals( inStore, arr ), "should have same values" );

        arr[0] = -1;
        assertFalse( Arrays.equals( inStore, arr ), "should not modify inStore array" );
        assertTrue( Arrays.equals( inStore, (int[])value.asObjectCopy() ), "should still generate inStore array" );
    }

    @Test
    void shouldProvideDirectLongArrayAsPublic()
    {
        long[] inStore = {1};
        Value value = Values.longArray( inStore );
        Object asObject = value.asObjectCopy();
        assertNotNull( asObject, "should return long[]" );

        long[] arr = (long[]) asObject;
        assertTrue( Arrays.equals( inStore, arr ), "should have same values" );

        arr[0] = -1;
        assertFalse( Arrays.equals( inStore, arr ), "should not modify inStore array" );
        assertTrue( Arrays.equals( inStore, (long[])value.asObjectCopy() ), "should still generate inStore array" );
    }

    @Test
    void shouldProvideDirectFloatArrayAsPublic()
    {
        float[] inStore = {1};
        Value value = Values.floatArray( inStore );
        Object asObject = value.asObjectCopy();
        assertNotNull( asObject, "should return float[]" );

        float[] arr = (float[]) asObject;
        assertTrue( Arrays.equals( inStore, arr ), "should have same values" );

        arr[0] = -1;
        assertFalse( Arrays.equals( inStore, arr ), "should not modify inStore array" );
        assertTrue( Arrays.equals( inStore, (float[])value.asObjectCopy() ), "should still generate inStore array" );
    }

    @Test
    void shouldProvideDirectDoubleArrayAsPublic()
    {
        double[] inStore = {1};
        Value value = Values.doubleArray( inStore );
        Object asObject = value.asObjectCopy();
        assertNotNull( asObject, "should return double[]" );

        double[] arr = (double[]) asObject;
        assertTrue( Arrays.equals( inStore, arr ), "should have same values" );

        arr[0] = -1;
        assertFalse( Arrays.equals( inStore, arr ), "should not modify inStore array" );
        assertTrue( Arrays.equals( inStore, (double[])value.asObjectCopy() ), "should still generate inStore array" );
    }

    @Test
    void shouldProvideDirectCharArrayAsPublic()
    {
        char[] inStore = {'a'};
        Value value = Values.charArray( inStore );
        Object asObject = value.asObjectCopy();
        assertNotNull( asObject, "should return char[]" );

        char[] arr = (char[]) asObject;
        assertTrue( Arrays.equals( inStore, arr ), "should have same values" );

        arr[0] = 'b';
        assertFalse( Arrays.equals( inStore, arr ), "should not modify inStore array" );
        assertTrue( Arrays.equals( inStore, (char[])value.asObjectCopy() ), "should still generate inStore array" );
    }

    @Test
    void shouldProvideDirectStringArrayAsPublic()
    {
        String[] inStore = {"a"};
        Value value = Values.stringArray( inStore );
        Object asObject = value.asObjectCopy();
        assertNotNull( asObject, "should return String[]" );

        String[] arr = (String[]) asObject;
        assertTrue( Arrays.equals( inStore, arr ), "should have same values" );

        arr[0] = "b";
        assertFalse( Arrays.equals( inStore, arr ), "should not modify inStore array" );
        assertTrue( Arrays.equals( inStore, (String[])value.asObjectCopy() ), "should still generate inStore array" );
    }

    @Test
    void shouldProvideDirectBooleanArrayAsPublic()
    {
        boolean[] inStore = {true};
        Value value = Values.booleanArray( inStore );
        Object asObject = value.asObjectCopy();
        assertNotNull( asObject, "should return boolean[]" );

        boolean[] arr = (boolean[]) asObject;
        assertTrue( Arrays.equals( inStore, arr ), "should have same values" );

        arr[0] = false;
        assertFalse( Arrays.equals( inStore, arr ), "should not modify inStore array" );
        assertTrue( Arrays.equals( inStore, (boolean[])value.asObjectCopy() ), "should still generate inStore array" );
    }

    private AsObjectCopyTest shouldGivePublic( Value value, Object asObject )
    {
        return new AsObjectCopyTest( value, asObject );
    }

    private static class AsObjectCopyTest
    {
        private final Value value;
        private final Object expected;

        private AsObjectCopyTest( Value value, Object expected )
        {
            this.value = value;
            this.expected = expected;
        }

        void assertGeneratesPublic()
        {
            assertThat( value.asObjectCopy(), equalTo( expected ) );
        }
    }
}
