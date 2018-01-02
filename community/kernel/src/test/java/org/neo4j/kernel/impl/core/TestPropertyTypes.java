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
package org.neo4j.kernel.impl.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Array;
import java.util.Arrays;

import org.neo4j.graphdb.Node;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.ObjectUtil;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static java.lang.String.format;

public class TestPropertyTypes extends AbstractNeo4jTestCase
{
    private Node node1 = null;

    @Before
    public void createInitialNode()
    {
        node1 = getGraphDb().createNode();
    }

    @After
    public void deleteInitialNode()
    {
        node1.delete();
    }

    @Test
    public void testDoubleType()
    {
        Double dValue = new Double( 45.678d );
        String key = "testdouble";
        node1.setProperty( key, dValue );
        newTransaction();
        Double propertyValue = null;
        propertyValue = (Double) node1.getProperty( key );
        assertEquals( dValue, propertyValue );
        dValue = new Double( 56784.3243d );
        node1.setProperty( key, dValue );
        newTransaction();
        propertyValue = (Double) node1.getProperty( key );
        assertEquals( dValue, propertyValue );

        node1.removeProperty( key );
        newTransaction();
        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testFloatType()
    {
        Float fValue = new Float( 45.678f );
        String key = "testfloat";
        node1.setProperty( key, fValue );
        newTransaction();

        Float propertyValue = null;
        propertyValue = (Float) node1.getProperty( key );
        assertEquals( fValue, propertyValue );

        fValue = new Float( 5684.3243f );
        node1.setProperty( key, fValue );
        newTransaction();

        propertyValue = (Float) node1.getProperty( key );
        assertEquals( fValue, propertyValue );

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testLongType()
    {
        long time = System.currentTimeMillis();
        Long lValue = new Long( time );
        String key = "testlong";
        node1.setProperty( key, lValue );
        newTransaction();

        Long propertyValue = null;
        propertyValue = (Long) node1.getProperty( key );
        assertEquals( lValue, propertyValue );

        lValue = new Long( System.currentTimeMillis() );
        node1.setProperty( key, lValue );
        newTransaction();

        propertyValue = (Long) node1.getProperty( key );
        assertEquals( lValue, propertyValue );

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );

        node1.setProperty( "other", 123L );
        assertEquals( 123L, node1.getProperty( "other" ) );
        newTransaction();
        assertEquals( 123L, node1.getProperty( "other" ) );
    }

    @Test
    public void testIntType()
    {
        int time = (int)System.currentTimeMillis();
        Integer iValue = new Integer( time );
        String key = "testing";
        node1.setProperty( key, iValue );
        newTransaction();

        Integer propertyValue = null;
        propertyValue = (Integer) node1.getProperty( key );
        assertEquals( iValue, propertyValue );

        iValue = new Integer( (int)System.currentTimeMillis() );
        node1.setProperty( key, iValue );
        newTransaction();

        propertyValue = (Integer) node1.getProperty( key );
        assertEquals( iValue, propertyValue );

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );

        node1.setProperty( "other", 123L );
        assertEquals( 123L, node1.getProperty( "other" ) );
        newTransaction();
        assertEquals( 123L, node1.getProperty( "other" ) );
    }

    @Test
    public void testByteType()
    {
        byte b = (byte) 177;
        Byte bValue = new Byte( b );
        String key = "testbyte";
        node1.setProperty( key, bValue );
        newTransaction();

        Byte propertyValue = null;
        propertyValue = (Byte) node1.getProperty( key );
        assertEquals( bValue, propertyValue );

        bValue = new Byte( (byte) 200 );
        node1.setProperty( key, bValue );
        newTransaction();

        propertyValue = (Byte) node1.getProperty( key );
        assertEquals( bValue, propertyValue );

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testShortType()
    {
        short value = 453;
        Short sValue = new Short( value );
        String key = "testshort";
        node1.setProperty( key, sValue );
        newTransaction();

        Short propertyValue = null;
        propertyValue = (Short) node1.getProperty( key );
        assertEquals( sValue, propertyValue );

        sValue = new Short( (short) 5335 );
        node1.setProperty( key, sValue );
        newTransaction();

        propertyValue = (Short) node1.getProperty( key );
        assertEquals( sValue, propertyValue );

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testCharType()
    {
        char c = 'c';
        Character cValue = new Character( c );
        String key = "testchar";
        node1.setProperty( key, cValue );
        newTransaction();

        Character propertyValue = null;
        propertyValue = (Character) node1.getProperty( key );
        assertEquals( cValue, propertyValue );

        cValue = new Character( 'd' );
        node1.setProperty( key, cValue );
        newTransaction();

        propertyValue = (Character) node1.getProperty( key );
        assertEquals( cValue, propertyValue );

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testBooleanType()
    {
        boolean value = true;
        Boolean bValue = new Boolean( value );
        String key = "testbool";
        node1.setProperty( key, bValue );
        newTransaction();

        Boolean propertyValue = null;
        propertyValue = (Boolean) node1.getProperty( key );
        assertEquals( bValue, propertyValue );

        bValue = new Boolean( false );
        node1.setProperty( key, bValue );
        newTransaction();

        propertyValue = (Boolean) node1.getProperty( key );
        assertEquals( bValue, propertyValue );

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testIntArray()
    {
        int[] array1 = new int[] { 1, 2, 3, 4, 5 };
        Integer[] array2 = new Integer[] { 6, 7, 8 };
        String key = "testintarray";
        node1.setProperty( key, array1 );
        newTransaction();

        int propertyValue[] = null;
        propertyValue = (int[]) node1.getProperty( key );
        assertEquals( array1.length, propertyValue.length );
        for ( int i = 0; i < array1.length; i++ )
        {
            assertEquals( array1[i], propertyValue[i] );
        }

        node1.setProperty( key, array2 );
        newTransaction();

        propertyValue = (int[]) node1.getProperty( key );
        assertEquals( array2.length, propertyValue.length );
        for ( int i = 0; i < array2.length; i++ )
        {
            assertEquals( array2[i], new Integer( propertyValue[i] ) );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testShortArray()
    {
        short[] array1 = new short[] { 1, 2, 3, 4, 5 };
        Short[] array2 = new Short[] { 6, 7, 8 };
        String key = "testintarray";
        node1.setProperty( key, array1 );
        newTransaction();

        short propertyValue[] = null;
        propertyValue = (short[]) node1.getProperty( key );
        assertEquals( array1.length, propertyValue.length );
        for ( int i = 0; i < array1.length; i++ )
        {
            assertEquals( array1[i], propertyValue[i] );
        }

        node1.setProperty( key, array2 );
        newTransaction();

        propertyValue = (short[]) node1.getProperty( key );
        assertEquals( array2.length, propertyValue.length );
        for ( int i = 0; i < array2.length; i++ )
        {
            assertEquals( array2[i], new Short( propertyValue[i] ) );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testStringArray()
    {
        String[] array1 = new String[] { "a", "b", "c", "d", "e" };
        String[] array2 = new String[] { "ff", "gg", "hh" };
        String key = "teststringarray";
        node1.setProperty( key, array1 );
        newTransaction();

        String propertyValue[] = null;
        propertyValue = (String[]) node1.getProperty( key );
        assertEquals( array1.length, propertyValue.length );
        for ( int i = 0; i < array1.length; i++ )
        {
            assertEquals( array1[i], propertyValue[i] );
        }

        node1.setProperty( key, array2 );
        newTransaction();

        propertyValue = (String[]) node1.getProperty( key );
        assertEquals( array2.length, propertyValue.length );
        for ( int i = 0; i < array2.length; i++ )
        {
            assertEquals( array2[i], propertyValue[i] );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testBooleanArray()
    {
        boolean[] array1 = new boolean[] { true, false, true, false, true };
        Boolean[] array2 = new Boolean[] { false, true, false };
        String key = "testboolarray";
        node1.setProperty( key, array1 );
        newTransaction();

        boolean propertyValue[] = null;
        propertyValue = (boolean[]) node1.getProperty( key );
        assertEquals( array1.length, propertyValue.length );
        for ( int i = 0; i < array1.length; i++ )
        {
            assertEquals( array1[i], propertyValue[i] );
        }

        node1.setProperty( key, array2 );
        newTransaction();

        propertyValue = (boolean[]) node1.getProperty( key );
        assertEquals( array2.length, propertyValue.length );
        for ( int i = 0; i < array2.length; i++ )
        {
            assertEquals( array2[i], new Boolean( propertyValue[i] ) );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testDoubleArray()
    {
        double[] array1 = new double[] { 1.0, 2.0, 3.0, 4.0, 5.0 };
        Double[] array2 = new Double[] { 6.0, 7.0, 8.0 };
        String key = "testdoublearray";
        node1.setProperty( key, array1 );
        newTransaction();

        double propertyValue[] = null;
        propertyValue = (double[]) node1.getProperty( key );
        assertEquals( array1.length, propertyValue.length );
        for ( int i = 0; i < array1.length; i++ )
        {
            assertEquals( array1[i], propertyValue[i], 0.0 );
        }

        node1.setProperty( key, array2 );
        newTransaction();

        propertyValue = (double[]) node1.getProperty( key );
        assertEquals( array2.length, propertyValue.length );
        for ( int i = 0; i < array2.length; i++ )
        {
            assertEquals( array2[i], new Double( propertyValue[i] ) );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testFloatArray()
    {
        float[] array1 = new float[] { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f };
        Float[] array2 = new Float[] { 6.0f, 7.0f, 8.0f };
        String key = "testfloatarray";
        node1.setProperty( key, array1 );
        newTransaction();

        float propertyValue[] = null;
        propertyValue = (float[]) node1.getProperty( key );
        assertEquals( array1.length, propertyValue.length );
        for ( int i = 0; i < array1.length; i++ )
        {
            assertEquals( array1[i], propertyValue[i], 0.0 );
        }

        node1.setProperty( key, array2 );
        newTransaction();

        propertyValue = (float[]) node1.getProperty( key );
        assertEquals( array2.length, propertyValue.length );
        for ( int i = 0; i < array2.length; i++ )
        {
            assertEquals( array2[i], new Float( propertyValue[i] ) );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testLongArray()
    {
        long[] array1 = new long[] { 1, 2, 3, 4, 5 };
        Long[] array2 = new Long[] { 6l, 7l, 8l };
        String key = "testlongarray";
        node1.setProperty( key, array1 );
        newTransaction();

        long[] propertyValue = null;
        propertyValue = (long[]) node1.getProperty( key );
        assertEquals( array1.length, propertyValue.length );
        for ( int i = 0; i < array1.length; i++ )
        {
            assertEquals( array1[i], propertyValue[i] );
        }

        node1.setProperty( key, array2 );
        newTransaction();

        propertyValue = (long[]) node1.getProperty( key );
        assertEquals( array2.length, propertyValue.length );
        for ( int i = 0; i < array2.length; i++ )
        {
            assertEquals( array2[i], new Long( propertyValue[i] ) );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testByteArray()
    {
        byte[] array1 = new byte[] { 1, 2, 3, 4, 5 };
        Byte[] array2 = new Byte[] { 6, 7, 8 };
        String key = "testbytearray";
        node1.setProperty( key, array1 );
        newTransaction();

        byte[] propertyValue = null;
        propertyValue = (byte[]) node1.getProperty( key );
        assertEquals( array1.length, propertyValue.length );
        for ( int i = 0; i < array1.length; i++ )
        {
            assertEquals( array1[i], propertyValue[i] );
        }

        node1.setProperty( key, array2 );
        newTransaction();

        propertyValue = (byte[]) node1.getProperty( key );
        assertEquals( array2.length, propertyValue.length );
        for ( int i = 0; i < array2.length; i++ )
        {
            assertEquals( array2[i], new Byte( propertyValue[i] ) );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testCharArray()
    {
        char[] array1 = new char[] { '1', '2', '3', '4', '5' };
        Character[] array2 = new Character[] { '6', '7', '8' };
        String key = "testchararray";
        node1.setProperty( key, array1 );
        newTransaction();

        char[] propertyValue = null;
        propertyValue = (char[]) node1.getProperty( key );
        assertEquals( array1.length, propertyValue.length );
        for ( int i = 0; i < array1.length; i++ )
        {
            assertEquals( array1[i], propertyValue[i] );
        }

        node1.setProperty( key, array2 );
        newTransaction();

        propertyValue = (char[]) node1.getProperty( key );
        assertEquals( array2.length, propertyValue.length );
        for ( int i = 0; i < array2.length; i++ )
        {
            assertEquals( array2[i], new Character( propertyValue[i] ) );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testEmptyString() throws Exception
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "1", 2 );
        node.setProperty( "2", "" );
        node.setProperty( "3", "" );
        newTransaction();

        assertEquals( 2, node.getProperty( "1" ) );
        assertEquals( "", node.getProperty( "2" ) );
        assertEquals( "", node.getProperty( "3" ) );
    }

    @Test
    public void shouldNotBeAbleToPoisonBooleanArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( new boolean[] {false, false, false}, true );
    }

    @Test
    public void shouldNotBeAbleToPoisonByteArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( new byte[] {0, 0, 0}, (byte)1 );
    }

    @Test
    public void shouldNotBeAbleToPoisonShortArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( new short[] {0, 0, 0}, (short)1 );
    }

    @Test
    public void shouldNotBeAbleToPoisonIntArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( new int[] {0, 0, 0}, 1 );
    }

    @Test
    public void shouldNotBeAbleToPoisonLongArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( new long[] {0, 0, 0}, 1L );
    }

    @Test
    public void shouldNotBeAbleToPoisonFloatArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( new float[] {0F, 0F, 0F}, 1F );
    }

    @Test
    public void shouldNotBeAbleToPoisonDoubleArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( new double[] {0D, 0D, 0D}, 1D );
    }

    @Test
    public void shouldNotBeAbleToPoisonCharArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( new char[] {'0', '0', '0'}, '1' );
    }

    @Test
    public void shouldNotBeAbleToPoisonStringArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( new String[] {"zero", "zero", "zero"}, "one" );
    }

    private Object veryLongArray( Class<?> type )
    {
        Object array = Array.newInstance( type, 1000 );
        return array;
    }

    private String[] veryLongStringArray()
    {
        String[] array = new String[100];
        Arrays.fill( array, "zero" );
        return array;
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongBooleanArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Boolean.TYPE ), true );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongByteArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Byte.TYPE ), (byte)1 );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongShortArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Short.TYPE ), (short)1 );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongIntArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Integer.TYPE ), 1 );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongLongArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Long.TYPE ), 1L );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongFloatArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Float.TYPE ), 1F );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongDoubleArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Double.TYPE ), 1D );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongCharArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Character.TYPE ), '1' );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongStringArrayProperty() throws Exception
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongStringArray(), "one" );
    }

    private void shouldNotBeAbleToPoisonArrayProperty( Object value, Object poison )
    {
        shouldNotBeAbleToPoisonArrayPropertyInsideTransaction( value, poison );
        shouldNotBeAbleToPoisonArrayPropertyOutsideTransaction( value, poison );
    }

    private void shouldNotBeAbleToPoisonArrayPropertyInsideTransaction( Object value, Object poison )
    {
        // GIVEN
        String key = "key";
        // setting a property, then reading it back
        node1.setProperty( key, value );
        Object readValue = node1.getProperty( key );

        // WHEN changing the value read back
        Array.set( readValue, 0, poison );

        // THEN reading the value one more time should still yield the set property
        assertTrue(
                format( "Expected %s, but was %s", ObjectUtil.toString( value ), ObjectUtil.toString( readValue ) ),
                ArrayUtil.equals( value, node1.getProperty( key ) ) );
    }

    private void shouldNotBeAbleToPoisonArrayPropertyOutsideTransaction( Object value, Object poison )
    {
        // GIVEN
        String key = "key";
        // setting a property, then reading it back
        node1.setProperty( key, value );
        newTransaction();
        Object readValue = node1.getProperty( key );

        // WHEN changing the value read back
        Array.set( readValue, 0, poison );

        // THEN reading the value one more time should still yield the set property
        assertTrue(
                format( "Expected %s, but was %s", ObjectUtil.toString( value ), ObjectUtil.toString( readValue ) ),
                ArrayUtil.equals( value, node1.getProperty( key ) ) );
    }
}
