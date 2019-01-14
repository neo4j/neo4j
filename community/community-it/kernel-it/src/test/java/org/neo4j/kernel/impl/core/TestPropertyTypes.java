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
package org.neo4j.kernel.impl.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.Strings;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPropertyTypes extends AbstractNeo4jTestCase
{
    private Node node1;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
        Double dValue = 45.678d;
        String key = "testdouble";
        node1.setProperty( key, dValue );
        newTransaction();
        Double propertyValue;
        propertyValue = (Double) node1.getProperty( key );
        assertEquals( dValue, propertyValue );
        dValue = 56784.3243d;
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
        Float fValue = 45.678f;
        String key = "testfloat";
        node1.setProperty( key, fValue );
        newTransaction();

        Float propertyValue = null;
        propertyValue = (Float) node1.getProperty( key );
        assertEquals( fValue, propertyValue );

        fValue = 5684.3243f;
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
        Long lValue = System.currentTimeMillis();
        String key = "testlong";
        node1.setProperty( key, lValue );
        newTransaction();

        Long propertyValue = null;
        propertyValue = (Long) node1.getProperty( key );
        assertEquals( lValue, propertyValue );

        lValue = System.currentTimeMillis();
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
        Integer iValue = time;
        String key = "testing";
        node1.setProperty( key, iValue );
        newTransaction();

        Integer propertyValue = null;
        propertyValue = (Integer) node1.getProperty( key );
        assertEquals( iValue, propertyValue );

        iValue = (int) System.currentTimeMillis();
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
        Byte bValue = b;
        String key = "testbyte";
        node1.setProperty( key, bValue );
        newTransaction();

        Byte propertyValue = null;
        propertyValue = (Byte) node1.getProperty( key );
        assertEquals( bValue, propertyValue );

        bValue = (byte) 200;
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
        Short sValue = (short) 453;
        String key = "testshort";
        node1.setProperty( key, sValue );
        newTransaction();

        Short propertyValue = null;
        propertyValue = (Short) node1.getProperty( key );
        assertEquals( sValue, propertyValue );

        sValue = (short) 5335;
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
        Character cValue = c;
        String key = "testchar";
        node1.setProperty( key, cValue );
        newTransaction();

        Character propertyValue = null;
        propertyValue = (Character) node1.getProperty( key );
        assertEquals( cValue, propertyValue );

        cValue = 'd';
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
        String key = "testbool";
        node1.setProperty( key, Boolean.TRUE );
        newTransaction();

        Boolean propertyValue = (Boolean) node1.getProperty( key );
        assertEquals( Boolean.TRUE, propertyValue );

        node1.setProperty( key, Boolean.FALSE );
        newTransaction();

        propertyValue = (Boolean) node1.getProperty( key );
        assertEquals( Boolean.FALSE, propertyValue );

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testPointType()
    {
        Point point = Values.pointValue( CoordinateReferenceSystem.Cartesian, 1, 1 );
        String key = "location";
        node1.setProperty( key, point );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( point, property );
    }

    @Test
    public void testPointTypeWithOneOtherProperty()
    {
        Point point = Values.pointValue( CoordinateReferenceSystem.Cartesian, 1, 1 );
        String key = "location";
        node1.setProperty( "prop1", 1 );
        node1.setProperty( key, point );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( point, property );
    }

    @Test
    public void testPointTypeWithTwoOtherProperties()
    {
        Point point = Values.pointValue( CoordinateReferenceSystem.Cartesian, 1, 1 );
        String key = "location";
        node1.setProperty( "prop1", 1 );
        node1.setProperty( "prop2", 2 );
        node1.setProperty( key, point );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( point, property );
    }

    @Test
    public void test3DPointType()
    {
        Point point = Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 1, 1, 1 );
        String key = "location";
        node1.setProperty( key, point );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( point, property );
    }

    @Test
    public void test4DPointType()
    {
        thrown.expect(Exception.class);
        node1.setProperty( "location", Values.unsafePointValue( CoordinateReferenceSystem.Cartesian, 1, 1, 1, 1 ) );
        newTransaction();
    }

    @Test
    public void testPointArray()
    {
        Point[] array = new Point[]{Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 1, 1, 1 ),
                                    Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 2, 1, 3 )};
        String key = "testpointarray";
        node1.setProperty( key, array );
        newTransaction();

        Point[] propertyValue = null;
        propertyValue = (Point[]) node1.getProperty( key );
        assertEquals( array.length, propertyValue.length );
        for ( int i = 0; i < array.length; i++ )
        {
            assertEquals( array[i], propertyValue[i] );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testDateTypeSmallEpochDay()
    {
        LocalDate date = DateValue.date( 2018, 1, 31 ).asObjectCopy();
        String key = "dt";
        node1.setProperty( key, date );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( date, property );
    }

    @Test
    public void testDateTypeLargeEpochDay()
    {
        LocalDate date = DateValue.epochDate( 2147483648L ).asObjectCopy();
        String key = "dt";
        node1.setProperty( key, date );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( date, property );
    }

    @Test
    public void testDateArray()
    {
        LocalDate[] array = new LocalDate[]{DateValue.date( 2018, 1, 31 ).asObjectCopy(), DateValue.epochDate( 2147483648L ).asObjectCopy()};
        String key = "testarray";
        node1.setProperty( key, array );
        newTransaction();

        LocalDate[] propertyValue = (LocalDate[]) node1.getProperty( key );
        assertEquals( array.length, propertyValue.length );
        for ( int i = 0; i < array.length; i++ )
        {
            assertEquals( array[i], propertyValue[i] );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testLocalTimeTypeSmallNano()
    {
        LocalTime time = LocalTimeValue.localTime( 0, 0, 0, 37 ).asObjectCopy();
        String key = "dt";
        node1.setProperty( key, time );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( time, property );
    }

    @Test
    public void testLocalTimeTypeLargeNano()
    {
        LocalTime time = LocalTimeValue.localTime( 0, 0, 13, 37 ).asObjectCopy();
        String key = "dt";
        node1.setProperty( key, time );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( time, property );
    }

    @Test
    public void testLocalTimeArray()
    {
        LocalTime[] array = new LocalTime[]{LocalTimeValue.localTime( 0, 0, 0, 37 ).asObjectCopy(), LocalTimeValue.localTime( 0, 0, 13, 37 ).asObjectCopy()};
        String key = "testarray";
        node1.setProperty( key, array );
        newTransaction();

        LocalTime[] propertyValue = (LocalTime[]) node1.getProperty( key );
        assertEquals( array.length, propertyValue.length );
        for ( int i = 0; i < array.length; i++ )
        {
            assertEquals( array[i], propertyValue[i] );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testLocalDateTimeType()
    {
        LocalDateTime dateTime = LocalDateTimeValue.localDateTime( 1991, 1, 1, 0, 0, 13, 37 ).asObjectCopy();
        String key = "dt";
        node1.setProperty( key, dateTime );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( dateTime, property );
    }

    @Test
    public void testLocalDateTimeArray()
    {
        LocalDateTime[] array = new LocalDateTime[]{LocalDateTimeValue.localDateTime( 1991, 1, 1, 0, 0, 13, 37 ).asObjectCopy(),
                LocalDateTimeValue.localDateTime( 1992, 2, 28, 1, 15, 0, 4000 ).asObjectCopy()};
        String key = "testarray";
        node1.setProperty( key, array );
        newTransaction();

        LocalDateTime[] propertyValue = (LocalDateTime[]) node1.getProperty( key );
        assertEquals( array.length, propertyValue.length );
        for ( int i = 0; i < array.length; i++ )
        {
            assertEquals( array[i], propertyValue[i] );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testTimeType()
    {
        OffsetTime time = TimeValue.time( 23, 11, 8, 0, "+17:59" ).asObjectCopy();
        String key = "dt";
        node1.setProperty( key, time );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( time, property );
    }

    @Test
    public void testTimeArray()
    {
        String key = "testarray";

        // array sizes 1 through 4
        for ( OffsetTime[] array : new OffsetTime[][]{new OffsetTime[]{TimeValue.time( 23, 11, 8, 0, "+17:59" ).asObjectCopy()},
                new OffsetTime[]{TimeValue.time( 23, 11, 8, 0, "+17:59" ).asObjectCopy(), TimeValue.time( 14, 34, 55, 3478, "+02:00" ).asObjectCopy()},
                new OffsetTime[]{TimeValue.time( 23, 11, 8, 0, "+17:59" ).asObjectCopy(), TimeValue.time( 14, 34, 55, 3478, "+02:00" ).asObjectCopy(),
                        TimeValue.time( 0, 17, 20, 783478, "-03:00" ).asObjectCopy()},
                new OffsetTime[]{TimeValue.time( 23, 11, 8, 0, "+17:59" ).asObjectCopy(), TimeValue.time( 14, 34, 55, 3478, "+02:00" ).asObjectCopy(),
                        TimeValue.time( 0, 17, 20, 783478, "-03:00" ).asObjectCopy(), TimeValue.time( 1, 1, 1, 1, "-01:00" ).asObjectCopy()}} )
        {
            node1.setProperty( key, array );
            newTransaction();

            OffsetTime[] propertyValue = (OffsetTime[]) node1.getProperty( key );
            assertEquals( array.length, propertyValue.length );
            for ( int i = 0; i < array.length; i++ )
            {
                assertEquals( array[i], propertyValue[i] );
            }
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testDurationType()
    {
        TemporalAmount duration = DurationValue.duration( 57, 57, 57, 57 ).asObjectCopy();
        String key = "dt";
        node1.setProperty( key, duration );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( duration, property );
    }

    @Test
    public void testDurationArray()
    {
        TemporalAmount[] array = new TemporalAmount[]{DurationValue.duration( 57, 57, 57, 57 ).asObjectCopy(),
                DurationValue.duration( -40, -189, -6247, -1 ).asObjectCopy()};
        String key = "testarray";
        node1.setProperty( key, array );
        newTransaction();

        TemporalAmount[] propertyValue = (TemporalAmount[]) node1.getProperty( key );
        assertEquals( array.length, propertyValue.length );
        for ( int i = 0; i < array.length; i++ )
        {
            assertEquals( array[i], propertyValue[i] );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testDateTimeTypeWithZoneOffset()
    {
        DateTimeValue dateTime = DateTimeValue.datetime( 1991, 1, 1, 0, 0, 13, 37, "+01:00" );
        String key = "dt";
        node1.setProperty( key, dateTime );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( dateTime.asObjectCopy(), property );
    }

    @Test
    public void testDateTimeArrayWithZoneOffset()
    {
        ZonedDateTime[] array = new ZonedDateTime[]{DateTimeValue.datetime( 1991, 1, 1, 0, 0, 13, 37, "-01:00" ).asObjectCopy(),
                DateTimeValue.datetime( 1992, 2, 28, 1, 15, 0, 4000, "+11:00" ).asObjectCopy()};
        String key = "testarray";
        node1.setProperty( key, array );
        newTransaction();

        ZonedDateTime[] propertyValue = (ZonedDateTime[]) node1.getProperty( key );
        assertEquals( array.length, propertyValue.length );
        for ( int i = 0; i < array.length; i++ )
        {
            assertEquals( array[i], propertyValue[i] );
        }

        node1.removeProperty( key );
        newTransaction();

        assertTrue( !node1.hasProperty( key ) );
    }

    @Test
    public void testDateTimeTypeWithZoneId()
    {
        DateTimeValue dateTime = DateTimeValue.datetime( 1991, 1, 1, 0, 0, 13, 37, ZoneId.of( "Europe/Stockholm" ) );
        String key = "dt";
        node1.setProperty( key, dateTime );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( dateTime.asObjectCopy(), property );
    }

    @Test
    public void testDateTimeArrayWithZoneOffsetAndZoneID()
    {
        ZonedDateTime[] array = new ZonedDateTime[]{DateTimeValue.datetime( 1991, 1, 1, 0, 0, 13, 37, "-01:00" ).asObjectCopy(),
                DateTimeValue.datetime( 1992, 2, 28, 1, 15, 0, 4000, "+11:00" ).asObjectCopy(),
                DateTimeValue.datetime( 1992, 2, 28, 1, 15, 0, 4000, ZoneId.of( "Europe/Stockholm" ) ).asObjectCopy()};
        String key = "testarray";
        node1.setProperty( key, array );
        newTransaction();

        ZonedDateTime[] propertyValue = (ZonedDateTime[]) node1.getProperty( key );
        assertEquals( array.length, propertyValue.length );
        for ( int i = 0; i < array.length; i++ )
        {
            assertEquals( array[i], propertyValue[i] );
        }

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

        int[] propertyValue = null;
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
            assertEquals( array2[i], Integer.valueOf( propertyValue[i] ) );
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

        short[] propertyValue = null;
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
            assertEquals( array2[i], Short.valueOf( propertyValue[i] ) );
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

        String[] propertyValue = null;
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

        boolean[] propertyValue = null;
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
            assertEquals( array2[i], propertyValue[i] );
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

        double[] propertyValue = null;
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

        float[] propertyValue = null;
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
        Long[] array2 = new Long[] { 6L, 7L, 8L };
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
            assertEquals( array2[i], Long.valueOf( propertyValue[i] ) );
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
            assertEquals( array2[i], Byte.valueOf( propertyValue[i] ) );
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
    public void testEmptyString()
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
    public void shouldNotBeAbleToPoisonBooleanArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new boolean[] {false, false, false}, true );
    }

    @Test
    public void shouldNotBeAbleToPoisonByteArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new byte[] {0, 0, 0}, (byte)1 );
    }

    @Test
    public void shouldNotBeAbleToPoisonShortArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new short[] {0, 0, 0}, (short)1 );
    }

    @Test
    public void shouldNotBeAbleToPoisonIntArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new int[] {0, 0, 0}, 1 );
    }

    @Test
    public void shouldNotBeAbleToPoisonLongArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new long[] {0, 0, 0}, 1L );
    }

    @Test
    public void shouldNotBeAbleToPoisonFloatArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new float[] {0F, 0F, 0F}, 1F );
    }

    @Test
    public void shouldNotBeAbleToPoisonDoubleArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new double[] {0D, 0D, 0D}, 1D );
    }

    @Test
    public void shouldNotBeAbleToPoisonCharArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new char[] {'0', '0', '0'}, '1' );
    }

    @Test
    public void shouldNotBeAbleToPoisonStringArrayProperty()
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
    public void shouldNotBeAbleToPoisonVeryLongBooleanArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Boolean.TYPE ), true );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongByteArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Byte.TYPE ), (byte)1 );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongShortArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Short.TYPE ), (short)1 );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongIntArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Integer.TYPE ), 1 );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongLongArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Long.TYPE ), 1L );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongFloatArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Float.TYPE ), 1F );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongDoubleArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Double.TYPE ), 1D );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongCharArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Character.TYPE ), '1' );
    }

    @Test
    public void shouldNotBeAbleToPoisonVeryLongStringArrayProperty()
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
                format( "Expected %s, but was %s", Strings.prettyPrint( value ), Strings.prettyPrint( readValue ) ),
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
                format( "Expected %s, but was %s", Strings.prettyPrint( value ), Strings.prettyPrint( readValue ) ),
                ArrayUtil.equals( value, node1.getProperty( key ) ) );
    }
}
