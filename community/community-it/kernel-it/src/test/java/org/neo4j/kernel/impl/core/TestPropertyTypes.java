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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.helpers.Strings;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPropertyTypes extends AbstractNeo4jTestCase
{
    private Node node1;

    @BeforeEach
    void createInitialNode()
    {
        node1 = getGraphDb().createNode();
    }

    @Test
    void testDoubleType()
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
        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testFloatType()
    {
        Float fValue = 45.678f;
        String key = "testfloat";
        node1.setProperty( key, fValue );
        newTransaction();

        Float propertyValue = (Float) node1.getProperty( key );
        assertEquals( fValue, propertyValue );

        fValue = 5684.3243f;
        node1.setProperty( key, fValue );
        newTransaction();

        propertyValue = (Float) node1.getProperty( key );
        assertEquals( fValue, propertyValue );

        node1.removeProperty( key );
        newTransaction();

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testLongType()
    {
        Long lValue = System.currentTimeMillis();
        String key = "testlong";
        node1.setProperty( key, lValue );
        newTransaction();

        Long propertyValue = (Long) node1.getProperty( key );
        assertEquals( lValue, propertyValue );

        lValue = System.currentTimeMillis();
        node1.setProperty( key, lValue );
        newTransaction();

        propertyValue = (Long) node1.getProperty( key );
        assertEquals( lValue, propertyValue );

        node1.removeProperty( key );
        newTransaction();

        assertFalse( node1.hasProperty( key ) );

        node1.setProperty( "other", 123L );
        assertEquals( 123L, node1.getProperty( "other" ) );
        newTransaction();
        assertEquals( 123L, node1.getProperty( "other" ) );
    }

    @Test
    void testIntType()
    {
        int time = (int)System.currentTimeMillis();
        Integer iValue = time;
        String key = "testing";
        node1.setProperty( key, iValue );
        newTransaction();

        Integer propertyValue = (Integer) node1.getProperty( key );
        assertEquals( iValue, propertyValue );

        iValue = (int) System.currentTimeMillis();
        node1.setProperty( key, iValue );
        newTransaction();

        propertyValue = (Integer) node1.getProperty( key );
        assertEquals( iValue, propertyValue );

        node1.removeProperty( key );
        newTransaction();

        assertFalse( node1.hasProperty( key ) );

        node1.setProperty( "other", 123L );
        assertEquals( 123L, node1.getProperty( "other" ) );
        newTransaction();
        assertEquals( 123L, node1.getProperty( "other" ) );
    }

    @Test
    void testByteType()
    {
        byte b = (byte) 177;
        Byte bValue = b;
        String key = "testbyte";
        node1.setProperty( key, bValue );
        newTransaction();

        Byte propertyValue = (Byte) node1.getProperty( key );
        assertEquals( bValue, propertyValue );

        bValue = (byte) 200;
        node1.setProperty( key, bValue );
        newTransaction();

        propertyValue = (Byte) node1.getProperty( key );
        assertEquals( bValue, propertyValue );

        node1.removeProperty( key );
        newTransaction();

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testShortType()
    {
        Short sValue = (short) 453;
        String key = "testshort";
        node1.setProperty( key, sValue );
        newTransaction();

        Short propertyValue = (Short) node1.getProperty( key );
        assertEquals( sValue, propertyValue );

        sValue = (short) 5335;
        node1.setProperty( key, sValue );
        newTransaction();

        propertyValue = (Short) node1.getProperty( key );
        assertEquals( sValue, propertyValue );

        node1.removeProperty( key );
        newTransaction();

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testCharType()
    {
        Character cValue = 'c';
        String key = "testchar";
        node1.setProperty( key, cValue );
        newTransaction();

        Character propertyValue = (Character) node1.getProperty( key );
        assertEquals( cValue, propertyValue );

        cValue = 'd';
        node1.setProperty( key, cValue );
        newTransaction();

        propertyValue = (Character) node1.getProperty( key );
        assertEquals( cValue, propertyValue );

        node1.removeProperty( key );
        newTransaction();

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testBooleanType()
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testPointType()
    {
        Point point = Values.pointValue( CoordinateReferenceSystem.Cartesian, 1, 1 );
        String key = "location";
        node1.setProperty( key, point );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( point, property );
    }

    @Test
    void testPointTypeWithOneOtherProperty()
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
    void testPointTypeWithTwoOtherProperties()
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
    void test3DPointType()
    {
        Point point = Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 1, 1, 1 );
        String key = "location";
        node1.setProperty( key, point );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( point, property );
    }

    @Test
    void test4DPointType()
    {
        node1.setProperty( "location", Values.unsafePointValue( CoordinateReferenceSystem.Cartesian, 1, 1, 1, 1 ) );
        assertThrows( TransactionFailureException.class, () -> commit() );
    }

    @Test
    void testPointArray()
    {
        Point[] array = {Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 1, 1, 1 ),
                                    Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 2, 1, 3 )};
        String key = "testpointarray";
        node1.setProperty( key, array );
        newTransaction();

        Point[] propertyValue = (Point[]) node1.getProperty( key );
        assertEquals( array.length, propertyValue.length );
        for ( int i = 0; i < array.length; i++ )
        {
            assertEquals( array[i], propertyValue[i] );
        }

        node1.removeProperty( key );
        newTransaction();

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testDateTypeSmallEpochDay()
    {
        LocalDate date = DateValue.date( 2018, 1, 31 ).asObjectCopy();
        String key = "dt";
        node1.setProperty( key, date );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( date, property );
    }

    @Test
    void testDateTypeLargeEpochDay()
    {
        LocalDate date = DateValue.epochDate( 2147483648L ).asObjectCopy();
        String key = "dt";
        node1.setProperty( key, date );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( date, property );
    }

    @Test
    void testDateArray()
    {
        LocalDate[] array = {DateValue.date( 2018, 1, 31 ).asObjectCopy(), DateValue.epochDate( 2147483648L ).asObjectCopy()};
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testLocalTimeTypeSmallNano()
    {
        LocalTime time = LocalTimeValue.localTime( 0, 0, 0, 37 ).asObjectCopy();
        String key = "dt";
        node1.setProperty( key, time );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( time, property );
    }

    @Test
    void testLocalTimeTypeLargeNano()
    {
        LocalTime time = LocalTimeValue.localTime( 0, 0, 13, 37 ).asObjectCopy();
        String key = "dt";
        node1.setProperty( key, time );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( time, property );
    }

    @Test
    void testLocalTimeArray()
    {
        LocalTime[] array = {LocalTimeValue.localTime( 0, 0, 0, 37 ).asObjectCopy(), LocalTimeValue.localTime( 0, 0, 13, 37 ).asObjectCopy()};
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testLocalDateTimeType()
    {
        LocalDateTime dateTime = LocalDateTimeValue.localDateTime( 1991, 1, 1, 0, 0, 13, 37 ).asObjectCopy();
        String key = "dt";
        node1.setProperty( key, dateTime );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( dateTime, property );
    }

    @Test
    void testLocalDateTimeArray()
    {
        LocalDateTime[] array = {LocalDateTimeValue.localDateTime( 1991, 1, 1, 0, 0, 13, 37 ).asObjectCopy(),
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testTimeType()
    {
        OffsetTime time = TimeValue.time( 23, 11, 8, 0, "+17:59" ).asObjectCopy();
        String key = "dt";
        node1.setProperty( key, time );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( time, property );
    }

    @Test
    void testTimeArray()
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testDurationType()
    {
        TemporalAmount duration = DurationValue.duration( 57, 57, 57, 57 ).asObjectCopy();
        String key = "dt";
        node1.setProperty( key, duration );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( duration, property );
    }

    @Test
    void testDurationArray()
    {
        TemporalAmount[] array = {DurationValue.duration( 57, 57, 57, 57 ).asObjectCopy(),
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testDateTimeTypeWithZoneOffset()
    {
        DateTimeValue dateTime = DateTimeValue.datetime( 1991, 1, 1, 0, 0, 13, 37, "+01:00" );
        String key = "dt";
        node1.setProperty( key, dateTime );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( dateTime.asObjectCopy(), property );
    }

    @Test
    void testDateTimeArrayWithZoneOffset()
    {
        ZonedDateTime[] array = {DateTimeValue.datetime( 1991, 1, 1, 0, 0, 13, 37, "-01:00" ).asObjectCopy(),
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testDateTimeTypeWithZoneId()
    {
        DateTimeValue dateTime = DateTimeValue.datetime( 1991, 1, 1, 0, 0, 13, 37, ZoneId.of( "Europe/Stockholm" ) );
        String key = "dt";
        node1.setProperty( key, dateTime );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( dateTime.asObjectCopy(), property );
    }

    @Test
    void testDateTimeArrayWithZoneOffsetAndZoneID()
    {
        ZonedDateTime[] array = {DateTimeValue.datetime( 1991, 1, 1, 0, 0, 13, 37, "-01:00" ).asObjectCopy(),
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testIntArray()
    {
        int[] array1 = { 1, 2, 3, 4, 5 };
        Integer[] array2 = { 6, 7, 8 };
        String key = "testintarray";
        node1.setProperty( key, array1 );
        newTransaction();

        int[] propertyValue = (int[]) node1.getProperty( key );
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testShortArray()
    {
        short[] array1 = { 1, 2, 3, 4, 5 };
        Short[] array2 = { 6, 7, 8 };
        String key = "testintarray";
        node1.setProperty( key, array1 );
        newTransaction();

        short[] propertyValue = (short[]) node1.getProperty( key );
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testStringArray()
    {
        String[] array1 = { "a", "b", "c", "d", "e" };
        String[] array2 = { "ff", "gg", "hh" };
        String key = "teststringarray";
        node1.setProperty( key, array1 );
        newTransaction();

        String[] propertyValue = (String[]) node1.getProperty( key );
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testBooleanArray()
    {
        boolean[] array1 = { true, false, true, false, true };
        Boolean[] array2 = { false, true, false };
        String key = "testboolarray";
        node1.setProperty( key, array1 );
        newTransaction();

        boolean[] propertyValue = (boolean[]) node1.getProperty( key );
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testDoubleArray()
    {
        double[] array1 = { 1.0, 2.0, 3.0, 4.0, 5.0 };
        Double[] array2 = { 6.0, 7.0, 8.0 };
        String key = "testdoublearray";
        node1.setProperty( key, array1 );
        newTransaction();

        double[] propertyValue = (double[]) node1.getProperty( key );
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testFloatArray()
    {
        float[] array1 = { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f };
        Float[] array2 = { 6.0f, 7.0f, 8.0f };
        String key = "testfloatarray";
        node1.setProperty( key, array1 );
        newTransaction();

        float[] propertyValue = (float[]) node1.getProperty( key );
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testLongArray()
    {
        long[] array1 = { 1, 2, 3, 4, 5 };
        Long[] array2 = { 6L, 7L, 8L };
        String key = "testlongarray";
        node1.setProperty( key, array1 );
        newTransaction();

        long[] propertyValue = (long[]) node1.getProperty( key );
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testByteArray()
    {
        byte[] array1 = { 1, 2, 3, 4, 5 };
        Byte[] array2 = { 6, 7, 8 };
        String key = "testbytearray";
        node1.setProperty( key, array1 );
        newTransaction();

        byte[] propertyValue = (byte[]) node1.getProperty( key );
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testCharArray()
    {
        char[] array1 = { '1', '2', '3', '4', '5' };
        Character[] array2 = { '6', '7', '8' };
        String key = "testchararray";
        node1.setProperty( key, array1 );
        newTransaction();

        char[] propertyValue = (char[]) node1.getProperty( key );
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

        assertFalse( node1.hasProperty( key ) );
    }

    @Test
    void testEmptyString()
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
    void shouldNotBeAbleToPoisonBooleanArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new boolean[] {false, false, false}, true );
    }

    @Test
    void shouldNotBeAbleToPoisonByteArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new byte[] {0, 0, 0}, (byte)1 );
    }

    @Test
    void shouldNotBeAbleToPoisonShortArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new short[] {0, 0, 0}, (short)1 );
    }

    @Test
    void shouldNotBeAbleToPoisonIntArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new int[] {0, 0, 0}, 1 );
    }

    @Test
    void shouldNotBeAbleToPoisonLongArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new long[] {0, 0, 0}, 1L );
    }

    @Test
    void shouldNotBeAbleToPoisonFloatArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new float[] {0F, 0F, 0F}, 1F );
    }

    @Test
    void shouldNotBeAbleToPoisonDoubleArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new double[] {0D, 0D, 0D}, 1D );
    }

    @Test
    void shouldNotBeAbleToPoisonCharArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( new char[] {'0', '0', '0'}, '1' );
    }

    @Test
    void shouldNotBeAbleToPoisonStringArrayProperty()
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
    void shouldNotBeAbleToPoisonVeryLongBooleanArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Boolean.TYPE ), true );
    }

    @Test
    void shouldNotBeAbleToPoisonVeryLongByteArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Byte.TYPE ), (byte)1 );
    }

    @Test
    void shouldNotBeAbleToPoisonVeryLongShortArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Short.TYPE ), (short)1 );
    }

    @Test
    void shouldNotBeAbleToPoisonVeryLongIntArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Integer.TYPE ), 1 );
    }

    @Test
    void shouldNotBeAbleToPoisonVeryLongLongArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Long.TYPE ), 1L );
    }

    @Test
    void shouldNotBeAbleToPoisonVeryLongFloatArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Float.TYPE ), 1F );
    }

    @Test
    void shouldNotBeAbleToPoisonVeryLongDoubleArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Double.TYPE ), 1D );
    }

    @Test
    void shouldNotBeAbleToPoisonVeryLongCharArrayProperty()
    {
        shouldNotBeAbleToPoisonArrayProperty( veryLongArray( Character.TYPE ), '1' );
    }

    @Test
    void shouldNotBeAbleToPoisonVeryLongStringArrayProperty()
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
            ArrayUtil.equals( value, node1.getProperty( key ) ),
            format( "Expected %s, but was %s", Strings.prettyPrint( value ), Strings.prettyPrint( readValue ) ) );
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
            ArrayUtil.equals( value, node1.getProperty( key ) ),
            format( "Expected %s, but was %s", Strings.prettyPrint( value ), Strings.prettyPrint( readValue ) ) );
    }
}
