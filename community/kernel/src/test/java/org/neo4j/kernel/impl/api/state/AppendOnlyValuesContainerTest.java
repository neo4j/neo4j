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
package org.neo4j.kernel.impl.api.state;

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.api.tuple.primitive.ObjectLongPair;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.impl.util.collection.CachingOffHeapBlockAllocator;
import org.neo4j.kernel.impl.util.collection.OffHeapMemoryAllocator;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BOOLEAN_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_CHAR_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_DOUBLE_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_FLOAT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_SHORT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples.pair;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringValue;

@ExtendWith( RandomExtension.class )
class AppendOnlyValuesContainerTest
{
    @Inject
    private RandomRule rnd;

    private final CachingOffHeapBlockAllocator blockAllocator = new CachingOffHeapBlockAllocator();
    private final MemoryAllocationTracker memoryTracker = new LocalMemoryTracker();

    private final AppendOnlyValuesContainer container = new AppendOnlyValuesContainer( new OffHeapMemoryAllocator( memoryTracker, blockAllocator ) );

    @AfterAll
    static void afterAll()
    {
    }

    @AfterEach
    void afterEach()
    {
        container.close();
        assertEquals( 0, memoryTracker.usedDirectMemory(), "Got memory leak" );
        blockAllocator.release();
    }

    @TestFactory
    Stream<DynamicTest> addGet()
    {
        final List<Pair<String, Value[]>> inputs = asList(
                testInput( "NoValue", Function.identity(), NoValue.NO_VALUE ),

                testInput( "Boolean", Values::booleanValue, true, false, true, false ),
                testInput( "BooleanArray", Values::booleanArray, new boolean[] {false, true, false}, EMPTY_BOOLEAN_ARRAY ),

                testInput( "Byte", Values::byteValue, (byte) 0, (byte) 1, (byte) -1, Byte.MIN_VALUE, Byte.MAX_VALUE ),
                testInput( "ByteArray", Values::byteArray,
                        new byte[] {(byte) 0, (byte) 1, (byte) -1, Byte.MIN_VALUE, Byte.MAX_VALUE}, EMPTY_BYTE_ARRAY ),

                testInput( "Short", Values::shortValue, (short) 0, (short) 1, (short) -1, Short.MIN_VALUE, Short.MAX_VALUE ),
                testInput( "ShortArray", Values::shortArray,
                        new short[] {(short) 0, (short) 1, (short) -1, Short.MIN_VALUE, Short.MAX_VALUE}, EMPTY_SHORT_ARRAY ),

                testInput( "Char", Values::charValue, 'a', '\uFFFF', '∂', '©' ),
                testInput( "CharArray", Values::charArray, new char[] {'a', '\uFFFF', '∂', '©'}, EMPTY_CHAR_ARRAY ),

                testInput( "Int", Values::intValue, 0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE ),
                testInput( "IntArray", Values::intArray, new int[] {0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE}, EMPTY_INT_ARRAY ),

                testInput( "Long", Values::longValue, 0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE ),
                testInput( "LongArray", Values::longArray, new long[] {0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE}, EMPTY_LONG_ARRAY ),

                testInput( "Double", Values::doubleValue,
                        0.0, 1.0, -1.0, Double.MIN_VALUE, Double.MAX_VALUE, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY ),
                testInput( "DoubleArray", Values::doubleArray,
                        new double[] {0.0, 1.0, -1.0, Double.MIN_VALUE, Double.MAX_VALUE, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY},
                        EMPTY_DOUBLE_ARRAY ),

                testInput( "Float", Values::floatValue,
                        0.0f, 1.0f, -1.0f, Float.MIN_VALUE, Float.MAX_VALUE, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY ),
                testInput( "FloatArray", Values::floatArray,
                        new float[] {0.0f, 1.0f, -1.0f, Float.MIN_VALUE, Float.MAX_VALUE, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY},
                        EMPTY_FLOAT_ARRAY ),

                testInput( "String", Values::stringValue, "", "x", "foobar" ),
                testInput( "StringArray", Values::stringArray, new String[] {"", "x", "foobar"}, EMPTY_STRING_ARRAY ),

                testInput( "Point", input -> pointValue( input.getOne(), input.getTwo() ),
                        Tuples.pair( CoordinateReferenceSystem.WGS84, new double[] {1.0, 2.0} ),
                        Tuples.pair( CoordinateReferenceSystem.WGS84_3D, new double[] {1.0, 2.0, 3.0} ),
                        Tuples.pair( CoordinateReferenceSystem.Cartesian, new double[] {1.0, 2.0} ),
                        Tuples.pair( CoordinateReferenceSystem.Cartesian_3D, new double[] {1.0, 2.0, 3.0} )
                ),
                testInput( "PointArray", Values::pointArray,
                        new Point[] {
                                pointValue( CoordinateReferenceSystem.WGS84, 1.0, 2.0 ),
                                pointValue( CoordinateReferenceSystem.WGS84_3D, 1.0, 2.0, 3.0 ),
                                pointValue( CoordinateReferenceSystem.Cartesian, 1.0, 2.0 ),
                                pointValue( CoordinateReferenceSystem.Cartesian_3D, 1.0, 2.0, 3.0 )
                        }, new Point[0]
                ),

                testInput( "Duration", Values::durationValue, (TemporalAmount) Duration.parse( "P2DT3H4M" ), Period.parse( "P1Y2M3W4D" ) ),
                testInput( "DurationArray", Values::durationArray,
                        new TemporalAmount[] {Duration.parse( "P2DT3H4M" ), Period.parse( "P1Y2M3W4D" )}, new TemporalAmount[0] ),

                testInput( "Date", DateValue::date, LocalDate.now(), LocalDate.parse( "1977-05-25" ) ),
                testInput( "DateArray", Values::dateArray,
                        new LocalDate[] {LocalDate.now(), LocalDate.parse( "1977-05-25" )},
                        new LocalDate[0] ),

                testInput( "Time", TimeValue::time, OffsetTime.now(), OffsetTime.parse( "19:28:34.123+02:00" ) ),
                testInput( "TimeArray", Values::timeArray,
                        new OffsetTime[] {OffsetTime.now(), OffsetTime.parse( "19:28:34.123+02:00" )}, new OffsetTime[0] ),

                testInput( "LocalTime", LocalTimeValue::localTime, LocalTime.now(), LocalTime.parse( "19:28:34.123" ) ),
                testInput( "LocalTimeArray", Values::localTimeArray,
                        new LocalTime[] {LocalTime.now(), LocalTime.parse( "19:28:34.123" )}, new LocalTime[0] ),

                testInput( "LocalDateTime", LocalDateTimeValue::localDateTime, LocalDateTime.now(), LocalDateTime.parse( "1956-10-04T19:28:34.123" ) ),
                testInput( "LocalDateTimeArray", Values::localDateTimeArray,
                        new LocalDateTime[] {LocalDateTime.now(), LocalDateTime.parse( "1956-10-04T19:28:34.123" )}, new LocalDateTime[0] ),

                testInput( "DateTime", DateTimeValue::datetime,
                        ZonedDateTime.now(),
                        ZonedDateTime.parse( "1956-10-04T19:28:34.123+01:00[Europe/Paris]" ),
                        ZonedDateTime.parse( "1956-10-04T19:28:34.123+01:15" ),
                        ZonedDateTime.parse( "2018-09-13T16:12:16.12345+14:00[Pacific/Kiritimati]" ),
                        ZonedDateTime.parse( "2018-09-13T16:12:16.12345-12:00[Etc/GMT+12]" ),
                        ZonedDateTime.parse( "2018-09-13T16:12:16.12345-18:00" ),
                        ZonedDateTime.parse( "2018-09-13T16:12:16.12345+18:00" )
                ),
                testInput( "DateTimeArray", Values::dateTimeArray,
                        new ZonedDateTime[] {
                                ZonedDateTime.parse( "1956-10-04T19:28:34.123+01:00[Europe/Paris]" ),
                                ZonedDateTime.parse( "1956-10-04T19:28:34.123+01:15" ),
                                ZonedDateTime.parse( "2018-09-13T16:12:16.12345+14:00[Pacific/Kiritimati]" ),
                                ZonedDateTime.parse( "2018-09-13T16:12:16.12345-12:00[Etc/GMT+12]" ),
                                ZonedDateTime.parse( "2018-09-13T16:12:16.12345-18:00" ),
                                ZonedDateTime.parse( "2018-09-13T16:12:16.12345+18:00" )
                        },
                        new ZonedDateTime[0] )

        );

        return DynamicTest.stream( inputs.iterator(), Pair::getOne, pair ->
        {
            final Value[] values = pair.getTwo();
            final long[] refs = Arrays.stream( values ).mapToLong( container::add ).toArray();
            for ( int i = 0; i < values.length; i++ )
            {
                assertEquals( values[i], container.get( refs[i] ) );
            }
        } );
    }

    private static <T> Pair<String, Value[]> testInput( String name, Function<T, Value> ctor, T... values )
    {
        return Tuples.pair( name, Arrays.stream( values ).map( ctor ).toArray( Value[]::new ) );
    }

    @Test
    void getFailsOnInvalidRef()
    {
        final long ref = container.add( intValue( 42 ) );
        container.get( ref );
        assertThrows( IllegalArgumentException.class, () -> container.get( 128L ), "invalid chunk offset" );
        assertThrows( IllegalArgumentException.class, () -> container.get( 1L << 32 ), "invalid chunk index" );
    }

    @Test
    void remove()
    {
        final long ref = container.add( intValue( 42 ) );
        container.remove( ref );
        assertThrows( IllegalArgumentException.class, () -> container.get( ref ) );
    }

    @Test
    void valueSizeExceedsChunkSize()
    {
        final AppendOnlyValuesContainer container2 = new AppendOnlyValuesContainer( 4, new TestMemoryAllocator() );
        final long ref1 = container2.add( longValue( 42 ) );
        final long ref2 = container2.add( stringValue( "1234567890ABCDEF" ) );

        assertEquals( longValue( 42 ), container2.get( ref1 ) );
        assertEquals( stringValue( "1234567890ABCDEF" ), container2.get( ref2 ) );

        container2.close();
    }

    @Test
    void close()
    {
        final AppendOnlyValuesContainer container2 = new AppendOnlyValuesContainer( 4, new TestMemoryAllocator() );
        final long ref = container2.add( intValue( 42 ) );
        container2.close();
        assertThrows( IllegalStateException.class, () -> container2.add( intValue( 1 ) ) );
        assertThrows( IllegalStateException.class, () -> container2.get( ref ) );
        assertThrows( IllegalStateException.class, () -> container2.remove( ref ) );
        assertThrows( IllegalStateException.class, container2::close );
    }

    @Test
    void randomizedTest()
    {
        final int count = 10000 + rnd.nextInt( 1000 );

        final List<ObjectLongPair<Value>> valueRefPairs = new ArrayList<>();
        final MutableList<ObjectLongPair<Value>> toRemove = new FastList<>();

        for ( int i = 0; i < count; i++ )
        {
            final Value value = rnd.randomValues().nextValue();
            final long ref = container.add( value );
            final ObjectLongPair<Value> pair = pair( value, ref );
            if ( rnd.nextBoolean() )
            {
                toRemove.add( pair );
            }
            else
            {
                valueRefPairs.add( pair );
            }
        }

        toRemove.shuffleThis(rnd.random() );
        for ( final ObjectLongPair<Value> valueRefPair : toRemove )
        {
            final Value removed = container.remove( valueRefPair.getTwo() );
            assertEquals( valueRefPair.getOne(), removed );
            assertThrows( IllegalArgumentException.class, () -> container.remove( valueRefPair.getTwo() ) );
            assertThrows( IllegalArgumentException.class, () -> container.get( valueRefPair.getTwo() ) );
        }

        for ( final ObjectLongPair<Value> valueRefPair : valueRefPairs )
        {
            final Value actualValue = container.get( valueRefPair.getTwo() );
            assertEquals( valueRefPair.getOne(), actualValue );
        }
    }
}
