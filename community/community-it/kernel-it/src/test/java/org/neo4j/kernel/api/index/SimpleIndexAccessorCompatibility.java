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
package org.neo4j.kernel.api.index;

import org.apache.commons.lang3.ArrayUtils;
import org.hamcrest.Matchers;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.SimpleNodeValueClient;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.kernel.api.IndexQuery.exact;
import static org.neo4j.internal.kernel.api.IndexQuery.exists;
import static org.neo4j.internal.kernel.api.IndexQuery.range;
import static org.neo4j.internal.kernel.api.IndexQuery.stringContains;
import static org.neo4j.internal.kernel.api.IndexQuery.stringPrefix;
import static org.neo4j.internal.kernel.api.IndexQuery.stringSuffix;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.epochDate;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.stringValue;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " IndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public abstract class SimpleIndexAccessorCompatibility extends IndexAccessorCompatibility
{
    public SimpleIndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite, IndexDescriptor descriptor )
    {
        super( testSuite, descriptor );
    }

    // This behaviour is shared by General and Unique indexes

    @Test
    public void testIndexSeekByPrefix() throws Exception
    {
        updateAndCommit( asList(
                add( 1L, descriptor.schema(), "a" ),
                add( 2L, descriptor.schema(), "A" ),
                add( 3L, descriptor.schema(), "apa" ),
                add( 4L, descriptor.schema(), "apA" ),
                add( 5L, descriptor.schema(), "b" ) ) );

        assertThat( query( IndexQuery.stringPrefix( 1, stringValue( "a" ) ) ), equalTo( asList( 1L, 3L, 4L ) ) );
        assertThat( query( IndexQuery.stringPrefix( 1, stringValue( "A" ) ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( IndexQuery.stringPrefix( 1, stringValue( "ba" ) ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( IndexQuery.stringPrefix( 1, stringValue( "" ) ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
    }

    @Test
    public void testIndexSeekByPrefixOnNonStrings() throws Exception
    {
        updateAndCommit( asList(
                add( 1L, descriptor.schema(), "2a" ),
                add( 2L, descriptor.schema(), 2L ),
                add( 2L, descriptor.schema(), 20L ) ) );
        assertThat( query( IndexQuery.stringPrefix( 1, stringValue( "2" ) ) ), equalTo( singletonList( 1L ) ) );
    }

    @Test
    public void testIndexRangeSeekByDateTimeWithSneakyZones() throws Exception
    {
        DateTimeValue d1 = datetime( 9999, 100, ZoneId.of( "+18:00" ) );
        DateTimeValue d4 = datetime( 10000, 100, ZoneId.of( "UTC" ) );
        DateTimeValue d5 = datetime( 10000, 100, ZoneId.of( "+01:00" ) );
        DateTimeValue d6 = datetime( 10000, 100, ZoneId.of( "Europe/Stockholm" ) );
        DateTimeValue d7 = datetime( 10000, 100, ZoneId.of( "+03:00" ) );
        DateTimeValue d8 = datetime( 10000, 101, ZoneId.of( "UTC" ) );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), d1 ),
                add( 4L, descriptor.schema(), d4 ),
                add( 5L, descriptor.schema(), d5 ),
                add( 6L, descriptor.schema(), d6 ),
                add( 7L, descriptor.schema(), d7 ),
                add( 8L, descriptor.schema(), d8 )
            ) );

        assertThat( query( range( 1, d4, true, d7, true ) ), Matchers.contains( 4L, 5L, 6L, 7L ) );
    }

    @Test
    public void testIndexRangeSeekWithSpatial() throws Exception
    {
        Assume.assumeTrue( testSuite.supportsSpatial() );

        PointValue p1 = Values.pointValue( CoordinateReferenceSystem.WGS84, -180, -1 );
        PointValue p2 = Values.pointValue( CoordinateReferenceSystem.WGS84, -180, 1 );
        PointValue p3 = Values.pointValue( CoordinateReferenceSystem.WGS84, 0, 0 );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), p1 ),
                add( 2L, descriptor.schema(), p2 ),
                add( 3L, descriptor.schema(), p3 )
            ) );

        assertThat( query( range( 1, p1, true, p2, true ) ), Matchers.contains( 1L, 2L ) );
    }

    @Test
    public void shouldUpdateWithAllValues() throws Exception
    {
        // GIVEN
        List<IndexEntryUpdate<?>> updates = updates( valueSet1 );
        updateAndCommit( updates );

        // then
        int propertyKeyId = descriptor.schema().getPropertyId();
        for ( NodeAndValue entry : valueSet1 )
        {
            List<Long> result = query( IndexQuery.exact( propertyKeyId, entry.value ) );
            assertThat( result, equalTo( singletonList( entry.nodeId ) ) );
        }
    }

    @Test
    public void shouldScanAllValues() throws Exception
    {
        // GIVEN
        List<IndexEntryUpdate<?>> updates = updates( valueSet1 );
        updateAndCommit( updates );
        Long[] allNodes = valueSet1.stream().map( x -> x.nodeId ).toArray( Long[]::new );

        // THEN
        int propertyKeyId = descriptor.schema().getPropertyId();
        List<Long> result = query( IndexQuery.exists( propertyKeyId ) );
        assertThat( result, containsInAnyOrder( allNodes ) );
    }

    @Test
    public void testIndexRangeSeekByNumber() throws Exception
    {
        testIndexRangeSeek( () -> random.randomValues().nextNumberValue() );
    }

    @Test
    public void testIndexRangeSeekByText() throws Exception
    {
        testIndexRangeSeek( () -> random.randomValues().nextTextValue() );
    }

    @Test
    public void testIndexRangeSeekByChar() throws Exception
    {
        testIndexRangeSeek( () -> random.randomValues().nextCharValue() );
    }

    @Test
    public void testIndexRangeSeekByDateTime() throws Exception
    {
        testIndexRangeSeek( () -> random.randomValues().nextDateTimeValue() );
    }

    @Test
    public void testIndexRangeSeekByLocalDateTime() throws Exception
    {
        testIndexRangeSeek( () -> random.randomValues().nextLocalDateTimeValue() );
    }

    @Test
    public void testIndexRangeSeekByDate() throws Exception
    {
        testIndexRangeSeek( () -> random.randomValues().nextDateValue() );
    }

    @Test
    public void testIndexRangeSeekByTime() throws Exception
    {
        testIndexRangeSeek( () -> random.randomValues().nextTimeValue() );
    }

    @Test
    public void testIndexRangeSeekByLocalTime() throws Exception
    {
        testIndexRangeSeek( () -> random.randomValues().nextLocalTimeValue() );
    }

    @Test
    public void testIndexRangeSeekByDuration() throws Exception
    {
        testIndexRangeSeek( () -> random.randomValues().nextDuration() );
    }

    @Test
    public void testIndexRangeSeekByPeriod() throws Exception
    {
        testIndexRangeSeek( () -> random.randomValues().nextPeriod() );
    }

    // testIndexRangeSeekGeometry not present because geometry is not orderable
    // testIndexRangeSeekBoolean not present because test needs more than two possible values

    @Test
    public void testIndexRangeSeekByZonedDateTimeArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextDateTimeArray() );
    }

    @Test
    public void testIndexRangeSeekByLocalDateTimeArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextLocalDateTimeArray() );
    }

    @Test
    public void testIndexRangeSeekByDateArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextDateArray() );
    }

    @Test
    public void testIndexRangeSeekByZonedTimeArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextTimeArray() );
    }

    @Test
    public void testIndexRangeSeekByLocalTimeArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextLocalTimeArray() );
    }

    @Test
    public void testIndexRangeSeekByDurationArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextDurationArray() );
    }

    @Test
    public void testIndexRangeSeekByTextArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextBasicMultilingualPlaneTextArray() );
    }

    @Test
    public void testIndexRangeSeekByCharArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextCharArray() );
    }

    @Test
    public void testIndexRangeSeekByBooleanArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextBooleanArray() );
    }

    @Test
    public void testIndexRangeSeekByByteArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextByteArray() );
    }

    @Test
    public void testIndexRangeSeekByShortArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextShortArray() );
    }

    @Test
    public void testIndexRangeSeekByIntArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextIntArray() );
    }

    @Test
    public void testIndexRangeSeekByLongArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextLongArray() );
    }

    @Test
    public void testIndexRangeSeekByFloatArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextFloatArray() );
    }

    @Test
    public void testIndexRangeSeekByDoubleArray() throws Exception
    {
        testIndexRangeSeekArray( () -> random.randomValues().nextDoubleArray() );
    }

    private void testIndexRangeSeekArray( Supplier<ArrayValue> generator ) throws Exception
    {
        Assume.assumeTrue( testSuite.supportsGranularCompositeQueries() );
        testIndexRangeSeek( generator );
    }

    private void testIndexRangeSeek( Supplier<? extends Value> generator ) throws Exception
    {
        int count = random.nextInt( 5, 10 );
        List<Value> values = new ArrayList<>();
        List<IndexEntryUpdate<?>> updates = new ArrayList<>();
        Set<Value> duplicateCheck = new HashSet<>();
        for ( int i = 0; i < count; i++ )
        {
            Value value;
            do
            {
                value = generator.get();
            }
            while ( !duplicateCheck.add( value ) );
            values.add( value );
        }
        values.sort( Values.COMPARATOR );
        for ( int i = 0; i < count; i++ )
        {
            updates.add( add( i + 1, descriptor.schema(), values.get( i ) ) );
        }
        Collections.shuffle( updates ); // <- Don't rely on insert order

        updateAndCommit( updates );

        for ( int f = 0; f < values.size(); f++ )
        {
            for ( int t = f; t < values.size(); t++ )
            {
                Value from = values.get( f );
                Value to = values.get( t );
                for ( boolean fromInclusive : new boolean[] {true, false} )
                {
                    for ( boolean toInclusive : new boolean[] {true, false} )
                    {
                        assertThat( query( range( 1, from, fromInclusive, to, toInclusive ) ), equalTo( ids( f, fromInclusive, t, toInclusive ) ) );
                    }
                }
            }
        }
    }

    private List<Long> ids( int fromIndex, boolean fromInclusive, int toIndex, boolean toInclusive )
    {
        List<Long> ids = new ArrayList<>();
        int from = fromInclusive ? fromIndex : fromIndex + 1;
        int to = toInclusive ? toIndex : toIndex - 1;
        for ( int i = from; i <= to; i++ )
        {
            ids.add( (long) (i + 1) );
        }
        return ids;
    }

    @Test
    public void shouldRangeSeekInOrderAscendingNumber() throws Exception
    {
        Object o0 = 0;
        Object o1 = 1;
        Object o2 = 2;
        Object o3 = 3;
        Object o4 = 4;
        Object o5 = 5;
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingNumber() throws Exception
    {
        Object o0 = 0;
        Object o1 = 1;
        Object o2 = 2;
        Object o3 = 3;
        Object o4 = 4;
        Object o5 = 5;
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingString() throws Exception
    {
        Object o0 = "0";
        Object o1 = "1";
        Object o2 = "2";
        Object o3 = "3";
        Object o4 = "4";
        Object o5 = "5";
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingString() throws Exception
    {
        Object o0 = "0";
        Object o1 = "1";
        Object o2 = "2";
        Object o3 = "3";
        Object o4 = "4";
        Object o5 = "5";
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingDate() throws Exception
    {
        Object o0 = DateValue.epochDateRaw( 0 );
        Object o1 = DateValue.epochDateRaw( 1 );
        Object o2 = DateValue.epochDateRaw( 2 );
        Object o3 = DateValue.epochDateRaw( 3 );
        Object o4 = DateValue.epochDateRaw( 4 );
        Object o5 = DateValue.epochDateRaw( 5 );
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingDate() throws Exception
    {
        Object o0 = DateValue.epochDateRaw( 0 );
        Object o1 = DateValue.epochDateRaw( 1 );
        Object o2 = DateValue.epochDateRaw( 2 );
        Object o3 = DateValue.epochDateRaw( 3 );
        Object o4 = DateValue.epochDateRaw( 4 );
        Object o5 = DateValue.epochDateRaw( 5 );
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingLocalTime() throws Exception
    {
        Object o0 = LocalTimeValue.localTimeRaw( 0 );
        Object o1 = LocalTimeValue.localTimeRaw( 1 );
        Object o2 = LocalTimeValue.localTimeRaw( 2 );
        Object o3 = LocalTimeValue.localTimeRaw( 3 );
        Object o4 = LocalTimeValue.localTimeRaw( 4 );
        Object o5 = LocalTimeValue.localTimeRaw( 5 );
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingLocalTime() throws Exception
    {
        Object o0 = LocalTimeValue.localTimeRaw( 0 );
        Object o1 = LocalTimeValue.localTimeRaw( 1 );
        Object o2 = LocalTimeValue.localTimeRaw( 2 );
        Object o3 = LocalTimeValue.localTimeRaw( 3 );
        Object o4 = LocalTimeValue.localTimeRaw( 4 );
        Object o5 = LocalTimeValue.localTimeRaw( 5 );
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingTime() throws Exception
    {
        Object o0 = TimeValue.timeRaw( 0, ZoneOffset.ofHours( 0 ) );
        Object o1 = TimeValue.timeRaw( 1, ZoneOffset.ofHours( 0 ) );
        Object o2 = TimeValue.timeRaw( 2, ZoneOffset.ofHours( 0 ) );
        Object o3 = TimeValue.timeRaw( 3, ZoneOffset.ofHours( 0 ) );
        Object o4 = TimeValue.timeRaw( 4, ZoneOffset.ofHours( 0 ) );
        Object o5 = TimeValue.timeRaw( 5, ZoneOffset.ofHours( 0 ) );
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingTime() throws Exception
    {
        Object o0 = TimeValue.timeRaw( 0, ZoneOffset.ofHours( 0 ) );
        Object o1 = TimeValue.timeRaw( 1, ZoneOffset.ofHours( 0 ) );
        Object o2 = TimeValue.timeRaw( 2, ZoneOffset.ofHours( 0 ) );
        Object o3 = TimeValue.timeRaw( 3, ZoneOffset.ofHours( 0 ) );
        Object o4 = TimeValue.timeRaw( 4, ZoneOffset.ofHours( 0 ) );
        Object o5 = TimeValue.timeRaw( 5, ZoneOffset.ofHours( 0 ) );
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingLocalDateTime() throws Exception
    {
        Object o0 = LocalDateTimeValue.localDateTimeRaw( 10, 0 );
        Object o1 = LocalDateTimeValue.localDateTimeRaw( 10, 1 );
        Object o2 = LocalDateTimeValue.localDateTimeRaw( 10, 2 );
        Object o3 = LocalDateTimeValue.localDateTimeRaw( 10, 3 );
        Object o4 = LocalDateTimeValue.localDateTimeRaw( 10, 4 );
        Object o5 = LocalDateTimeValue.localDateTimeRaw( 10, 5 );
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingLocalDateTime() throws Exception
    {
        Object o0 = LocalDateTimeValue.localDateTimeRaw( 10, 0 );
        Object o1 = LocalDateTimeValue.localDateTimeRaw( 10, 1 );
        Object o2 = LocalDateTimeValue.localDateTimeRaw( 10, 2 );
        Object o3 = LocalDateTimeValue.localDateTimeRaw( 10, 3 );
        Object o4 = LocalDateTimeValue.localDateTimeRaw( 10, 4 );
        Object o5 = LocalDateTimeValue.localDateTimeRaw( 10, 5 );
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingDateTime() throws Exception
    {
        Object o0 = DateTimeValue.datetimeRaw( 1, 0, ZoneId.of( "UTC" ) );
        Object o1 = DateTimeValue.datetimeRaw( 1, 1, ZoneId.of( "UTC" ) );
        Object o2 = DateTimeValue.datetimeRaw( 1, 2, ZoneId.of( "UTC" ) );
        Object o3 = DateTimeValue.datetimeRaw( 1, 3, ZoneId.of( "UTC" ) );
        Object o4 = DateTimeValue.datetimeRaw( 1, 4, ZoneId.of( "UTC" ) );
        Object o5 = DateTimeValue.datetimeRaw( 1, 5, ZoneId.of( "UTC" ) );
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingDateTime() throws Exception
    {
        Object o0 = DateTimeValue.datetimeRaw( 1, 0, ZoneId.of( "UTC" ) );
        Object o1 = DateTimeValue.datetimeRaw( 1, 1, ZoneId.of( "UTC" ) );
        Object o2 = DateTimeValue.datetimeRaw( 1, 2, ZoneId.of( "UTC" ) );
        Object o3 = DateTimeValue.datetimeRaw( 1, 3, ZoneId.of( "UTC" ) );
        Object o4 = DateTimeValue.datetimeRaw( 1, 4, ZoneId.of( "UTC" ) );
        Object o5 = DateTimeValue.datetimeRaw( 1, 5, ZoneId.of( "UTC" ) );
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingDuration() throws Exception
    {
        Object o0 = Duration.ofMillis( 0 );
        Object o1 = Duration.ofMillis( 1 );
        Object o2 = Duration.ofMillis( 2 );
        Object o3 = Duration.ofMillis( 3 );
        Object o4 = Duration.ofMillis( 4 );
        Object o5 = Duration.ofMillis( 5 );
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingDuration() throws Exception
    {
        Object o0 = Duration.ofMillis( 0 );
        Object o1 = Duration.ofMillis( 1 );
        Object o2 = Duration.ofMillis( 2 );
        Object o3 = Duration.ofMillis( 3 );
        Object o4 = Duration.ofMillis( 4 );
        Object o5 = Duration.ofMillis( 5 );
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingNumberArray() throws Exception
    {
        Object o0 = new int[]{0};
        Object o1 = new int[]{1};
        Object o2 = new int[]{2};
        Object o3 = new int[]{3};
        Object o4 = new int[]{4};
        Object o5 = new int[]{5};
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingNumberArray() throws Exception
    {
        Object o0 = new int[]{0};
        Object o1 = new int[]{1};
        Object o2 = new int[]{2};
        Object o3 = new int[]{3};
        Object o4 = new int[]{4};
        Object o5 = new int[]{5};
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingStringArray() throws Exception
    {
        Object o0 = new String[]{"0"};
        Object o1 = new String[]{"1"};
        Object o2 = new String[]{"2"};
        Object o3 = new String[]{"3"};
        Object o4 = new String[]{"4"};
        Object o5 = new String[]{"5"};
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingStringArray() throws Exception
    {
        Object o0 = new String[]{"0"};
        Object o1 = new String[]{"1"};
        Object o2 = new String[]{"2"};
        Object o3 = new String[]{"3"};
        Object o4 = new String[]{"4"};
        Object o5 = new String[]{"5"};
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingBooleanArray() throws Exception
    {
        Object o0 = new boolean[]{false};
        Object o1 = new boolean[]{false, false};
        Object o2 = new boolean[]{false, true};
        Object o3 = new boolean[]{true};
        Object o4 = new boolean[]{true, false};
        Object o5 = new boolean[]{true, true};
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingBooleanArray() throws Exception
    {
        Object o0 = new boolean[]{false};
        Object o1 = new boolean[]{false, false};
        Object o2 = new boolean[]{false, true};
        Object o3 = new boolean[]{true};
        Object o4 = new boolean[]{true, false};
        Object o5 = new boolean[]{true, true};
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingDateTimeArray() throws Exception
    {
        Object o0 = new ZonedDateTime[]{ZonedDateTime.of( 10, 10, 10, 10, 10, 10, 0, ZoneId.of( "UTC" ) )};
        Object o1 = new ZonedDateTime[]{ZonedDateTime.of( 10, 10, 10, 10, 10, 10, 1, ZoneId.of( "UTC" ) )};
        Object o2 = new ZonedDateTime[]{ZonedDateTime.of( 10, 10, 10, 10, 10, 10, 2, ZoneId.of( "UTC" ) )};
        Object o3 = new ZonedDateTime[]{ZonedDateTime.of( 10, 10, 10, 10, 10, 10, 3, ZoneId.of( "UTC" ) )};
        Object o4 = new ZonedDateTime[]{ZonedDateTime.of( 10, 10, 10, 10, 10, 10, 4, ZoneId.of( "UTC" ) )};
        Object o5 = new ZonedDateTime[]{ZonedDateTime.of( 10, 10, 10, 10, 10, 10, 5, ZoneId.of( "UTC" ) )};
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingDateTimeArray() throws Exception
    {
        Object o0 = new ZonedDateTime[]{ZonedDateTime.of( 10, 10, 10, 10, 10, 10, 0, ZoneId.of( "UTC" ) )};
        Object o1 = new ZonedDateTime[]{ZonedDateTime.of( 10, 10, 10, 10, 10, 10, 1, ZoneId.of( "UTC" ) )};
        Object o2 = new ZonedDateTime[]{ZonedDateTime.of( 10, 10, 10, 10, 10, 10, 2, ZoneId.of( "UTC" ) )};
        Object o3 = new ZonedDateTime[]{ZonedDateTime.of( 10, 10, 10, 10, 10, 10, 3, ZoneId.of( "UTC" ) )};
        Object o4 = new ZonedDateTime[]{ZonedDateTime.of( 10, 10, 10, 10, 10, 10, 4, ZoneId.of( "UTC" ) )};
        Object o5 = new ZonedDateTime[]{ZonedDateTime.of( 10, 10, 10, 10, 10, 10, 5, ZoneId.of( "UTC" ) )};
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingLocalDateTimeArray() throws Exception
    {
        Object o0 = new LocalDateTime[]{LocalDateTime.of( 10, 10, 10, 10, 10, 10, 0 )};
        Object o1 = new LocalDateTime[]{LocalDateTime.of( 10, 10, 10, 10, 10, 10, 1 )};
        Object o2 = new LocalDateTime[]{LocalDateTime.of( 10, 10, 10, 10, 10, 10, 2 )};
        Object o3 = new LocalDateTime[]{LocalDateTime.of( 10, 10, 10, 10, 10, 10, 3 )};
        Object o4 = new LocalDateTime[]{LocalDateTime.of( 10, 10, 10, 10, 10, 10, 4 )};
        Object o5 = new LocalDateTime[]{LocalDateTime.of( 10, 10, 10, 10, 10, 10, 5 )};
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingLocalDateTimeArray() throws Exception
    {
        Object o0 = new LocalDateTime[]{LocalDateTime.of( 10, 10, 10, 10, 10, 10, 0 )};
        Object o1 = new LocalDateTime[]{LocalDateTime.of( 10, 10, 10, 10, 10, 10, 1 )};
        Object o2 = new LocalDateTime[]{LocalDateTime.of( 10, 10, 10, 10, 10, 10, 2 )};
        Object o3 = new LocalDateTime[]{LocalDateTime.of( 10, 10, 10, 10, 10, 10, 3 )};
        Object o4 = new LocalDateTime[]{LocalDateTime.of( 10, 10, 10, 10, 10, 10, 4 )};
        Object o5 = new LocalDateTime[]{LocalDateTime.of( 10, 10, 10, 10, 10, 10, 5 )};
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingTimeArray() throws Exception
    {
        Object o0 = new OffsetTime[]{OffsetTime.of( 10, 10, 10, 0, ZoneOffset.ofHours( 0 ) )};
        Object o1 = new OffsetTime[]{OffsetTime.of( 10, 10, 10, 1, ZoneOffset.ofHours( 0 ) )};
        Object o2 = new OffsetTime[]{OffsetTime.of( 10, 10, 10, 2, ZoneOffset.ofHours( 0 ) )};
        Object o3 = new OffsetTime[]{OffsetTime.of( 10, 10, 10, 3, ZoneOffset.ofHours( 0 ) )};
        Object o4 = new OffsetTime[]{OffsetTime.of( 10, 10, 10, 4, ZoneOffset.ofHours( 0 ) )};
        Object o5 = new OffsetTime[]{OffsetTime.of( 10, 10, 10, 5, ZoneOffset.ofHours( 0 ) )};
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingTimeArray() throws Exception
    {
        Object o0 = new OffsetTime[]{OffsetTime.of( 10, 10, 10, 0, ZoneOffset.ofHours( 0 ) )};
        Object o1 = new OffsetTime[]{OffsetTime.of( 10, 10, 10, 1, ZoneOffset.ofHours( 0 ) )};
        Object o2 = new OffsetTime[]{OffsetTime.of( 10, 10, 10, 2, ZoneOffset.ofHours( 0 ) )};
        Object o3 = new OffsetTime[]{OffsetTime.of( 10, 10, 10, 3, ZoneOffset.ofHours( 0 ) )};
        Object o4 = new OffsetTime[]{OffsetTime.of( 10, 10, 10, 4, ZoneOffset.ofHours( 0 ) )};
        Object o5 = new OffsetTime[]{OffsetTime.of( 10, 10, 10, 5, ZoneOffset.ofHours( 0 ) )};
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingDateArray() throws Exception
    {
        Object o0 = new LocalDate[]{LocalDate.of( 10, 10, 1 )};
        Object o1 = new LocalDate[]{LocalDate.of( 10, 10, 2 )};
        Object o2 = new LocalDate[]{LocalDate.of( 10, 10, 3 )};
        Object o3 = new LocalDate[]{LocalDate.of( 10, 10, 4 )};
        Object o4 = new LocalDate[]{LocalDate.of( 10, 10, 5 )};
        Object o5 = new LocalDate[]{LocalDate.of( 10, 10, 6 )};
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingDateArray() throws Exception
    {
        Object o0 = new LocalDate[]{LocalDate.of( 10, 10, 1 )};
        Object o1 = new LocalDate[]{LocalDate.of( 10, 10, 2 )};
        Object o2 = new LocalDate[]{LocalDate.of( 10, 10, 3 )};
        Object o3 = new LocalDate[]{LocalDate.of( 10, 10, 4 )};
        Object o4 = new LocalDate[]{LocalDate.of( 10, 10, 5 )};
        Object o5 = new LocalDate[]{LocalDate.of( 10, 10, 6 )};
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingLocalTimeArray() throws Exception
    {
        Object o0 = new LocalTime[]{LocalTime.of( 10, 0 )};
        Object o1 = new LocalTime[]{LocalTime.of( 10, 1  )};
        Object o2 = new LocalTime[]{LocalTime.of( 10, 2 )};
        Object o3 = new LocalTime[]{LocalTime.of( 10, 3 )};
        Object o4 = new LocalTime[]{LocalTime.of( 10, 4 )};
        Object o5 = new LocalTime[]{LocalTime.of( 10, 5 )};
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingLocalTimeArray() throws Exception
    {
        Object o0 = new LocalTime[]{LocalTime.of( 10, 0 )};
        Object o1 = new LocalTime[]{LocalTime.of( 10, 1 )};
        Object o2 = new LocalTime[]{LocalTime.of( 10, 2 )};
        Object o3 = new LocalTime[]{LocalTime.of( 10, 3 )};
        Object o4 = new LocalTime[]{LocalTime.of( 10, 4 )};
        Object o5 = new LocalTime[]{LocalTime.of( 10, 5 )};
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderAscendingDurationArray() throws Exception
    {
        Object o0 = new Duration[]{Duration.of( 0, ChronoUnit.SECONDS )};
        Object o1 = new Duration[]{Duration.of( 1, ChronoUnit.SECONDS )};
        Object o2 = new Duration[]{Duration.of( 2, ChronoUnit.SECONDS )};
        Object o3 = new Duration[]{Duration.of( 3, ChronoUnit.SECONDS )};
        Object o4 = new Duration[]{Duration.of( 4, ChronoUnit.SECONDS )};
        Object o5 = new Duration[]{Duration.of( 5, ChronoUnit.SECONDS )};
        shouldRangeSeekInOrder( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderDescendingDurationArray() throws Exception
    {
        Object o0 = new Duration[]{Duration.of( 0, ChronoUnit.SECONDS )};
        Object o1 = new Duration[]{Duration.of( 1, ChronoUnit.SECONDS )};
        Object o2 = new Duration[]{Duration.of( 2, ChronoUnit.SECONDS )};
        Object o3 = new Duration[]{Duration.of( 3, ChronoUnit.SECONDS )};
        Object o4 = new Duration[]{Duration.of( 4, ChronoUnit.SECONDS )};
        Object o5 = new Duration[]{Duration.of( 5, ChronoUnit.SECONDS )};
        shouldRangeSeekInOrder( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    private void shouldRangeSeekInOrder( IndexOrder order, Object o0, Object o1, Object o2, Object o3, Object o4, Object o5 ) throws Exception
    {
        IndexQuery range = range( 100, Values.of( o0 ), true, Values.of( o5 ), true );
        IndexOrder[] indexOrders = orderCapability( range );
        Assume.assumeTrue( "Assume support for order " + order, ArrayUtils.contains( indexOrders, order ) );

        updateAndCommit( asList(
                add( 1, descriptor.schema(), o0 ),
                add( 1, descriptor.schema(), o5 ),
                add( 1, descriptor.schema(), o1 ),
                add( 1, descriptor.schema(), o4 ),
                add( 1, descriptor.schema(), o2 ),
                add( 1, descriptor.schema(), o3 )
        ) );

        SimpleNodeValueClient client = new SimpleNodeValueClient();
        try ( AutoCloseable ignored = query( client, order, range ) )
        {
            List<Long> seenIds = assertClientReturnValuesInOrder( client, order );
            assertThat( seenIds.size(), equalTo( 6 ) );
        }
    }

    @Test
    public void shouldUpdateEntries() throws Exception
    {
        ValueType[] valueTypes = testSuite.supportedValueTypes();
        long entityId = random.nextLong( 1_000_000_000 );
        for ( ValueType valueType : valueTypes )
        {
            // given
            Value value = random.nextValue( valueType );
            updateAndCommit( singletonList( IndexEntryUpdate.add( entityId, descriptor.schema(), value ) ) );
            assertEquals( singletonList( entityId ), query( IndexQuery.exact( 0, value ) ) );

            // when
            Value newValue;
            do
            {
                newValue = random.nextValue( valueType );
            }
            while ( value.equals( newValue ) );
            updateAndCommit( singletonList( IndexEntryUpdate.change( entityId, descriptor.schema(), value, newValue ) ) );

            // then
            assertEquals( emptyList(), query( IndexQuery.exact( 0, value ) ) );
            assertEquals( singletonList( entityId ), query( IndexQuery.exact( 0, newValue ) ) );
        }
    }

    @Test
    public void shouldRemoveEntries() throws Exception
    {
        ValueType[] valueTypes = testSuite.supportedValueTypes();
        long entityId = random.nextLong( 1_000_000_000 );
        for ( ValueType valueType : valueTypes )
        {
            // given
            Value value = random.nextValue( valueType );
            updateAndCommit( singletonList( IndexEntryUpdate.add( entityId, descriptor.schema(), value ) ) );
            assertEquals( singletonList( entityId ), query( IndexQuery.exact( 0, value ) ) );

            // when
            updateAndCommit( singletonList( IndexEntryUpdate.remove( entityId, descriptor.schema(), value ) ) );

            // then
            assertTrue( query( IndexQuery.exact( 0, value ) ).isEmpty() );
        }
    }

    // This behaviour is expected by General indexes

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class General extends SimpleIndexAccessorCompatibility
    {
        public General( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, TestIndexDescriptorFactory.forLabel( 1000, 100 ) );
        }

        @Test
        public void closingAnOnlineIndexUpdaterMustNotThrowEvenIfItHasBeenFedConflictingData() throws Exception
        {
            // The reason is that we use and close IndexUpdaters in commit - not in prepare - and therefor
            // we cannot have them go around and throw exceptions, because that could potentially break
            // recovery.
            // Conflicting data can happen because of faulty data coercion. These faults are resolved by
            // the exact-match filtering we do on index seeks.

            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), "a" ),
                    add( 2L, descriptor.schema(), "a" ) ) );

            assertThat( query( exact( 1, "a" ) ), equalTo( asList( 1L, 2L ) ) );
        }

        @Test
        public void testIndexSeekAndScan() throws Exception
        {
            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), "a" ),
                    add( 2L, descriptor.schema(), "a" ),
                    add( 3L, descriptor.schema(), "b" ) ) );

            assertThat( query( exact( 1, "a" ) ), equalTo( asList( 1L, 2L ) ) );
            assertThat( query( exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        }

        @Test
        public void testIndexRangeSeekByNumberWithDuplicates() throws Exception
        {
            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), -5 ),
                    add( 2L, descriptor.schema(), -5 ),
                    add( 3L, descriptor.schema(), 0 ),
                    add( 4L, descriptor.schema(), 5 ),
                    add( 5L, descriptor.schema(), 5 ) ) );

            assertThat( query( range( 1, -5, true, 5, true ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
            assertThat( query( range( 1, -3, true, -1, true ) ), equalTo( EMPTY_LIST ) );
            assertThat( query( range( 1, -5, true, 4, true ) ), equalTo( asList( 1L, 2L, 3L ) ) );
            assertThat( query( range( 1, -4, true, 5, true ) ), equalTo( asList( 3L, 4L, 5L ) ) );
            assertThat( query( range( 1, -5, true, 5, true ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        }

        @Test
        public void testIndexRangeSeekByStringWithDuplicates() throws Exception
        {
            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), "Anna" ),
                    add( 2L, descriptor.schema(), "Anna" ),
                    add( 3L, descriptor.schema(), "Bob" ),
                    add( 4L, descriptor.schema(), "William" ),
                    add( 5L, descriptor.schema(), "William" ) ) );

            assertThat( query( range( 1, "Anna", false, "William", false ) ), equalTo( singletonList( 3L ) ) );
            assertThat( query( range( 1, "Arabella", false, "Bob", false ) ), equalTo( EMPTY_LIST ) );
            assertThat( query( range( 1, "Anna", true, "William", false ) ), equalTo( asList( 1L, 2L, 3L ) ) );
            assertThat( query( range( 1, "Anna", false, "William", true ) ), equalTo( asList( 3L, 4L, 5L ) ) );
            assertThat( query( range( 1, "Anna", true, "William", true ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        }

        @Test
        public void testIndexRangeSeekByDateWithDuplicates() throws Exception
        {
            testIndexRangeSeekWithDuplicates( epochDate( 100 ),
                                              epochDate( 101 ),
                                              epochDate( 200 ),
                                              epochDate( 300 ) );
        }

        @Test
        public void testIndexRangeSeekByLocalDateTimeWithDuplicates() throws Exception
        {
            testIndexRangeSeekWithDuplicates( localDateTime( 1000, 10 ),
                                              localDateTime( 1000, 11 ),
                                              localDateTime( 2000, 10 ),
                                              localDateTime( 3000, 10 ) );
        }

        @Test
        public void testIndexRangeSeekByDateTimeWithDuplicates() throws Exception
        {
            testIndexRangeSeekWithDuplicates( datetime( 1000, 10, UTC ),
                                              datetime( 1000, 11, UTC ),
                                              datetime( 2000, 10, UTC ),
                                              datetime( 3000, 10, UTC ) );
        }

        @Test
        public void testIndexRangeSeekByLocalTimeWithDuplicates() throws Exception
        {
            testIndexRangeSeekWithDuplicates( localTime( 1000 ),
                                              localTime( 1001 ),
                                              localTime( 2000 ),
                                              localTime( 3000 ) );
        }

        @Test
        public void testIndexRangeSeekByTimeWithDuplicates() throws Exception
        {
            testIndexRangeSeekWithDuplicates( time( 1000, UTC ),
                                              time( 1001, UTC ),
                                              time( 2000, UTC ),
                                              time( 3000, UTC ) );
        }

        @Test
        public void testIndexRangeSeekByTimeWithZonesAndDuplicates() throws Exception
        {
            testIndexRangeSeekWithDuplicates( time( 20, 31, 53, 4, ZoneOffset.of("+17:02") ),
                                              time( 20, 31, 54, 3, ZoneOffset.of("+17:02") ),
                                              time( 19, 31, 54, 2, UTC ),
                                              time( 18, 23, 27, 1, ZoneOffset.of("-18:00") ) );
        }

        @Test
        public void testIndexRangeSeekByDurationWithDuplicates() throws Exception
        {
            testIndexRangeSeekWithDuplicates( duration( 1, 1, 1, 1 ),
                                              duration( 1, 1, 1, 2 ),
                                              duration( 2, 1, 1, 1 ),
                                              duration( 3, 1, 1, 1 ) );
        }

        /**
         * Helper for testing range seeks. Takes 4 ordered sample values.
         */
        private <VALUE extends Value> void testIndexRangeSeekWithDuplicates( VALUE v1, VALUE v2, VALUE v3, VALUE v4 ) throws Exception
        {
            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), v1 ),
                    add( 2L, descriptor.schema(), v1 ),
                    add( 3L, descriptor.schema(), v3 ),
                    add( 4L, descriptor.schema(), v4 ),
                    add( 5L, descriptor.schema(), v4 ) ) );

            assertThat( query( range( 1, v1, false, v4, false ) ), equalTo( singletonList( 3L ) ) );
            assertThat( query( range( 1, v2, false, v3, false ) ), equalTo( EMPTY_LIST ) );
            assertThat( query( range( 1, v1, true, v4, false ) ), equalTo( asList( 1L, 2L, 3L ) ) );
            assertThat( query( range( 1, v1, false, v4, true ) ), equalTo( asList( 3L, 4L, 5L ) ) );
            assertThat( query( range( 1, v1, true, v4, true ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        }

        @Test
        public void testIndexRangeSeekByPrefixWithDuplicates() throws Exception
        {
            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), "a" ),
                    add( 2L, descriptor.schema(), "A" ),
                    add( 3L, descriptor.schema(), "apa" ),
                    add( 4L, descriptor.schema(), "apa" ),
                    add( 5L, descriptor.schema(), "apa" ) ) );

            assertThat( query( stringPrefix( 1, stringValue( "a" ) ) ), equalTo( asList( 1L, 3L, 4L, 5L ) ) );
            assertThat( query( stringPrefix( 1, stringValue( "apa" ) ) ), equalTo( asList( 3L, 4L, 5L ) ) );
        }

        @Test
        public void testIndexFullSearchWithDuplicates() throws Exception
        {
            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), "a" ),
                    add( 2L, descriptor.schema(), "A" ),
                    add( 3L, descriptor.schema(), "apa" ),
                    add( 4L, descriptor.schema(), "apa" ),
                    add( 5L, descriptor.schema(), "apalong" ) ) );

            assertThat( query( stringContains( 1, stringValue( "a" ) ) ), equalTo( asList( 1L, 3L, 4L, 5L ) ) );
            assertThat( query( stringContains( 1, stringValue( "apa" ) ) ), equalTo( asList( 3L, 4L, 5L ) ) );
            assertThat( query( stringContains( 1, stringValue( "apa*" ) ) ), equalTo( Collections.emptyList() ) );
        }

        @Test
        public void testIndexEndsWithWithDuplicated() throws Exception
        {
            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), "a" ),
                    add( 2L, descriptor.schema(), "A" ),
                    add( 3L, descriptor.schema(), "apa" ),
                    add( 4L, descriptor.schema(), "apa" ),
                    add( 5L, descriptor.schema(), "longapa" ),
                    add( 6L, descriptor.schema(), "apalong" ) ) );

            assertThat( query( stringSuffix( 1, stringValue( "a" ) ) ), equalTo( asList( 1L, 3L, 4L, 5L ) ) );
            assertThat( query( stringSuffix( 1, stringValue( "apa" ) ) ), equalTo( asList( 3L, 4L, 5L ) ) );
            assertThat( query( stringSuffix( 1, stringValue( "apa*" ) ) ), equalTo( Collections.emptyList() ) );
            assertThat( query( stringSuffix( 1, stringValue( "" ) ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L, 6L ) ) );
        }

        @Test
        public void testIndexShouldHandleLargeAmountOfDuplicatesString() throws Exception
        {
            doTestShouldHandleLargeAmountOfDuplicates( "this is a semi-long string that will need to be split" );
        }

        @Test
        public void testIndexShouldHandleLargeAmountOfDuplicatesStringArray() throws Exception
        {
            Value arrayValue = nextRandomValidArrayValue();
            doTestShouldHandleLargeAmountOfDuplicates( arrayValue );
        }

        private void doTestShouldHandleLargeAmountOfDuplicates( Object value ) throws Exception
        {
            List<IndexEntryUpdate<?>> updates = new ArrayList<>();
            List<Long> nodeIds = new ArrayList<>();
            for ( long i = 0; i < 1000; i++ )
            {
                nodeIds.add( i );
                updates.add( add( i, descriptor.schema(), value ) );
            }
            updateAndCommit( updates );

            assertThat( query( exists( 1 ) ), equalTo( nodeIds ) );
        }

        private Value nextRandomValidArrayValue()
        {
            Value value;
            do
            {
                value = random.randomValues().nextArray();
                // todo remove when spatial is supported by all
            }
            while ( !testSuite.supportsSpatial() && Values.isGeometryArray( value ) );
            return value;
        }
    }

    // This behaviour is expected by Unique indexes

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class Unique extends SimpleIndexAccessorCompatibility
    {
        public Unique( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, TestIndexDescriptorFactory.uniqueForLabel( 1000, 100 ) );
        }

        @Test
        public void closingAnOnlineIndexUpdaterMustNotThrowEvenIfItHasBeenFedConflictingData() throws Exception
        {
            // The reason is that we use and close IndexUpdaters in commit - not in prepare - and therefor
            // we cannot have them go around and throw exceptions, because that could potentially break
            // recovery.
            // Conflicting data can happen because of faulty data coercion. These faults are resolved by
            // the exact-match filtering we do on index seeks.

            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), "a" ),
                    add( 2L, descriptor.schema(), "a" ) ) );

            assertThat( query( exact( 1, "a" ) ), equalTo( asList( 1L, 2L ) ) );
        }

        @Test
        public void testIndexSeekAndScan() throws Exception
        {
            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), "a" ),
                    add( 2L, descriptor.schema(), "b" ),
                    add( 3L, descriptor.schema(), "c" ) ) );

            assertThat( query( exact( 1, "a" ) ), equalTo( singletonList( 1L ) ) );
            assertThat( query( IndexQuery.exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        }
    }
}
