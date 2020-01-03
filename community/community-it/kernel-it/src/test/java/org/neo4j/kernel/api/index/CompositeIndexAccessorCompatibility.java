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
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.SimpleNodeValueClient;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

import static java.time.LocalDate.ofEpochDay;
import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.internal.kernel.api.IndexQuery.exists;
import static org.neo4j.internal.kernel.api.IndexQuery.range;
import static org.neo4j.kernel.api.index.IndexQueryHelper.exact;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.epochDate;
import static org.neo4j.values.storable.ValueGroup.GEOMETRY;
import static org.neo4j.values.storable.ValueGroup.GEOMETRY_ARRAY;
import static org.neo4j.values.storable.Values.booleanArray;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longArray;
import static org.neo4j.values.storable.Values.pointArray;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " IndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public abstract class CompositeIndexAccessorCompatibility extends IndexAccessorCompatibility
{
    public CompositeIndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite, IndexDescriptor descriptor )
    {
        super( testSuite, descriptor );
    }

    /* testIndexSeekAndScan */

    @Test
    public void testIndexScanAndSeekExactWithExactByString() throws Exception
    {
        testIndexScanAndSeekExactWithExact( "a", "b" );
    }

    @Test
    public void testIndexScanAndSeekExactWithExactByNumber() throws Exception
    {
        testIndexScanAndSeekExactWithExact( 333, 101 );
    }

    @Test
    public void testIndexScanAndSeekExactWithExactByBoolean() throws Exception
    {
        testIndexScanAndSeekExactWithExact( true, false );
    }

    @Test
    public void testIndexScanAndSeekExactWithExactByTemporal() throws Exception
    {
        testIndexScanAndSeekExactWithExact( epochDate( 303 ), epochDate( 101 ) );
    }

    @Test
    public void testIndexScanAndSeekExactWithExactByStringArray() throws Exception
    {
        testIndexScanAndSeekExactWithExact( new String[]{"a", "c"}, new String[]{"b", "c"} );
    }

    @Test
    public void testIndexScanAndSeekExactWithExactByNumberArray() throws Exception
    {
        testIndexScanAndSeekExactWithExact( new int[]{333, 900}, new int[]{101, 900} );
    }

    @Test
    public void testIndexScanAndSeekExactWithExactByBooleanArray() throws Exception
    {
        testIndexScanAndSeekExactWithExact( new boolean[]{true, true}, new boolean[]{false, true} );
    }

    @Test
    public void testIndexScanAndSeekExactWithExactByTemporalArray() throws Exception
    {
        testIndexScanAndSeekExactWithExact( dateArray( 333, 900 ), dateArray( 101, 900 ) );
    }

    private void testIndexScanAndSeekExactWithExact( Object a, Object b ) throws Exception
    {
        testIndexScanAndSeekExactWithExact( Values.of( a ), Values.of( b ) );
    }

    private void testIndexScanAndSeekExactWithExact( Value a, Value b ) throws Exception
    {
        updateAndCommit( asList(
                add( 1L, descriptor.schema(), a, a ),
                add( 2L, descriptor.schema(), b, b ),
                add( 3L, descriptor.schema(), a, b ) ) );

        assertThat( query( exact( 0, a ), exact( 1, a ) ), equalTo( singletonList( 1L ) ) );
        assertThat( query( exact( 0, b ), exact( 1, b ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, a ), exact( 1, b ) ), equalTo( singletonList( 3L ) ) );
        assertThat( query( exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
    }

    @Test
    public void testIndexScanAndSeekExactWithExactByPoint() throws Exception
    {
        Assume.assumeTrue( "Assume support for spatial", testSuite.supportsSpatial() );

        PointValue gps = pointValue( WGS84, 12.6, 56.7 );
        PointValue car = pointValue( Cartesian, 12.6, 56.7 );
        PointValue gps3d = pointValue( CoordinateReferenceSystem.WGS84_3D, 12.6, 56.7, 100.0 );
        PointValue car3d = pointValue( CoordinateReferenceSystem.Cartesian_3D, 12.6, 56.7, 100.0 );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), gps, gps ),
                add( 2L, descriptor.schema(), car, car ),
                add( 3L, descriptor.schema(), gps, car ),
                add( 4L, descriptor.schema(), gps3d, gps3d ),
                add( 5L, descriptor.schema(), car3d, car3d ),
                add( 6L, descriptor.schema(), gps, car3d )
        ) );

        assertThat( query( exact( 0, gps ), exact( 1, gps ) ), equalTo( singletonList( 1L ) ) );
        assertThat( query( exact( 0, car ), exact( 1, car ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, gps ), exact( 1, car ) ), equalTo( singletonList( 3L ) ) );
        assertThat( query( exact( 0, gps3d ), exact( 1, gps3d ) ), equalTo( singletonList( 4L ) ) );
        assertThat( query( exact( 0, car3d ), exact( 1, car3d ) ), equalTo( singletonList( 5L ) ) );
        assertThat( query( exact( 0, gps ), exact( 1, car3d ) ), equalTo( singletonList( 6L ) ) );
        assertThat( query( exists( 1 ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L, 6L ) ) );
    }

    /* testIndexExactAndRangeExact_Range */

    @Test
    public void testIndexSeekExactWithRangeByString() throws Exception
    {
        testIndexSeekExactWithRange( Values.of( "a" ), Values.of( "b" ),
                Values.of( "Anabelle" ),
                Values.of( "Anna" ),
                Values.of( "Bob" ),
                Values.of( "Harriet" ),
                Values.of( "William" ) );
    }

    @Test
    public void testIndexSeekExactWithRangeByNumber() throws Exception
    {
        testIndexSeekExactWithRange( Values.of( 303 ), Values.of( 101 ),
                Values.of( 111 ),
                Values.of( 222 ),
                Values.of( 333 ),
                Values.of( 444 ),
                Values.of( 555 ) );
    }

    @Test
    public void testIndexSeekExactWithRangeByTemporal() throws Exception
    {
        testIndexSeekExactWithRange( epochDate( 303 ), epochDate( 101 ),
                epochDate( 111 ),
                epochDate( 222 ),
                epochDate( 333 ),
                epochDate( 444 ),
                epochDate( 555 ) );
    }

    @Test
    public void testIndexSeekExactWithRangeByBoolean() throws Exception
    {
        Assume.assumeTrue( "Assume support for boolean range queries", testSuite.supportsBooleanRangeQueries() );

        testIndexSeekExactWithRangeByBooleanType( BooleanValue.TRUE, BooleanValue.FALSE,
                BooleanValue.FALSE,
                BooleanValue.TRUE );
    }

    @Test
    public void testIndexSeekExactWithRangeByStringArray() throws Exception
    {
        testIndexSeekExactWithRange( stringArray( "a", "c" ), stringArray( "b", "c" ),
                stringArray( "Anabelle", "c" ),
                stringArray( "Anna", "c" ),
                stringArray( "Bob", "c" ),
                stringArray( "Harriet", "c" ),
                stringArray( "William", "c" )
        );
    }

    @Test
    public void testIndexSeekExactWithRangeByNumberArray() throws Exception
    {
        testIndexSeekExactWithRange( longArray( new long[]{333, 9000} ), longArray( new long[]{101, 900} ),
                longArray( new long[]{111, 900} ),
                longArray( new long[]{222, 900} ),
                longArray( new long[]{333, 900} ),
                longArray( new long[]{444, 900} ),
                longArray( new long[]{555, 900} )
        );
    }

    @Test
    public void testIndexSeekExactWithRangeByBooleanArray() throws Exception
    {
        testIndexSeekExactWithRange( booleanArray( new boolean[]{true, true} ), booleanArray( new boolean[]{false, false} ),
                booleanArray( new boolean[]{false, false} ),
                booleanArray( new boolean[]{false, true} ),
                booleanArray( new boolean[]{true, false} ),
                booleanArray( new boolean[]{true, true} ),
                booleanArray( new boolean[]{true, true, true} )
        );
    }

    @Test
    public void testIndexSeekExactWithRangeByTemporalArray() throws Exception
    {
        testIndexSeekExactWithRange( dateArray( 303, 900 ), dateArray( 101, 900 ),
                dateArray( 111, 900 ),
                dateArray( 222, 900 ),
                dateArray( 333, 900 ),
                dateArray( 444, 900 ),
                dateArray( 555, 900 ) );
    }

    @Test
    public void testIndexSeekExactWithRangeBySpatial() throws Exception
    {
        testIndexSeekExactWithRange( intValue( 100 ), intValue( 10 ),
                pointValue( WGS84, -10D, -10D ),
                pointValue( WGS84, -1D, -1D ),
                pointValue( WGS84, 0D, 0D ),
                pointValue( WGS84, 1D, 1D ),
                pointValue( WGS84, 10D, 10D ) );
    }

    private void testIndexSeekExactWithRange( Value base1, Value base2, Value obj1, Value obj2, Value obj3, Value obj4, Value obj5 )
            throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), base1, obj1 ),
                add( 2L, descriptor.schema(), base1, obj2 ),
                add( 3L, descriptor.schema(), base1, obj3 ),
                add( 4L, descriptor.schema(), base1, obj4 ),
                add( 5L, descriptor.schema(), base1, obj5 ),
                add( 6L, descriptor.schema(), base2, obj1 ),
                add( 7L, descriptor.schema(), base2, obj2 ),
                add( 8L, descriptor.schema(), base2, obj3 ),
                add( 9L, descriptor.schema(), base2, obj4 ),
                add( 10L, descriptor.schema(), base2, obj5 ) ) );

        assertThat( query( exact( 0, base1 ), range( 1, obj2, true, obj4, false ) ), equalTo( asList( 2L, 3L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, obj4, true, null, false ) ), equalTo( asList( 4L, 5L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, obj4, false, null, true ) ), equalTo( singletonList( 5L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, obj5, false, obj2, true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, base1 ), range( 1, null, false, obj3, false ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, null, true, obj3, true ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, obj1, false, obj2, true ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, obj1, false, obj3, false ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj2, true, obj4, false ) ), equalTo( asList( 7L, 8L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj4, true, null, false ) ), equalTo( asList( 9L, 10L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj4, false, null, true ) ), equalTo( singletonList( 10L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj5, false, obj2, true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, base2 ), range( 1, null, false, obj3, false ) ), equalTo( asList( 6L, 7L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, null, true, obj3, true ) ), equalTo( asList( 6L, 7L, 8L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1, false, obj2, true ) ), equalTo( singletonList( 7L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1, false, obj3, false ) ), equalTo( singletonList( 7L ) ) );

        ValueGroup valueGroup = obj1.valueGroup();
        if ( valueGroup != GEOMETRY && valueGroup != GEOMETRY_ARRAY )
        {
            assertThat( query( exact( 0, base1 ), range( 1, valueGroup ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
            assertThat( query( exact( 0, base2 ), range( 1, valueGroup ) ), equalTo( asList( 6L, 7L, 8L, 9L, 10L ) ) );
        }
        else
        {
            CoordinateReferenceSystem crs = getCrs( obj1 );
            assertThat( query( exact( 0, base1 ), range( 1, crs ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
            assertThat( query( exact( 0, base2 ), range( 1, crs ) ), equalTo( asList( 6L, 7L, 8L, 9L, 10L ) ) );
        }
    }

    private CoordinateReferenceSystem getCrs( Value value )
    {
        if ( Values.isGeometryValue( value ) )
        {
            return ((PointValue) value).getCoordinateReferenceSystem();
        }
        else if ( Values.isGeometryArray( value ) )
        {
            PointArray array = (PointArray) value;
            return array.pointValue( 0 ).getCoordinateReferenceSystem();
        }
        throw new IllegalArgumentException( "Expected some geometry value to get CRS from, but got " + value );
    }

    private void testIndexSeekExactWithRangeByBooleanType( Value base1, Value base2, Value obj1, Value obj2 ) throws Exception
    {
        updateAndCommit( asList(
                add( 1L, descriptor.schema(), base1, obj1 ),
                add( 2L, descriptor.schema(), base1, obj2 ),
                add( 3L, descriptor.schema(), base2, obj1 ),
                add( 4L, descriptor.schema(), base2, obj2 ) ) );

        assertThat( query( exact( 0, base1 ), range( 1, obj1, true, obj2, true ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, obj1, false, obj2, true ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, obj1, true, obj2, false ) ), equalTo( singletonList( 1L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, obj1, false, obj2, false ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, base1 ), range( 1, null, true, obj2, true ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, obj1, true, null, true ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, obj1.valueGroup() ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, obj2, true, obj1, true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1, true, obj2, true ) ), equalTo( asList( 3L, 4L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1, false, obj2, true ) ), equalTo( singletonList( 4L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1, true, obj2, false ) ), equalTo( singletonList( 3L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1, false, obj2, false ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, base2 ), range( 1, null, true, obj2, true ) ), equalTo( asList( 3L, 4L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1, true, null, true ) ), equalTo( asList( 3L, 4L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1.valueGroup() ) ), equalTo( asList( 3L, 4L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj2, true, obj1, true ) ), equalTo( EMPTY_LIST ) );
    }

    /* stringPrefix */

    @Test
    public void testIndexSeekExactWithPrefixRangeByString() throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), "a", "a" ),
                add( 2L, descriptor.schema(), "a", "A" ),
                add( 3L, descriptor.schema(), "a", "apa" ),
                add( 4L, descriptor.schema(), "a", "apA" ),
                add( 5L, descriptor.schema(), "a", "b" ),
                add( 6L, descriptor.schema(), "b", "a" ),
                add( 7L, descriptor.schema(), "b", "A" ),
                add( 8L, descriptor.schema(), "b", "apa" ),
                add( 9L, descriptor.schema(), "b", "apA" ),
                add( 10L, descriptor.schema(), "b", "b" ) ) );

        assertThat( query( exact( 0, "a" ), IndexQuery.stringPrefix( 1, stringValue( "a" ) ) ), equalTo( asList( 1L, 3L, 4L ) ) );
        assertThat( query( exact( 0, "a" ), IndexQuery.stringPrefix( 1, stringValue( "A" )) ), equalTo( Collections.singletonList( 2L ) ) );
        assertThat( query( exact( 0, "a" ), IndexQuery.stringPrefix( 1, stringValue( "ba") ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, "a" ), IndexQuery.stringPrefix( 1, stringValue( "" )) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( query( exact( 0, "b" ), IndexQuery.stringPrefix( 1, stringValue( "a" )) ), equalTo( asList( 6L, 8L, 9L ) ) );
        assertThat( query( exact( 0, "b" ), IndexQuery.stringPrefix( 1, stringValue( "A" )) ), equalTo( Collections.singletonList( 7L ) ) );
        assertThat( query( exact( 0, "b" ), IndexQuery.stringPrefix( 1, stringValue( "ba") ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, "b" ), IndexQuery.stringPrefix( 1, stringValue( "" ) ) ), equalTo( asList( 6L, 7L, 8L, 9L, 10L ) ) );
    }

    @Test
    public void testIndexSeekPrefixRangeWithExistsByString() throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), "a", 1 ),
                add( 2L, descriptor.schema(), "A", epochDate( 2 ) ),
                add( 3L, descriptor.schema(), "apa", "..." ),
                add( 4L, descriptor.schema(), "apA", "someString" ),
                add( 5L, descriptor.schema(), "b", true ),
                add( 6L, descriptor.schema(), "a", 100 ),
                add( 7L, descriptor.schema(), "A", epochDate( 200 ) ),
                add( 8L, descriptor.schema(), "apa", "!!!" ),
                add( 9L, descriptor.schema(), "apA", "someOtherString" ),
                add( 10L, descriptor.schema(), "b", false )
        ) );

        assertThat( query( IndexQuery.stringPrefix( 0, stringValue( "a" )), exists( 1 ) ), equalTo( asList( 1L, 3L, 4L, 6L, 8L, 9L ) ) );
        assertThat( query( IndexQuery.stringPrefix( 0, stringValue( "A" )), exists( 1 ) ), equalTo( asList( 2L, 7L) ) );
        assertThat( query( IndexQuery.stringPrefix( 0, stringValue( "ba") ), exists( 1 ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( IndexQuery.stringPrefix( 0, stringValue( "" )), exists( 1 ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L ) ) );
    }

    /* testIndexSeekExactWithExists */

    @Test
    public void testIndexSeekExactWithExistsByString() throws Exception
    {
        testIndexSeekExactWithExists( "a", "b" );
    }

    @Test
    public void testIndexSeekExactWithExistsByNumber() throws Exception
    {
        testIndexSeekExactWithExists( 303, 101 );
    }

    @Test
    public void testIndexSeekExactWithExistsByTemporal() throws Exception
    {
        testIndexSeekExactWithExists( epochDate( 303 ), epochDate( 101 ) );
    }

    @Test
    public void testIndexSeekExactWithExistsByBoolean() throws Exception
    {
        testIndexSeekExactWithExists( true, false );
    }

    @Test
    public void testIndexSeekExactWithExistsByStringArray() throws Exception
    {
        testIndexSeekExactWithExists( new String[]{"a", "c"}, new String[]{"b", "c"} );
    }

    @Test
    public void testIndexSeekExactWithExistsByNumberArray() throws Exception
    {
        testIndexSeekExactWithExists( new long[]{303, 900}, new long[]{101, 900} );
    }

    @Test
    public void testIndexSeekExactWithExistsByBooleanArray() throws Exception
    {
        testIndexSeekExactWithExists( new boolean[]{true, true}, new boolean[]{false, true} );
    }

    @Test
    public void testIndexSeekExactWithExistsByTemporalArray() throws Exception
    {
        testIndexSeekExactWithExists( dateArray( 303, 900 ), dateArray( 101, 900 ) );
    }

    @Test
    public void testIndexSeekExactWithExistsBySpatial() throws Exception
    {
        testIndexSeekExactWithExists( pointValue( WGS84, 100D, 100D ), pointValue( WGS84, 0D, 0D ) );
    }

    @Test
    public void testIndexSeekExactWithExistsBySpatialArray() throws Exception
    {
        testIndexSeekExactWithExists(
                pointArray( new PointValue[] {pointValue( Cartesian, 100D, 100D ), pointValue( Cartesian, 101D, 101D )} ),
                pointArray( new PointValue[] {pointValue( Cartesian, 0D, 0D ), pointValue( Cartesian, 1D, 1D )} ) );
    }

    private void testIndexSeekExactWithExists( Object a, Object b ) throws Exception
    {
        testIndexSeekExactWithExists( Values.of( a ), Values.of( b ) );
    }

    private void testIndexSeekExactWithExists( Value a, Value b ) throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );
        updateAndCommit( asList(
                add( 1L, descriptor.schema(), a, Values.of( 1 ) ),
                add( 2L, descriptor.schema(), b, Values.of( "abv" ) ),
                add( 3L, descriptor.schema(), a, Values.of( false ) ) ) );

        assertThat( query( exact( 0, a ), exists( 1 ) ), equalTo( asList( 1L, 3L ) ) );
        assertThat( query( exact( 0, b ), exists( 1 ) ), equalTo( singletonList( 2L ) ) );
    }

    /* testIndexSeekRangeWithExists */

    @Test
    public void testIndexSeekRangeWithExistsByString() throws Exception
    {
        testIndexSeekRangeWithExists( "Anabelle", "Anna", "Bob", "Harriet", "William" );
    }

    @Test
    public void testIndexSeekRangeWithExistsByNumber() throws Exception
    {
        testIndexSeekRangeWithExists( -5, 0, 5.5, 10.0, 100.0 );
    }

    @Test
    public void testIndexSeekRangeWithExistsByTemporal() throws Exception
    {
        DateTimeValue d1 = datetime( 9999, 100, ZoneId.of( "+18:00" ) );
        DateTimeValue d2 = datetime( 10000, 100, ZoneId.of( "UTC" ) );
        DateTimeValue d3 = datetime( 10000, 100, ZoneId.of( "+01:00" ) );
        DateTimeValue d4 = datetime( 10000, 100, ZoneId.of( "Europe/Stockholm" ) );
        DateTimeValue d5 = datetime( 10000, 100, ZoneId.of( "+03:00" ) );
        testIndexSeekRangeWithExists( d1, d2, d3, d4, d5  );
    }

    @Test
    public void testIndexSeekRangeWithExistsByBoolean() throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );
        Assume.assumeTrue( "Assume support for boolean range queries", testSuite.supportsBooleanRangeQueries() );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), false, "someString" ),
                add( 2L, descriptor.schema(), true, 1000 ) ) );

        assertThat( query( range( 0, BooleanValue.FALSE, true, BooleanValue.TRUE, true ), exists( 1 ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( range( 0, BooleanValue.FALSE, false, BooleanValue.TRUE, true ), exists( 1 ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( range( 0, BooleanValue.FALSE, true, BooleanValue.TRUE, false ), exists( 1 ) ), equalTo( singletonList( 1L ) ) );
        assertThat( query( range( 0, BooleanValue.FALSE, false, BooleanValue.TRUE, false ), exists( 1 ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( range( 0, null, true, BooleanValue.TRUE, true ), exists( 1 ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( range( 0, BooleanValue.FALSE, true, null, true ), exists( 1 ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( range( 0, BooleanValue.TRUE, true, BooleanValue.FALSE, true ), exists( 1 ) ), equalTo( EMPTY_LIST ) );
    }

    @Test
    public void testIndexSeekRangeWithExistsByStringArray() throws Exception
    {
        testIndexSeekRangeWithExists(
                new String[]{"Anabelle", "Anabelle"},
                new String[]{"Anabelle", "Anablo"},
                new String[]{"Anna", "Anabelle"},
                new String[]{"Anna", "Anablo"},
                new String[]{"Bob"} );
    }

    @Test
    public void testIndexSeekRangeWithExistsByNumberArray() throws Exception
    {
        testIndexSeekRangeWithExists(
                new long[]{303, 303},
                new long[]{303, 404},
                new long[]{600, 303},
                new long[]{600, 404},
                new long[]{900} );
    }

    @Test
    public void testIndexSeekRangeWithExistsByBooleanArray() throws Exception
    {
        testIndexSeekRangeWithExists(
                new boolean[]{false, false},
                new boolean[]{false, true},
                new boolean[]{true, false},
                new boolean[]{true, true},
                new boolean[]{true, true, false} );
    }

    @Test
    public void testIndexSeekRangeWithExistsByTemporalArray() throws Exception
    {
        testIndexSeekRangeWithExists(
                dateArray( 303, 303 ),
                dateArray( 303, 404 ),
                dateArray( 404, 303 ),
                dateArray( 404, 404 ),
                dateArray( 404, 404, 303 ) );
    }

    @Test
    public void testIndexSeekRangeWithExistsBySpatial() throws Exception
    {
        testIndexSeekRangeWithExists(
                pointValue( Cartesian, 0D, 0D ),
                pointValue( Cartesian, 1D, 1D ),
                pointValue( Cartesian, 2D, 2D ),
                pointValue( Cartesian, 3D, 3D ),
                pointValue( Cartesian, 4D, 4D ) );
    }

    @Test
    public void testIndexSeekRangeWithExistsBySpatialArray() throws Exception
    {
        testIndexSeekRangeWithExists(
                pointArray( new PointValue[] {pointValue( Cartesian, 0D, 0D ), pointValue( Cartesian, 0D, 1D )} ),
                pointArray( new PointValue[] {pointValue( Cartesian, 10D, 1D ), pointValue( Cartesian, 10D, 2D )} ),
                pointArray( new PointValue[] {pointValue( Cartesian, 20D, 2D ), pointValue( Cartesian, 20D, 3D )} ),
                pointArray( new PointValue[] {pointValue( Cartesian, 30D, 3D ), pointValue( Cartesian, 30D, 4D )} ),
                pointArray( new PointValue[] {pointValue( Cartesian, 40D, 4D ), pointValue( Cartesian, 40D, 5D )} ) );
    }

    @Test
    public void testExactMatchOnRandomCompositeValues() throws Exception
    {
        // given
        ValueType[] types = randomSetOfSupportedTypes();
        List<IndexEntryUpdate<?>> updates = new ArrayList<>();
        Set<ValueTuple> duplicateChecker = new HashSet<>();
        for ( long id = 0; id < 10_000; id++ )
        {
            IndexEntryUpdate<SchemaDescriptor> update;
            do
            {
                update = add( id, descriptor.schema(),
                        random.randomValues().nextValueOfTypes( types ),
                        random.randomValues().nextValueOfTypes( types ) );
            }
            while ( !duplicateChecker.add( ValueTuple.of( update.values() ) ) );
            updates.add( update );
        }
        updateAndCommit( updates );

        // when
        for ( IndexEntryUpdate<?> update : updates )
        {
            // then
            List<Long> hits = query( exact( 0, update.values()[0] ), exact( 1, update.values()[1] ) );
            assertEquals( update + " " + hits.toString(), 1, hits.size() );
            assertThat( single( hits ), equalTo( update.getEntityId() ) );
        }
    }

    private void testIndexSeekRangeWithExists( Object obj1, Object obj2, Object obj3, Object obj4, Object obj5 ) throws Exception
    {
        testIndexSeekRangeWithExists( Values.of( obj1 ), Values.of( obj2 ), Values.of( obj3 ), Values.of( obj4 ), Values.of( obj5 ) );
    }

    private void testIndexSeekRangeWithExists( Value obj1, Value obj2, Value obj3, Value obj4, Value obj5 ) throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );
        updateAndCommit( asList(
                add( 1L, descriptor.schema(), obj1, Values.of( 100 ) ),
                add( 2L, descriptor.schema(), obj2, Values.of( "someString" ) ),
                add( 3L, descriptor.schema(), obj3, Values.of( epochDate( 300 ) ) ),
                add( 4L, descriptor.schema(), obj4, Values.of( true ) ),
                add( 5L, descriptor.schema(), obj5, Values.of( 42 ) ) ) );

        assertThat( query( range( 0, obj2, true, obj4, false ), exists( 1 ) ), equalTo( asList( 2L, 3L ) ) );
        assertThat( query( range( 0, obj4, true, null, false ), exists( 1 ) ), equalTo( asList( 4L, 5L ) ) );
        assertThat( query( range( 0, obj4, false, null, true ), exists( 1 ) ), equalTo( singletonList( 5L ) ) );
        assertThat( query( range( 0, obj5, false, obj2, true ), exists( 1 ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( range( 0, null, false, obj3, false ), exists( 1 ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( range( 0, null, true, obj3, true ), exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        ValueGroup valueGroup = obj1.valueGroup();
        if ( valueGroup != GEOMETRY && valueGroup != GEOMETRY_ARRAY )
        {
            // This cannot be done for spatial values because each bound in a spatial query needs a coordinate reference system,
            // and those are provided by Value instances, e.g. PointValue
            assertThat( query( range( 0, obj1.valueGroup() ), exists( 1 ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        }
        assertThat( query( range( 0, obj1, false, obj2, true ), exists( 1 ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( range( 0, obj1, false, obj3, false ), exists( 1 ) ), equalTo( singletonList( 2L ) ) );
    }

    /* IndexOrder */

    @Test
    public void shouldRangeSeekInOrderNumberAscending() throws Exception
    {
        Object o0 = 0;
        Object o1 = 1;
        Object o2 = 2;
        Object o3 = 3;
        Object o4 = 4;
        Object o5 = 5;
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderNumberDescending() throws Exception
    {
        Object o0 = 0;
        Object o1 = 1;
        Object o2 = 2;
        Object o3 = 3;
        Object o4 = 4;
        Object o5 = 5;
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderStringAscending() throws Exception
    {
        Object o0 = "0";
        Object o1 = "1";
        Object o2 = "2";
        Object o3 = "3";
        Object o4 = "4";
        Object o5 = "5";
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
    }

    @Test
    public void shouldRangeSeekInOrderStringDescending() throws Exception
    {
        Object o0 = "0";
        Object o1 = "1";
        Object o2 = "2";
        Object o3 = "3";
        Object o4 = "4";
        Object o5 = "5";
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5 );
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
        shouldSeekInOrderExactWithRange( IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5 );
    }

    private void shouldSeekInOrderExactWithRange( IndexOrder order, Object o0, Object o1, Object o2, Object o3, Object o4, Object o5 ) throws Exception
    {
        Object baseValue = 1; // Todo use random value instead
        IndexQuery exact = exact( 100, baseValue );
        IndexQuery range = range( 200, Values.of( o0 ), true, Values.of( o5 ), true );
        IndexOrder[] indexOrders = orderCapability( exact, range );
        Assume.assumeTrue( "Assume support for order " + order, ArrayUtils.contains( indexOrders, order ) );

        updateAndCommit( asList(
                add( 1, descriptor.schema(), baseValue, o0 ),
                add( 1, descriptor.schema(), baseValue, o5 ),
                add( 1, descriptor.schema(), baseValue, o1 ),
                add( 1, descriptor.schema(), baseValue, o4 ),
                add( 1, descriptor.schema(), baseValue, o2 ),
                add( 1, descriptor.schema(), baseValue, o3 )
        ) );

        SimpleNodeValueClient client = new SimpleNodeValueClient();
        try ( AutoCloseable ignored = query( client, order, exact, range ) )
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
            Value[] value = new Value[]{random.nextValue( valueType ), random.nextValue( valueType )};
            updateAndCommit( singletonList( IndexEntryUpdate.add( entityId, descriptor.schema(), value ) ) );
            assertEquals( singletonList( entityId ), query( exactQuery( value ) ) );

            // when
            Value[] newValue;
            do
            {
                newValue = new Value[]{random.nextValue( valueType ), random.nextValue( valueType )};
            }
            while ( ValueTuple.of( value ).equals( ValueTuple.of( newValue ) ) );
            updateAndCommit( singletonList( IndexEntryUpdate.change( entityId, descriptor.schema(), value, newValue ) ) );

            // then
            assertEquals( emptyList(), query( exactQuery( value ) ) );
            assertEquals( singletonList( entityId ), query( exactQuery( newValue ) ) );
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
            Value[] value = new Value[]{random.nextValue( valueType ), random.nextValue( valueType )};
            updateAndCommit( singletonList( IndexEntryUpdate.add( entityId, descriptor.schema(), value ) ) );
            assertEquals( singletonList( entityId ), query( exactQuery( value ) ) );

            // when
            updateAndCommit( singletonList( IndexEntryUpdate.remove( entityId, descriptor.schema(), value ) ) );

            // then
            assertEquals( emptyList(), query( exactQuery( value ) ) );
        }
    }

    private static IndexQuery[] exactQuery( Value[] values )
    {
        return Stream.of( values ).map( v -> IndexQuery.exact( 0, v ) ).toArray( IndexQuery[]::new );
    }

    // This behaviour is expected by General indexes

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class General extends CompositeIndexAccessorCompatibility
    {
        public General( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, TestIndexDescriptorFactory.forLabel( 1000, 100, 200 ) );
        }

        @Test
        public void testDuplicatesInIndexSeekByString() throws Exception
        {
            Object value = "a";
            testDuplicatesInIndexSeek( value );
        }

        @Test
        public void testDuplicatesInIndexSeekByNumber() throws Exception
        {
            testDuplicatesInIndexSeek( 333 );
        }

        @Test
        public void testDuplicatesInIndexSeekByPoint() throws Exception
        {
            Assume.assumeTrue( "Assume support for spatial", testSuite.supportsSpatial() );
            testDuplicatesInIndexSeek( pointValue( WGS84, 12.6, 56.7 ) );
        }

        @Test
        public void testDuplicatesInIndexSeekByBoolean() throws Exception
        {
            testDuplicatesInIndexSeek( true );
        }

        @Test
        public void testDuplicatesInIndexSeekByTemporal() throws Exception
        {
            testDuplicatesInIndexSeek( ofEpochDay( 303 ) );
        }

        @Test
        public void testDuplicatesInIndexSeekByStringArray() throws Exception
        {
            testDuplicatesInIndexSeek( new String[]{"anabelle", "anabollo"} );
        }

        @Test
        public void testDuplicatesInIndexSeekByNumberArray() throws Exception
        {
            testDuplicatesInIndexSeek( new long[]{303, 606} );
        }

        @Test
        public void testDuplicatesInIndexSeekByBooleanArray() throws Exception
        {
            testDuplicatesInIndexSeek( new boolean[]{true, false} );
        }

        @Test
        public void testDuplicatesInIndexSeekByTemporalArray() throws Exception
        {
            testDuplicatesInIndexSeek( dateArray(303, 606) );
        }

        @Test
        public void testDuplicatesInIndexSeekByPointArray() throws Exception
        {
            Assume.assumeTrue( "Assume support for spatial", testSuite.supportsSpatial() );
            testDuplicatesInIndexSeek( pointArray( new PointValue[]{
                    pointValue( WGS84, 12.6, 56.7 ),
                    pointValue( WGS84, 12.6, 56.7 )
            } ) );
        }

        private void testDuplicatesInIndexSeek( Object value ) throws Exception
        {
            testDuplicatesInIndexSeek( Values.of( value ) );
        }

        private void testDuplicatesInIndexSeek( Value value ) throws Exception
        {
            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), value, value ),
                    add( 2L, descriptor.schema(), value, value ) ) );

            assertThat( query( exact( 0, value ), exact( 1, value ) ), equalTo( asList( 1L, 2L ) ) );
        }
    }

    // This behaviour is expected by Unique indexes

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class Unique extends CompositeIndexAccessorCompatibility
    {
        public Unique( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, TestIndexDescriptorFactory.uniqueForLabel( 1000, 100, 200 ) );
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
                    add( 1L, descriptor.schema(), "a", "a" ),
                    add( 2L, descriptor.schema(), "a", "a" ) ) );

            assertThat( query( exact( 0, "a" ), exact( 1, "a" ) ), equalTo( asList( 1L, 2L ) ) );
        }
    }

    private static ArrayValue dateArray( int... epochDays )
    {
        LocalDate[] localDates = new LocalDate[epochDays.length];
        for ( int i = 0; i < epochDays.length; i++ )
        {
            localDates[i] = ofEpochDay( epochDays[i] );
        }
        return Values.dateArray( localDates );
    }

    private static IndexEntryUpdate<SchemaDescriptor> add( long nodeId, SchemaDescriptor schema, Object value1, Object value2 )
    {
        return add( nodeId, schema, Values.of( value1 ), Values.of( value2 ) );
    }

    private static IndexEntryUpdate<SchemaDescriptor> add( long nodeId, SchemaDescriptor schema, Value value1, Value value2 )
    {
        return IndexEntryUpdate.add( nodeId, schema, value1, value2 );
    }
}
