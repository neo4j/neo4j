/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collections;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static java.time.LocalDate.ofEpochDay;
import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.internal.kernel.api.IndexQuery.exists;
import static org.neo4j.internal.kernel.api.IndexQuery.range;
import static org.neo4j.kernel.api.index.IndexQueryHelper.exact;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.epochDate;
import static org.neo4j.values.storable.Values.booleanArray;
import static org.neo4j.values.storable.Values.longArray;
import static org.neo4j.values.storable.Values.stringArray;

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

        PointValue gps = Values.pointValue( CoordinateReferenceSystem.WGS84, 12.6, 56.7 );
        PointValue car = Values.pointValue( CoordinateReferenceSystem.Cartesian, 12.6, 56.7 );
        PointValue gps3d = Values.pointValue( CoordinateReferenceSystem.WGS84_3D, 12.6, 56.7, 100.0 );
        PointValue car3d = Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 12.6, 56.7, 100.0 );

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
        testIndexSeekExactWithRange( ValueGroup.TEXT, Values.of( "a" ), Values.of( "b" ),
                Values.of( "Anabelle" ),
                Values.of( "Anna" ),
                Values.of( "Bob" ),
                Values.of( "Harriet" ),
                Values.of( "William" ) );
    }

    @Test
    public void testIndexSeekExactWithRangeByNumber() throws Exception
    {
        testIndexSeekExactWithRange( ValueGroup.NUMBER, Values.of( 303 ), Values.of( 101 ),
                Values.of( 111 ),
                Values.of( 222 ),
                Values.of( 333 ),
                Values.of( 444 ),
                Values.of( 555 ) );
    }

    @Test
    public void testIndexSeekExactWithRangeByTemporal() throws Exception
    {
        testIndexSeekExactWithRange( ValueGroup.DATE, epochDate( 303 ), epochDate( 101 ),
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

        testIndexSeekExactWithRangeByBooleanType( ValueGroup.BOOLEAN_ARRAY, BooleanValue.TRUE, BooleanValue.FALSE,
                BooleanValue.FALSE,
                BooleanValue.TRUE );
    }

    @Test
    public void testIndexSeekExactWithRangeByStringArray() throws Exception
    {
        testIndexSeekExactWithRange( ValueGroup.TEXT_ARRAY, stringArray( "a", "c" ), stringArray( "b", "c" ),
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
        testIndexSeekExactWithRange( ValueGroup.NUMBER_ARRAY, longArray( new long[]{333, 9000} ), longArray( new long[]{101, 900} ),
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
        testIndexSeekExactWithRange( ValueGroup.BOOLEAN_ARRAY, booleanArray( new boolean[]{true, true} ), booleanArray( new boolean[]{false, false} ),
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
        testIndexSeekExactWithRange( ValueGroup.DATE_ARRAY, dateArray( 303, 900 ), dateArray( 101, 900 ),
                dateArray( 111, 900 ),
                dateArray( 222, 900 ),
                dateArray( 333, 900 ),
                dateArray( 444, 900 ),
                dateArray( 555, 900 ) );
    }

    // TODO testIndexQuery_Exact_Range_Array (spatial)
    // TODO testIndexQuery_Exact_Range_Spatial

    private void testIndexSeekExactWithRange( ValueGroup valueGroup, Value base1, Value base2, Value obj1, Value obj2, Value obj3, Value obj4, Value obj5 )
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
        assertThat( query( exact( 0, base1 ), range( 1, valueGroup ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, obj1, false, obj2, true ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, obj1, false, obj3, false ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj2, true, obj4, false ) ), equalTo( asList( 7L, 8L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj4, true, null, false ) ), equalTo( asList( 9L, 10L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj4, false, null, true ) ), equalTo( singletonList( 10L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj5, false, obj2, true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, base2 ), range( 1, null, false, obj3, false ) ), equalTo( asList( 6L, 7L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, null, true, obj3, true ) ), equalTo( asList( 6L, 7L, 8L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, valueGroup ) ), equalTo( asList( 6L, 7L, 8L, 9L, 10L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1, false, obj2, true ) ), equalTo( singletonList( 7L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1, false, obj3, false ) ), equalTo( singletonList( 7L ) ) );
    }

    private void testIndexSeekExactWithRangeByBooleanType( ValueGroup valueGroup, Value base1, Value base2, Value obj1, Value obj2 ) throws Exception
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
        assertThat( query( exact( 0, base1 ), range( 1, valueGroup ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( exact( 0, base1 ), range( 1, obj2, true, obj1, true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1, true, obj2, true ) ), equalTo( asList( 3L, 4L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1, false, obj2, true ) ), equalTo( singletonList( 4L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1, true, obj2, false ) ), equalTo( singletonList( 3L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1, false, obj2, false ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, base2 ), range( 1, null, true, obj2, true ) ), equalTo( asList( 3L, 4L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, obj1, true, null, true ) ), equalTo( asList( 3L, 4L ) ) );
        assertThat( query( exact( 0, base2 ), range( 1, valueGroup ) ), equalTo( asList( 3L, 4L ) ) );
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

        assertThat( query( exact( 0, "a" ), IndexQuery.stringPrefix( 1, "a" ) ), equalTo( asList( 1L, 3L, 4L ) ) );
        assertThat( query( exact( 0, "a" ), IndexQuery.stringPrefix( 1, "A" ) ), equalTo( Collections.singletonList( 2L ) ) );
        assertThat( query( exact( 0, "a" ), IndexQuery.stringPrefix( 1, "ba" ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, "a" ), IndexQuery.stringPrefix( 1, "" ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( query( exact( 0, "b" ), IndexQuery.stringPrefix( 1, "a" ) ), equalTo( asList( 6L, 8L, 9L ) ) );
        assertThat( query( exact( 0, "b" ), IndexQuery.stringPrefix( 1, "A" ) ), equalTo( Collections.singletonList( 7L ) ) );
        assertThat( query( exact( 0, "b" ), IndexQuery.stringPrefix( 1, "ba" ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, "b" ), IndexQuery.stringPrefix( 1, "" ) ), equalTo( asList( 6L, 7L, 8L, 9L, 10L ) ) );
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

        assertThat( query( IndexQuery.stringPrefix( 0, "a" ), exists( 1 ) ), equalTo( asList( 1L, 3L, 4L, 6L, 8L, 9L ) ) );
        assertThat( query( IndexQuery.stringPrefix( 0, "A" ), exists( 1 ) ), equalTo( asList( 2L, 7L) ) );
        assertThat( query( IndexQuery.stringPrefix( 0, "ba" ), exists( 1 ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( IndexQuery.stringPrefix( 0, "" ), exists( 1 ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L ) ) );
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

    // TODO testIndexSeekExactWithExistsByArray (spatial)
    // TODO testIndexSeekExactWithExistsBySpatial

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
        testIndexSeekRangeWithExists( ValueGroup.TEXT, "Anabelle", "Anna", "Bob", "Harriet", "William" );
    }

    @Test
    public void testIndexSeekRangeWithExistsByNumber() throws Exception
    {
        testIndexSeekRangeWithExists( ValueGroup.NUMBER, -5, 0, 5.5, 10.0, 100.0 );
    }

    @Test
    public void testIndexSeekRangeWithExistsByTemporal() throws Exception
    {
        DateTimeValue d1 = datetime( 9999, 100, ZoneId.of( "+18:00" ) );
        DateTimeValue d2 = datetime( 10000, 100, ZoneId.of( "UTC" ) );
        DateTimeValue d3 = datetime( 10000, 100, ZoneId.of( "+01:00" ) );
        DateTimeValue d4 = datetime( 10000, 100, ZoneId.of( "Europe/Stockholm" ) );
        DateTimeValue d5 = datetime( 10000, 100, ZoneId.of( "+03:00" ) );
        testIndexSeekRangeWithExists( ValueGroup.DATE, d1, d2, d3, d4, d5  );
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
        testIndexSeekRangeWithExists( ValueGroup.TEXT_ARRAY,
                new String[]{"Anabelle", "Anabelle"},
                new String[]{"Anabelle", "Anablo"},
                new String[]{"Anna", "Anabelle"},
                new String[]{"Anna", "Anablo"},
                new String[]{"Bob"} );
    }

    @Test
    public void testIndexSeekRangeWithExistsByNumberArray() throws Exception
    {
        testIndexSeekRangeWithExists( ValueGroup.NUMBER_ARRAY,
                new long[]{303, 303},
                new long[]{303, 404},
                new long[]{600, 303},
                new long[]{600, 404},
                new long[]{900} );
    }

    @Test
    public void testIndexSeekRangeWithExistsByBooleanArray() throws Exception
    {
        testIndexSeekRangeWithExists( ValueGroup.NUMBER_ARRAY,
                new boolean[]{false, false},
                new boolean[]{false, true},
                new boolean[]{true, false},
                new boolean[]{true, true},
                new boolean[]{true, true, false} );
    }

    @Test
    public void testIndexSeekRangeWithExistsByTemporalArray() throws Exception
    {
        testIndexSeekRangeWithExists( ValueGroup.NUMBER_ARRAY,
                dateArray( 303, 303 ),
                dateArray( 303, 404 ),
                dateArray( 404, 303 ),
                dateArray( 404, 404 ),
                dateArray( 404, 404, 303 ) );
    }

    // TODO testIndexSeekRangeWithExistsByArray (spatial
    // TODO testIndexSeekRangeWithExistsBySpatial

    private void testIndexSeekRangeWithExists( ValueGroup valueGroup, Object obj1, Object obj2, Object obj3, Object obj4, Object obj5 ) throws Exception
    {
        testIndexSeekRangeWithExists( valueGroup, Values.of( obj1 ), Values.of( obj2 ), Values.of( obj3 ), Values.of( obj4 ), Values.of( obj5 ) );
    }

    private void testIndexSeekRangeWithExists( ValueGroup valueGroup, Value obj1, Value obj2, Value obj3, Value obj4, Value obj5 ) throws Exception
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
        assertThat( query( range( 0, valueGroup ), exists( 1 ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( query( range( 0, obj1, false, obj2, true ), exists( 1 ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( range( 0, obj1, false, obj3, false ), exists( 1 ) ), equalTo( singletonList( 2L ) ) );
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
            testDuplicatesInIndexSeek( Values.pointValue( CoordinateReferenceSystem.WGS84, 12.6, 56.7 ) );
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
            testDuplicatesInIndexSeek( Values.pointArray( new PointValue[]{
                    Values.pointValue( CoordinateReferenceSystem.WGS84, 12.6, 56.7 ),
                    Values.pointValue( CoordinateReferenceSystem.WGS84, 12.6, 56.7 )
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
