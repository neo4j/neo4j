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

import org.hamcrest.Matchers;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Collections;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

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
        updateAndCommit( asList(
                add( 1L, descriptor.schema(), "a", "a" ),
                add( 2L, descriptor.schema(), "b", "b" ),
                add( 3L, descriptor.schema(), "a", "b" ) ) );

        assertThat( query( exact( 0, "a" ), exact( 1, "a" ) ), equalTo( singletonList( 1L ) ) );
        assertThat( query( exact( 0, "b" ), exact( 1, "b" ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, "a" ), exact( 1, "b" ) ), equalTo( singletonList( 3L ) ) );
        assertThat( query( exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
    }

    @Test
    public void testIndexScanAndSeekExactWithExactByNumber() throws Exception
    {
        updateAndCommit( asList(
                add( 1L, descriptor.schema(), 333, 333 ),
                add( 2L, descriptor.schema(), 101, 101 ),
                add( 3L, descriptor.schema(), 333, 101 ) ) );

        assertThat( query( exact( 0, 333 ), exact( 1, 333 ) ), equalTo( singletonList( 1L ) ) );
        assertThat( query( exact( 0, 101 ), exact( 1, 101 ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, 333 ), exact( 1, 101 ) ), equalTo( singletonList( 3L ) ) );
        assertThat( query( exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
    }

    @Test
    public void testIndexScanAndSeekExactWithExactByBoolean() throws Exception
    {
        updateAndCommit( asList(
                add( 1L, descriptor.schema(), true, true),
                add( 2L, descriptor.schema(), false, false ),
                add( 3L, descriptor.schema(), true, false ) ) );

        assertThat( query( exact( 0, true ), exact( 1, true ) ), equalTo( singletonList( 1L ) ) );
        assertThat( query( exact( 0, false ), exact( 1, false ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, true ), exact( 1, false ) ), equalTo( singletonList( 3L ) ) );
        assertThat( query( exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
    }

    @Test
    public void testIndexScanAndSeekExactWithExactByTemporal() throws Exception
    {
        updateAndCommit( asList(
                add( 1L, descriptor.schema(), epochDate( 303 ), epochDate( 303 ) ),
                add( 2L, descriptor.schema(), epochDate( 101 ), epochDate( 101 ) ),
                add( 3L, descriptor.schema(), epochDate( 303 ), epochDate( 101 ) ) ) );

        assertThat( query( exact( 0, epochDate( 303 ) ), exact( 1, epochDate( 303 ) ) ), equalTo( singletonList( 1L ) ) );
        assertThat( query( exact( 0, epochDate( 101 ) ), exact( 1, epochDate( 101 ) ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, epochDate( 303 ) ), exact( 1, epochDate( 101 ) ) ), equalTo( singletonList( 3L ) ) );
        assertThat( query( exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
    }

    // TODO testIndexQuery_Exact_Exact_Array

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
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), "a", "Anabelle" ),
                add( 2L, descriptor.schema(), "a", "Anna" ),
                add( 3L, descriptor.schema(), "a", "Bob" ),
                add( 4L, descriptor.schema(), "a", "Harriet" ),
                add( 5L, descriptor.schema(), "a", "William" ),
                add( 6L, descriptor.schema(), "b", "Anabelle" ),
                add( 7L, descriptor.schema(), "b", "Anna" ),
                add( 8L, descriptor.schema(), "b", "Bob" ),
                add( 9L, descriptor.schema(), "b", "Harriet" ),
                add( 10L, descriptor.schema(), "b", "William" ) ) );

        assertThat( query( exact( 0, "a" ), range( 1, "Anna", true, "Harriet", false ) ), equalTo( asList( 2L, 3L ) ) );
        assertThat( query( exact( 0, "a" ), range( 1, "Harriet", true, null, false ) ), equalTo( asList( 4L, 5L ) ) );
        assertThat( query( exact( 0, "a" ), range( 1, "Harriet", false, null, true ) ), equalTo( singletonList( 5L ) ) );
        assertThat( query( exact( 0, "a" ), range( 1, "William", false, "Anna", true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, "a" ), range( 1, null, false, "Bob", false ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( exact( 0, "a" ), range( 1, null, true, "Bob", true ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        assertThat( query( exact( 0, "a" ), range( 1, (String)null, true, null, true ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( query( exact( 0, "a" ), range( 1, "Anabelle", false, "Anna", true ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, "a" ), range( 1, "Anabelle", false, "Bob", false ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, "b" ), range( 1, "Anna", true, "Harriet", false ) ), equalTo( asList( 7L, 8L ) ) );
        assertThat( query( exact( 0, "b" ), range( 1, "Harriet", true, null, false ) ), equalTo( asList( 9L, 10L ) ) );
        assertThat( query( exact( 0, "b" ), range( 1, "Harriet", false, null, true ) ), equalTo( singletonList( 10L ) ) );
        assertThat( query( exact( 0, "b" ), range( 1, "William", false, "Anna", true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, "b" ), range( 1, null, false, "Bob", false ) ), equalTo( asList( 6L, 7L ) ) );
        assertThat( query( exact( 0, "b" ), range( 1, null, true, "Bob", true ) ), equalTo( asList( 6L, 7L, 8L ) ) );
        assertThat( query( exact( 0, "b" ), range( 1, (String)null, true, null, true ) ), equalTo( asList( 6L, 7L, 8L, 9L, 10L ) ) );
        assertThat( query( exact( 0, "b" ), range( 1, "Anabelle", false, "Anna", true ) ), equalTo( singletonList( 7L ) ) );
        assertThat( query( exact( 0, "b" ), range( 1, "Anabelle", false, "Bob", false ) ), equalTo( singletonList( 7L ) ) );
    }

    @Test
    public void testIndexSeekExactWithRangeByNumber() throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), 303, 111 ),
                add( 2L, descriptor.schema(), 303, 222 ),
                add( 3L, descriptor.schema(), 303, 333 ),
                add( 4L, descriptor.schema(), 303, 444 ),
                add( 5L, descriptor.schema(), 303, 555 ),
                add( 6L, descriptor.schema(), 101, 111 ),
                add( 7L, descriptor.schema(), 101, 222 ),
                add( 8L, descriptor.schema(), 101, 333 ),
                add( 9L, descriptor.schema(), 101, 444 ),
                add( 10L, descriptor.schema(), 101, 555 ) ) );

        assertThat( query( exact( 0, 303 ), range( 1, 222, true, 444, false ) ), equalTo( asList( 2L, 3L ) ) );
        assertThat( query( exact( 0, 303 ), range( 1, 444, true, null, false ) ), equalTo( asList( 4L, 5L ) ) );
        assertThat( query( exact( 0, 303 ), range( 1, 444, false, null, true ) ), equalTo( singletonList( 5L ) ) );
        assertThat( query( exact( 0, 303 ), range( 1, 555, false, 222, true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, 303 ), range( 1, null, false, 333, false ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( exact( 0, 303 ), range( 1, null, true, 333, true ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        assertThat( query( exact( 0, 303 ), range( 1, (String)null, true, null, true ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( query( exact( 0, 303 ), range( 1, 111, false, 222, true ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, 303 ), range( 1, 111, false, 333, false ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, 101 ), range( 1, 222, true, 444, false ) ), equalTo( asList( 7L, 8L ) ) );
        assertThat( query( exact( 0, 101 ), range( 1, 444, true, null, false ) ), equalTo( asList( 9L, 10L ) ) );
        assertThat( query( exact( 0, 101 ), range( 1, 444, false, null, true ) ), equalTo( singletonList( 10L ) ) );
        assertThat( query( exact( 0, 101 ), range( 1, 555, false, 222, true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, 101 ), range( 1, null, false, 333, false ) ), equalTo( asList( 6L, 7L ) ) );
        assertThat( query( exact( 0, 101 ), range( 1, null, true, 333, true ) ), equalTo( asList( 6L, 7L, 8L ) ) );
        assertThat( query( exact( 0, 101 ), range( 1, (String)null, true, null, true ) ), equalTo( asList( 6L, 7L, 8L, 9L, 10L ) ) );
        assertThat( query( exact( 0, 101 ), range( 1, 111, false, 222, true ) ), equalTo( singletonList( 7L ) ) );
        assertThat( query( exact( 0, 101 ), range( 1, 111, false, 333, false ) ), equalTo( singletonList( 7L ) ) );
    }

    @Test
    public void testIndexSeekExactWithRangeByTemporal() throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), epochDate( 303 ), epochDate( 111 ) ),
                add( 2L, descriptor.schema(), epochDate( 303 ), epochDate( 222 ) ),
                add( 3L, descriptor.schema(), epochDate( 303 ), epochDate( 333 ) ),
                add( 4L, descriptor.schema(), epochDate( 303 ), epochDate( 444 ) ),
                add( 5L, descriptor.schema(), epochDate( 303 ), epochDate( 555 ) ),
                add( 6L, descriptor.schema(), epochDate( 101 ), epochDate( 111 ) ),
                add( 7L, descriptor.schema(), epochDate( 101 ), epochDate( 222 ) ),
                add( 8L, descriptor.schema(), epochDate( 101 ), epochDate( 333 ) ),
                add( 9L, descriptor.schema(), epochDate( 101 ), epochDate( 444 ) ),
                add( 10L, descriptor.schema(), epochDate( 101 ), epochDate( 555 ) ) ) );

        assertThat( query( exact( 0, epochDate( 303 ) ), range( 1, epochDate( 222 ), true, epochDate( 444 ), false ) ), equalTo( asList( 2L, 3L ) ) );
        assertThat( query( exact( 0, epochDate( 303 ) ), range( 1, epochDate( 444 ), true, null, false ) ), equalTo( asList( 4L, 5L ) ) );
        assertThat( query( exact( 0, epochDate( 303 ) ), range( 1, epochDate( 444 ), false, null, true ) ), equalTo( singletonList( 5L ) ) );
        assertThat( query( exact( 0, epochDate( 303 ) ), range( 1, epochDate( 555 ), false, epochDate( 222 ), true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, epochDate( 303 ) ), range( 1, null, false, epochDate( 333 ), false ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( exact( 0, epochDate( 303 ) ), range( 1, null, true, epochDate( 333 ), true ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        // can't create range(null, null): assertThat( query( exact( 0, epochDate( 303 ) ), range( 1, (DateValue) null, true, null, true ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( query( exact( 0, epochDate( 303 ) ), range( 1, epochDate( 111 ), false, epochDate( 222 ), true ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, epochDate( 303 ) ), range( 1, epochDate( 111 ), false, epochDate( 333 ), false ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, epochDate( 101 ) ), range( 1, epochDate( 222 ), true, epochDate( 444 ), false ) ), equalTo( asList( 7L, 8L ) ) );
        assertThat( query( exact( 0, epochDate( 101 ) ), range( 1, epochDate( 444 ), true, null, false ) ), equalTo( asList( 9L, 10L ) ) );
        assertThat( query( exact( 0, epochDate( 101 ) ), range( 1, epochDate( 444 ), false, null, true ) ), equalTo( singletonList( 10L ) ) );
        assertThat( query( exact( 0, epochDate( 101 ) ), range( 1, epochDate( 555 ), false, epochDate( 222 ), true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, epochDate( 101 ) ), range( 1, null, false, epochDate( 333 ), false ) ), equalTo( asList( 6L, 7L ) ) );
        assertThat( query( exact( 0, epochDate( 101 ) ), range( 1, null, true, epochDate( 333 ), true ) ), equalTo( asList( 6L, 7L, 8L ) ) );
        assertThat( query( exact( 0, epochDate( 101 ) ), range( 1, (String) null, true, null, true ) ), equalTo( asList( 6L, 7L, 8L, 9L, 10L ) ) );
        assertThat( query( exact( 0, epochDate( 101 ) ), range( 1, epochDate( 111 ), false, epochDate( 222 ), true ) ), equalTo( singletonList( 7L ) ) );
        assertThat( query( exact( 0, epochDate( 101 ) ), range( 1, epochDate( 111 ), false, epochDate( 333 ), false ) ), equalTo( singletonList( 7L ) ) );
    }

    @Test
    public void testIndexSeekExactWithRangeByBoolean() throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), true, false ),
                add( 2L, descriptor.schema(), true, true ),
                add( 3L, descriptor.schema(), false, false ),
                add( 4L, descriptor.schema(), false, true ) ) );

        assertThat( query( exact( 0, true ), range( 1, BooleanValue.FALSE, true, BooleanValue.TRUE, true ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( exact( 0, true ), range( 1, BooleanValue.FALSE, false, BooleanValue.TRUE, true ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( exact( 0, true ), range( 1, BooleanValue.FALSE, true, BooleanValue.TRUE, false ) ), equalTo( singletonList( 1L ) ) );
        assertThat( query( exact( 0, true ), range( 1, BooleanValue.FALSE, false, BooleanValue.TRUE, false ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( exact( 0, true ), range( 1, null, true, BooleanValue.TRUE, true ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( exact( 0, true ), range( 1, BooleanValue.FALSE, true, null, true ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( exact( 0, true ), range( 1, BooleanValue.TRUE, true, BooleanValue.FALSE, true ) ), equalTo( EMPTY_LIST ) );
    }

    // TODO testIndexQuery_Exact_Range_Spatial
    // TODO testIndexQuery_Exact_Range_Array

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
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), "a", 1 ),
                add( 2L, descriptor.schema(), "b", "abv" ),
                add( 3L, descriptor.schema(), "a", false ) ) );

        assertThat( query( exact( 0, "a" ), exists( 1 ) ), equalTo( asList( 1L, 3L ) ) );
        assertThat( query( exact( 0, "b" ), exists( 1 ) ), equalTo( singletonList( 2L ) ) );
    }

    @Test
    public void testIndexSeekExactWithExistsByNumber() throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), 303, 1 ),
                add( 2L, descriptor.schema(), 101, "abv" ),
                add( 3L, descriptor.schema(), 303, false ) ) );

        assertThat( query( exact( 0, 303 ), exists( 1 ) ), equalTo( asList( 1L, 3L ) ) );
        assertThat( query( exact( 0, 101 ), exists( 1 ) ), equalTo( singletonList( 2L ) ) );
    }

    @Test
    public void testIndexSeekExactWithExistsByTemporal() throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), epochDate( 303 ), 1 ),
                add( 2L, descriptor.schema(), epochDate( 101 ), "abv" ),
                add( 3L, descriptor.schema(), epochDate( 303 ) , false ) ) );

        assertThat( query( exact( 0, epochDate( 303 ) ), exists( 1 ) ), equalTo( asList( 1L, 3L ) ) );
        assertThat( query( exact( 0, epochDate( 101 ) ), exists( 1 ) ), equalTo( singletonList( 2L ) ) );
    }

    @Test
    public void testIndexSeekExactWithExistsByBoolean() throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), true, 1 ),
                add( 2L, descriptor.schema(), false, "abv" ),
                add( 3L, descriptor.schema(), true , false ) ) );

        assertThat( query( exact( 0, true ), exists( 1 ) ), equalTo( asList( 1L, 3L ) ) );
        assertThat( query( exact( 0, false ), exists( 1 ) ), equalTo( singletonList( 2L ) ) );
    }

    // TODO testIndexSeekExactWithExistsByArray
    // TODO testIndexSeekExactWithExistsBySpatial

    @Test
    public void testIndexSeekRangeWithExistsByString() throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), "Anabelle", 100 ),
                add( 2L, descriptor.schema(), "Anna", "someString" ),
                add( 3L, descriptor.schema(), "Bob", epochDate( 300 ) ),
                add( 4L, descriptor.schema(), "Harriet", true ),
                add( 5L, descriptor.schema(), "William", 42 ) ) );

        assertThat( query( range( 0, "Anna", true, "Harriet", false ), exists( 1 ) ), equalTo( asList( 2L, 3L ) ) );
        assertThat( query( range( 0, "Harriet", true, null, false ), exists( 1 ) ), equalTo( asList( 4L, 5L ) ) );
        assertThat( query( range( 0, "Harriet", false, null, true ), exists( 1 ) ), equalTo( singletonList( 5L ) ) );
        assertThat( query( range( 0, "William", false, "Anna", true ), exists( 1 ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( range( 0, null, false, "Bob", false ), exists( 1 ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( range( 0, null, true, "Bob", true ), exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        assertThat( query( range( 0, (String)null, true, null, true ), exists( 1 ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( query( range( 0, "Anabelle", false, "Anna", true ), exists( 1 ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( range( 0, "Anabelle", false, "Bob", false ), exists( 1 ) ), equalTo( singletonList( 2L ) ) );
    }

    @Test
    public void testIndexSeekRangeWithExistsByNumber() throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), -5, 100 ),
                add( 2L, descriptor.schema(), 0, "someString" ),
                add( 3L, descriptor.schema(), 5.5, epochDate( 300 ) ),
                add( 4L, descriptor.schema(), 10.0, true ),
                add( 5L, descriptor.schema(), 100.0, 42 ) ) );

        assertThat( query( range( 0, 0, true, 10, true ), exists( 1 ) ), equalTo( asList( 2L, 3L, 4L ) ) );
        assertThat( query( range( 0, 10, true, null, true ), exists( 1 ) ), equalTo( asList( 4L, 5L ) ) );
        assertThat( query( range( 0, 100, true, 0, true ), exists( 1 ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( range( 0, null, true, 5.5, true ), exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        assertThat( query( range( 0, (Number)null, true, null, true ), exists( 1 ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( query( range( 0, -5, true, 0, true ), exists( 1 ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( range( 0, -5, true, 5.5, true ), exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
    }

    @Test
    public void testIndexSeekRangeWithExistsByTemporal() throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );
        Assume.assumeTrue( testSuite.supportsTemporal() );

        DateTimeValue d1 = datetime( 9999, 100, ZoneId.of( "+18:00" ) );
        DateTimeValue d4 = datetime( 10000, 100, ZoneId.of( "UTC" ) );
        DateTimeValue d5 = datetime( 10000, 100, ZoneId.of( "+01:00" ) );
        DateTimeValue d6 = datetime( 10000, 100, ZoneId.of( "Europe/Stockholm" ) );
        DateTimeValue d7 = datetime( 10000, 100, ZoneId.of( "+03:00" ) );
        DateTimeValue d8 = datetime( 10000, 101, ZoneId.of( "UTC" ) );

        updateAndCommit( asList(
                add( 1L, descriptor.schema(), d1, 100 ),
                add( 4L, descriptor.schema(), d4, "someString" ),
                add( 5L, descriptor.schema(), d5, true ),
                add( 6L, descriptor.schema(), d6, epochDate( 200 ) ),
                add( 7L, descriptor.schema(), d7, false ),
                add( 8L, descriptor.schema(), d8, "anotherString" )
        ) );

        assertThat( query( range( 0, d4, true, d7, true ), exists( 1 ) ), Matchers.contains( 4L, 5L, 6L, 7L ) );
    }

    @Test
    public void testIndexSeekRangeWithExistsByBoolean() throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );

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

    // TODO testIndexSeekRangeWithExistsByArray
    // TODO testIndexSeekRangeWithExistsBySpatial

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
            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), "a", "a" ),
                    add( 2L, descriptor.schema(), "a", "a" ) ) );

            assertThat( query( exact( 0, "a" ), exact( 1, "a" ) ), equalTo( asList( 1L, 2L ) ) );
        }

        @Test
        public void testDuplicatesInIndexSeekByNumber() throws Exception
        {
            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), 333, 333 ),
                    add( 2L, descriptor.schema(), 333, 333 ) ) );

            assertThat( query( exact( 0, 333 ), exact( 1, 333 ) ), equalTo( asList( 1L, 2L ) ) );
        }

        @Test
        public void testDuplicatesInIndexSeekByPoint() throws Exception
        {
            Assume.assumeTrue( "Assume support for spatial", testSuite.supportsSpatial() );
            PointValue gps = Values.pointValue( CoordinateReferenceSystem.WGS84, 12.6, 56.7 );
            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), gps, gps ),
                    add( 2L, descriptor.schema(), gps, gps ) ) );

            assertThat( query( exact( 0, gps ), exact( 1, gps ) ), equalTo( asList( 1L, 2L ) ) );
        }

        @Test
        public void testDuplicatesInIndexSeekByBoolean() throws Exception
        {
            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), true, false ),
                    add( 2L, descriptor.schema(), true, false ) ) );

            assertThat( query( exact( 0, true ), exact( 1, false ) ), equalTo( asList( 1L, 2L ) ) );
        }

        // todo testDuplicatesInIndexSeekBySpatial
        // todo testDuplicatesInIndexSeekByArray
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

    private static IndexEntryUpdate<SchemaDescriptor> add( long nodeId, SchemaDescriptor schema, Object value1, Object value2 )
    {
        return IndexEntryUpdate.add( nodeId, schema, Values.of( value1 ), Values.of( value2 ) );
    }
}
