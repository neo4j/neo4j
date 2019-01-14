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
package org.neo4j.kernel.api.index;

import org.hamcrest.Matchers;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
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

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " IndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public abstract class SimpleIndexAccessorCompatibility extends IndexAccessorCompatibility
{
    public SimpleIndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite,
            SchemaIndexDescriptor descriptor )
    {
        super( testSuite, descriptor );
    }

    // This behaviour is shared by General and Unique indexes

    @Test
    public void testIndexSeekByNumber() throws Exception
    {
        updateAndCommit( asList(
                add( 1L, descriptor.schema(), -5 ),
                add( 2L, descriptor.schema(), 0 ),
                add( 3L, descriptor.schema(), 5.5 ),
                add( 4L, descriptor.schema(), 10.0 ),
                add( 5L, descriptor.schema(), 100.0 ) ) );

        assertThat( query( range( 1, 0, true, 10, true ) ), equalTo( asList( 2L, 3L, 4L ) ) );
        assertThat( query( range( 1, 10, true, null, true ) ), equalTo( asList( 4L, 5L ) ) );
        assertThat( query( range( 1, 100, true, 0, true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( range( 1, null, true, 5.5, true ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        assertThat( query( range( 1, (Number)null, true, null, true ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( query( range( 1, -5, true, 0, true ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( range( 1, -5, true, 5.5, true ) ), equalTo( asList( 1L, 2L, 3L ) ) );
    }

    @Test
    public void testIndexSeekByString() throws Exception
    {
        updateAndCommit( asList(
                add( 1L, descriptor.schema(), "Anabelle" ),
                add( 2L, descriptor.schema(), "Anna" ),
                add( 3L, descriptor.schema(), "Bob" ),
                add( 4L, descriptor.schema(), "Harriet" ),
                add( 5L, descriptor.schema(), "William" ) ) );

        assertThat( query( range( 1, "Anna", true, "Harriet", false ) ), equalTo( asList( 2L, 3L ) ) );
        assertThat( query( range( 1, "Harriet", true, null, false ) ), equalTo( asList( 4L, 5L ) ) );
        assertThat( query( range( 1, "Harriet", false, null, true ) ), equalTo( singletonList( 5L ) ) );
        assertThat( query( range( 1, "William", false, "Anna", true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( range( 1, null, false, "Bob", false ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( range( 1, null, true, "Bob", true ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        assertThat( query( range( 1, (String)null, true, null, true ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( query( range( 1, "Anabelle", false, "Anna", true ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( range( 1, "Anabelle", false, "Bob", false ) ), equalTo( singletonList( 2L ) ) );
    }

    @Test
    public void testIndexSeekByPrefix() throws Exception
    {
        updateAndCommit( asList(
                add( 1L, descriptor.schema(), "a" ),
                add( 2L, descriptor.schema(), "A" ),
                add( 3L, descriptor.schema(), "apa" ),
                add( 4L, descriptor.schema(), "apA" ),
                add( 5L, descriptor.schema(), "b" ) ) );

        assertThat( query( IndexQuery.stringPrefix( 1, "a" ) ), equalTo( asList( 1L, 3L, 4L ) ) );
        assertThat( query( IndexQuery.stringPrefix( 1, "A" ) ), equalTo( Collections.singletonList( 2L ) ) );
        assertThat( query( IndexQuery.stringPrefix( 1, "ba" ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( IndexQuery.stringPrefix( 1, "" ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
    }

    @Test
    public void testIndexSeekByPrefixOnNonStrings() throws Exception
    {
        updateAndCommit( asList(
                add( 1L, descriptor.schema(), "a" ),
                add( 2L, descriptor.schema(), 2L ) ) );
        assertThat( query( IndexQuery.stringPrefix( 1, "2" ) ), equalTo( EMPTY_LIST ) );
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
            assertThat( result, equalTo( Collections.singletonList( entry.nodeId ) ) );
        }
    }

    @Test
    public void testIndexRangeSeekByDateTimeWithSneakyZones() throws Exception
    {
        Assume.assumeTrue( testSuite.supportsTemporal() );

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

    // This behaviour is expected by General indexes

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class General extends SimpleIndexAccessorCompatibility
    {
        public General( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, SchemaIndexDescriptorFactory.forLabel( 1000, 100 ) );
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
            Assume.assumeTrue( testSuite.supportsTemporal() );
            testIndexRangeSeekWithDuplicates( epochDate( 100 ),
                                              epochDate( 101 ),
                                              epochDate( 200 ),
                                              epochDate( 300 ) );
        }

        @Test
        public void testIndexRangeSeekByLocalDateTimeWithDuplicates() throws Exception
        {
            Assume.assumeTrue( testSuite.supportsTemporal() );
            testIndexRangeSeekWithDuplicates( localDateTime( 1000, 10 ),
                                              localDateTime( 1000, 11 ),
                                              localDateTime( 2000, 10 ),
                                              localDateTime( 3000, 10 ) );
        }

        @Test
        public void testIndexRangeSeekByDateTimeWithDuplicates() throws Exception
        {
            Assume.assumeTrue( testSuite.supportsTemporal() );
            testIndexRangeSeekWithDuplicates( datetime( 1000, 10, UTC ),
                                              datetime( 1000, 11, UTC ),
                                              datetime( 2000, 10, UTC ),
                                              datetime( 3000, 10, UTC ) );
        }

        @Test
        public void testIndexRangeSeekByLocalTimeWithDuplicates() throws Exception
        {
            Assume.assumeTrue( testSuite.supportsTemporal() );
            testIndexRangeSeekWithDuplicates( localTime( 1000 ),
                                              localTime( 1001 ),
                                              localTime( 2000 ),
                                              localTime( 3000 ) );
        }

        @Test
        public void testIndexRangeSeekByTimeWithDuplicates() throws Exception
        {
            Assume.assumeTrue( testSuite.supportsTemporal() );
            testIndexRangeSeekWithDuplicates( time( 1000, UTC ),
                                              time( 1001, UTC ),
                                              time( 2000, UTC ),
                                              time( 3000, UTC ) );
        }

        @Test
        public void testIndexRangeSeekByTimeWithZonesAndDuplicates() throws Exception
        {
            Assume.assumeTrue( testSuite.supportsTemporal() );
            testIndexRangeSeekWithDuplicates( time( 20, 31, 53, 4, ZoneOffset.of("+17:02") ),
                                              time( 20, 31, 54, 3, ZoneOffset.of("+17:02") ),
                                              time( 19, 31, 54, 2, UTC ),
                                              time( 18, 23, 27, 1, ZoneOffset.of("-18:00") ) );
        }

        @Test
        public void testIndexRangeSeekByDurationWithDuplicates() throws Exception
        {
            Assume.assumeTrue( testSuite.supportsTemporal() );
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

            assertThat( query( stringPrefix( 1, "a" ) ), equalTo( asList( 1L, 3L, 4L, 5L ) ) );
            assertThat( query( stringPrefix( 1, "apa" ) ), equalTo( asList( 3L, 4L, 5L ) ) );
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

            assertThat( query( stringContains( 1, "a" ) ), equalTo( asList( 1L, 3L, 4L, 5L ) ) );
            assertThat( query( stringContains( 1, "apa" ) ), equalTo( asList( 3L, 4L, 5L ) ) );
            assertThat( query( stringContains( 1, "apa*" ) ), equalTo( Collections.emptyList() ) );
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

            assertThat( query( stringSuffix( 1, "a" ) ), equalTo( asList( 1L, 3L, 4L, 5L ) ) );
            assertThat( query( stringSuffix( 1, "apa" ) ), equalTo( asList( 3L, 4L, 5L ) ) );
            assertThat( query( stringSuffix( 1, "apa*" ) ), equalTo( Collections.emptyList() ) );
            assertThat( query( stringSuffix( 1, "" ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L, 6L ) ) );
        }
    }

    // This behaviour is expected by Unique indexes

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class Unique extends SimpleIndexAccessorCompatibility
    {
        public Unique( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, SchemaIndexDescriptorFactory.uniqueForLabel( 1000, 100 ) );
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

            assertThat( query( exact( 1, "a" ) ), equalTo( asList( 1L ) ) );
            assertThat( query( IndexQuery.exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        }
    }
}
