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

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.internal.kernel.api.IndexQuery.exists;
import static org.neo4j.kernel.api.index.IndexQueryHelper.exact;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " IndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public abstract class CompositeIndexAccessorCompatibility extends IndexAccessorCompatibility
{
    public CompositeIndexAccessorCompatibility(
            IndexProviderCompatibilityTestSuite testSuite, SchemaIndexDescriptor descriptor )
    {
        super( testSuite, descriptor );
    }

    @Test
    public void testIndexSeekAndScanByString() throws Exception
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
    public void testIndexSeekAndScanByNumber() throws Exception
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
    public void testIndexSeekAndScanByPoint() throws Exception
    {
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

    // This behaviour is expected by General indexes

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class General extends CompositeIndexAccessorCompatibility
    {
        public General( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, SchemaIndexDescriptorFactory.forLabel( 1000, 100, 200 ) );
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
            PointValue gps = Values.pointValue( CoordinateReferenceSystem.WGS84, 12.6, 56.7 );
            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), gps, gps ),
                    add( 2L, descriptor.schema(), gps, gps ) ) );

            assertThat( query( exact( 0, gps ), exact( 1, gps ) ), equalTo( asList( 1L, 2L ) ) );
        }
    }

    // This behaviour is expected by Unique indexes

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class Unique extends CompositeIndexAccessorCompatibility
    {
        public Unique( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, SchemaIndexDescriptorFactory.uniqueForLabel( 1000, 100, 200 ) );
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

//TODO: add when supported:
    //testIndexSeekByString
    //testIndexSeekByPrefix
    //testIndexSeekByPrefixOnNonStrings
}
