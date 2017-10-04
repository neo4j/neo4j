/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.index;

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.api.index.IndexQueryHelper.exact;
import static org.neo4j.kernel.api.schema.IndexQuery.exists;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " SchemaIndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public abstract class CompositeIndexAccessorCompatibility extends IndexAccessorCompatibility
{
    public CompositeIndexAccessorCompatibility(
            IndexProviderCompatibilityTestSuite testSuite, IndexDescriptor descriptor )
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

    // This behaviour is expected by General indexes

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class General extends CompositeIndexAccessorCompatibility
    {
        public General( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, IndexDescriptorFactory.forLabel( 1000, 100, 200 ) );
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
    }

    // This behaviour is expected by Unique indexes

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class Unique extends CompositeIndexAccessorCompatibility
    {
        public Unique( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, IndexDescriptorFactory.uniqueForLabel( 1000, 100, 200 ) );
        }

        @Test
        public void closingAnOnlineIndexUpdaterMustNotThrowEvenIfItHasBeenFedConflictingData() throws Exception
        {
            // The reason is that we use and close IndexUpdaters in commit - not in prepare - and therefor
            // we cannot have them go around and throw exceptions, because that could potentially break
            // recovery.
            // Conflicting data can happen because of faulty data coercion. These faults are resolved by
            // the exact-match filtering we do on index seeks in StateHandlingStatementOperations.

            updateAndCommit( asList(
                    add( 1L, descriptor.schema(), "a", "a" ),
                    add( 2L, descriptor.schema(), "a", "a" ) ) );

            assertThat( query( exact( 0, "a" ), exact( 1, "a" ) ), equalTo( asList( 1L, 2L ) ) );
        }
    }

    private static IndexEntryUpdate<LabelSchemaDescriptor> add( long nodeId, LabelSchemaDescriptor schema, Object value1, Object value2 )
    {
        return IndexEntryUpdate.add( nodeId, schema, Values.of( value1 ), Values.of( value2 ) );
    }

//TODO: add when supported:
    //testIndexSeekByString
    //testIndexSeekByPrefix
    //testIndexSeekByPrefixOnNonStrings
}
