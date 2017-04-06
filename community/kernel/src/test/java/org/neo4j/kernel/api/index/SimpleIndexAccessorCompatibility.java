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

import java.util.Collections;

import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;

import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.api.schema.IndexQuery.exact;
import static org.neo4j.kernel.api.schema.IndexQuery.exists;
import static org.neo4j.kernel.api.schema.IndexQuery.range;
import static org.neo4j.kernel.api.schema.IndexQuery.stringContains;
import static org.neo4j.kernel.api.schema.IndexQuery.stringPrefix;
import static org.neo4j.kernel.api.schema.IndexQuery.stringSuffix;


@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " SchemaIndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public abstract class SimpleIndexAccessorCompatibility extends IndexAccessorCompatibility
{
    public SimpleIndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite,
            IndexDescriptor descriptor )
    {
        super( testSuite, descriptor );
    }

    // This behaviour is shared by General and Unique indexes

    @Test
    public void testIndexSeekByNumber() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, descriptor.schema(), -5 ),
                IndexEntryUpdate.add( 2L, descriptor.schema(), 0 ),
                IndexEntryUpdate.add( 3L, descriptor.schema(), 5.5 ),
                IndexEntryUpdate.add( 4L, descriptor.schema(), 10.0 ),
                IndexEntryUpdate.add( 5L, descriptor.schema(), 100.0 ) ) );

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
                IndexEntryUpdate.add( 1L, descriptor.schema(), "Anabelle" ),
                IndexEntryUpdate.add( 2L, descriptor.schema(), "Anna" ),
                IndexEntryUpdate.add( 3L, descriptor.schema(), "Bob" ),
                IndexEntryUpdate.add( 4L, descriptor.schema(), "Harriet" ),
                IndexEntryUpdate.add( 5L, descriptor.schema(), "William" ) ) );

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
                IndexEntryUpdate.add( 1L, descriptor.schema(), "a" ),
                IndexEntryUpdate.add( 2L, descriptor.schema(), "A" ),
                IndexEntryUpdate.add( 3L, descriptor.schema(), "apa" ),
                IndexEntryUpdate.add( 4L, descriptor.schema(), "apA" ),
                IndexEntryUpdate.add( 5L, descriptor.schema(), "b" ) ) );

        assertThat( query( IndexQuery.stringPrefix( 1, "a" ) ), equalTo( asList( 1L, 3L, 4L ) ) );
        assertThat( query( IndexQuery.stringPrefix( 1, "A" ) ), equalTo( Collections.singletonList( 2L ) ) );
        assertThat( query( IndexQuery.stringPrefix( 1, "ba" ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( IndexQuery.stringPrefix( 1, "" ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
    }

    @Test
    public void testIndexSeekByPrefixOnNonStrings() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, descriptor.schema(), "a" ),
                IndexEntryUpdate.add( 2L, descriptor.schema(), 2L ) ) );
        assertThat( query( IndexQuery.stringPrefix( 1, "2" ) ), equalTo( EMPTY_LIST ) );
    }

    // This behaviour is expected by General indexes

    public static class General extends SimpleIndexAccessorCompatibility
    {
        public General( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, IndexDescriptorFactory.forLabel( 1000, 100 ) );
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
                    IndexEntryUpdate.add( 1L, descriptor.schema(), "a" ),
                    IndexEntryUpdate.add( 2L, descriptor.schema(), "a" ) ) );

            assertThat( query( exact( 1, "a" ) ), equalTo( asList( 1L, 2L ) ) );
        }

        @Test
        public void testIndexSeekAndScan() throws Exception
        {
            updateAndCommit( asList(
                    IndexEntryUpdate.add( 1L, descriptor.schema(), "a" ),
                    IndexEntryUpdate.add( 2L, descriptor.schema(), "a" ),
                    IndexEntryUpdate.add( 3L, descriptor.schema(), "b" ) ) );

            assertThat( query( exact( 1, "a" ) ), equalTo( asList( 1L, 2L ) ) );
            assertThat( query( exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        }

        @Test
        public void testIndexRangeSeekByNumberWithDuplicates() throws Exception
        {
            updateAndCommit( asList(
                    IndexEntryUpdate.add( 1L, descriptor.schema(), -5 ),
                    IndexEntryUpdate.add( 2L, descriptor.schema(), -5 ),
                    IndexEntryUpdate.add( 3L, descriptor.schema(), 0 ),
                    IndexEntryUpdate.add( 4L, descriptor.schema(), 5 ),
                    IndexEntryUpdate.add( 5L, descriptor.schema(), 5 ) ) );

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
                    IndexEntryUpdate.add( 1L, descriptor.schema(), "Anna" ),
                    IndexEntryUpdate.add( 2L, descriptor.schema(), "Anna" ),
                    IndexEntryUpdate.add( 3L, descriptor.schema(), "Bob" ),
                    IndexEntryUpdate.add( 4L, descriptor.schema(), "William" ),
                    IndexEntryUpdate.add( 5L, descriptor.schema(), "William" ) ) );

            assertThat( query( range( 1, "Anna", false, "William", false ) ), equalTo( singletonList( 3L ) ) );
            assertThat( query( range( 1, "Arabella", false, "Bob", false ) ), equalTo( EMPTY_LIST ) );
            assertThat( query( range( 1, "Anna", true, "William", false ) ), equalTo( asList( 1L, 2L, 3L ) ) );
            assertThat( query( range( 1, "Anna", false, "William", true ) ), equalTo( asList( 3L, 4L, 5L ) ) );
            assertThat( query( range( 1, "Anna", true, "William", true ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        }

        @Test
        public void testIndexRangeSeekByPrefixWithDuplicates() throws Exception
        {
            updateAndCommit( asList(
                    IndexEntryUpdate.add( 1L, descriptor.schema(), "a" ),
                    IndexEntryUpdate.add( 2L, descriptor.schema(), "A" ),
                    IndexEntryUpdate.add( 3L, descriptor.schema(), "apa" ),
                    IndexEntryUpdate.add( 4L, descriptor.schema(), "apa" ),
                    IndexEntryUpdate.add( 5L, descriptor.schema(), "apa" ) ) );

            assertThat( query( stringPrefix( 1, "a" ) ), equalTo( asList( 1L, 3L, 4L, 5L ) ) );
            assertThat( query( stringPrefix( 1, "apa" ) ), equalTo( asList( 3L, 4L, 5L ) ) );
        }

        @Test
        public void testIndexFullSearchWithDuplicates() throws Exception
        {
            updateAndCommit( asList(
                    IndexEntryUpdate.add( 1L, descriptor.schema(), "a" ),
                    IndexEntryUpdate.add( 2L, descriptor.schema(), "A" ),
                    IndexEntryUpdate.add( 3L, descriptor.schema(), "apa" ),
                    IndexEntryUpdate.add( 4L, descriptor.schema(), "apa" ),
                    IndexEntryUpdate.add( 5L, descriptor.schema(), "apalong" ) ) );

            assertThat( query( stringContains( 1, "a" ) ), equalTo( asList( 1L, 3L, 4L, 5L ) ) );
            assertThat( query( stringContains( 1, "apa" ) ), equalTo( asList( 3L, 4L, 5L ) ) );
            assertThat( query( stringContains( 1, "apa*" ) ), equalTo( Collections.emptyList() ) );
        }

        @Test
        public void testIndexEndsWithWithDuplicated() throws Exception
        {
            updateAndCommit( asList(
                    IndexEntryUpdate.add( 1L, descriptor.schema(), "a" ),
                    IndexEntryUpdate.add( 2L, descriptor.schema(), "A" ),
                    IndexEntryUpdate.add( 3L, descriptor.schema(), "apa" ),
                    IndexEntryUpdate.add( 4L, descriptor.schema(), "apa" ),
                    IndexEntryUpdate.add( 5L, descriptor.schema(), "longapa" ),
                    IndexEntryUpdate.add( 6L, descriptor.schema(), "apalong" ) ) );

            assertThat( query( stringSuffix( 1, "a" ) ), equalTo( asList( 1L, 3L, 4L, 5L ) ) );
            assertThat( query( stringSuffix( 1, "apa" ) ), equalTo( asList( 3L, 4L, 5L ) ) );
            assertThat( query( stringSuffix( 1, "apa*" ) ), equalTo( Collections.emptyList() ) );
            assertThat( query( stringSuffix( 1, "" ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L, 6L ) ) );
        }
    }

    // This behaviour is expected by Unique indexes

    public static class Unique extends SimpleIndexAccessorCompatibility
    {
        public Unique( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, IndexDescriptorFactory.uniqueForLabel( 1000, 100 ) );
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
                    IndexEntryUpdate.add( 1L, descriptor.schema(), "a" ),
                    IndexEntryUpdate.add( 2L, descriptor.schema(), "a" ) ) );

            assertThat( query( exact( 1, "a" ) ), equalTo( asList( 1L, 2L ) ) );
        }

        @Test
        public void testIndexSeekAndScan() throws Exception
        {
            updateAndCommit( asList(
                    IndexEntryUpdate.add( 1L, descriptor.schema(), "a" ),
                    IndexEntryUpdate.add( 2L, descriptor.schema(), "b" ),
                    IndexEntryUpdate.add( 3L, descriptor.schema(), "c" ) ) );

            assertThat( query( exact( 1, "a" ) ), equalTo( asList( 1L ) ) );
            assertThat( query( IndexQuery.exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        }
    }
}
