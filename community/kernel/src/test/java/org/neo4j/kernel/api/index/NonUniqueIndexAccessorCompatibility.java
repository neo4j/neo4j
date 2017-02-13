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

import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;

import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " SchemaIndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class NonUniqueIndexAccessorCompatibility extends IndexAccessorCompatibility
{
    private static final NewIndexDescriptor index = NewIndexDescriptorFactory.forLabel( 1000, 100 );

    public NonUniqueIndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite )
    {
        super( testSuite, false );
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
                IndexEntryUpdate.add( 1L, index, "a" ),
                IndexEntryUpdate.add( 2L, index, "a" ) ) );

        assertThat( getAllNodesWithProperty( "a" ), equalTo( asList( 1L, 2L ) ) );
    }

    @Test
    public void testIndexSeekAndScan() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, index, "a" ),
                IndexEntryUpdate.add( 2L, index, "a" ),
                IndexEntryUpdate.add( 3L, index, "b" ) ) );

        assertThat( getAllNodesWithProperty( "a" ), equalTo( asList( 1L, 2L ) ) );
        assertThat( getAllNodes(), equalTo( asList( 1L, 2L, 3L ) ) );
    }

    @Test
    public void testIndexRangeSeekByNumberWithDuplicates() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, index, -5 ),
                IndexEntryUpdate.add( 2L, index, -5 ),
                IndexEntryUpdate.add( 3L, index, 0 ),
                IndexEntryUpdate.add( 4L, index, 5 ),
                IndexEntryUpdate.add( 5L, index, 5 ) ) );

        assertThat( getAllNodesFromInclusiveIndexSeekByNumber( -5, 5 ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( getAllNodesFromInclusiveIndexSeekByNumber( -3, -1 ), equalTo( EMPTY_LIST ) );
        assertThat( getAllNodesFromInclusiveIndexSeekByNumber( -5, 4 ), equalTo( asList( 1L, 2L, 3L ) ) );
        assertThat( getAllNodesFromInclusiveIndexSeekByNumber( -4, 5 ), equalTo( asList( 3L, 4L, 5L ) ) );
        assertThat( getAllNodesFromInclusiveIndexSeekByNumber( -5, 5 ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
    }

    @Test
    public void testIndexRangeSeekByStringWithDuplicates() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, index, "Anna" ),
                IndexEntryUpdate.add( 2L, index, "Anna" ),
                IndexEntryUpdate.add( 3L, index, "Bob" ),
                IndexEntryUpdate.add( 4L, index, "William" ),
                IndexEntryUpdate.add( 5L, index, "William" ) ) );

        assertThat( getAllNodesFromIndexSeekByString( "Anna", false, "William", false ), equalTo( singletonList( 3L ) ) );
        assertThat( getAllNodesFromIndexSeekByString( "Arabella", false, "Bob", false ), equalTo( EMPTY_LIST ) );
        assertThat( getAllNodesFromIndexSeekByString( "Anna", true, "William", false ), equalTo( asList( 1L, 2L, 3L ) ) );
        assertThat( getAllNodesFromIndexSeekByString( "Anna", false, "William", true ), equalTo( asList( 3L, 4L, 5L ) ) );
        assertThat( getAllNodesFromIndexSeekByString( "Anna", true, "William", true ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
    }

    @Test
    public void testIndexRangeSeekByPrefixWithDuplicates() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, index, "a" ),
                IndexEntryUpdate.add( 2L, index, "A" ),
                IndexEntryUpdate.add( 3L, index, "apa" ),
                IndexEntryUpdate.add( 4L, index, "apa" ),
                IndexEntryUpdate.add( 5L, index, "apa" ) ) );

        assertThat( getAllNodesFromIndexSeekByPrefix( "a" ), equalTo( asList( 1L, 3L, 4L, 5L ) ) );
        assertThat( getAllNodesFromIndexSeekByPrefix( "apa" ), equalTo( asList( 3L, 4L, 5L ) ) );
    }

    @Test
    public void testIndexFullSearchWithDuplicates() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, index, "a" ),
                IndexEntryUpdate.add( 2L, index, "A" ),
                IndexEntryUpdate.add( 3L, index, "apa" ),
                IndexEntryUpdate.add( 4L, index, "apa" ),
                IndexEntryUpdate.add( 5L, index, "apalong" ) ) );

        assertThat( getAllNodesFromIndexScanByContains( "a" ), equalTo( asList( 1L, 3L, 4L, 5L ) ) );
        assertThat( getAllNodesFromIndexScanByContains( "apa" ), equalTo( asList( 3L, 4L, 5L ) ) );
        assertThat( getAllNodesFromIndexScanByContains( "apa*" ), equalTo( Collections.emptyList() ) );
    }

    @Test
    public void testIndexEndsWithWithDuplicated() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, index, "a" ),
                IndexEntryUpdate.add( 2L, index, "A" ),
                IndexEntryUpdate.add( 3L, index, "apa" ),
                IndexEntryUpdate.add( 4L, index, "apa" ),
                IndexEntryUpdate.add( 5L, index, "longapa" ),
                IndexEntryUpdate.add( 6L, index, "apalong" ) ) );

        assertThat( getAllNodesFromIndexScanEndsWith( "a" ), equalTo( asList( 1L, 3L, 4L, 5L ) ) );
        assertThat( getAllNodesFromIndexScanEndsWith( "apa" ), equalTo( asList( 3L, 4L, 5L ) ) );
        assertThat( getAllNodesFromIndexScanEndsWith( "apa*" ), equalTo( Collections.emptyList() ) );
        assertThat( getAllNodesFromIndexScanEndsWith( "" ), equalTo( asList( 1L, 2L, 3L, 4L, 5L, 6L ) ) );
    }
}
