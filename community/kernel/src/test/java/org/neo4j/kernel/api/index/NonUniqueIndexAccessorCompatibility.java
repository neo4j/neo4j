/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
    private static final int PROPERTY_KEY_ID = 100;

    public NonUniqueIndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite )
    {
        super( testSuite, false );
    }

    @Ignore( "Invalid assumption since we currently must rely on close throwing exception for injected"
             + "transactions that violate a constraint" )
    @Test
    public void closingAnOnlineIndexUpdaterMustNotThrowEvenIfItHasBeenFedConflictingData() throws Exception
    {
        // The reason is that we use and close IndexUpdaters in commit - not in prepare - and therefor
        // we cannot have them go around and throw exceptions, because that could potentially break
        // recovery.
        // Conflicting data can happen because of faulty data coercion. These faults are resolved by
        // the exact-match filtering we do on index seeks in StateHandlingStatementOperations.

        updateAndCommit( asList(
                NodePropertyUpdate.add( 1L, PROPERTY_KEY_ID, "a", new long[]{1000} ),
                NodePropertyUpdate.add( 2L, PROPERTY_KEY_ID, "a", new long[]{1000} ) ) );

        assertThat( getAllNodesWithProperty( "a" ), equalTo( asList( 1L, 2L ) ) );
    }

    @Test
    public void testIndexSeekAndScan() throws Exception
    {
        updateAndCommit( asList(
                NodePropertyUpdate.add( 1L, PROPERTY_KEY_ID, "a", new long[]{1000} ),
                NodePropertyUpdate.add( 2L, PROPERTY_KEY_ID, "a", new long[]{1000} ),
                NodePropertyUpdate.add( 3L, PROPERTY_KEY_ID, "b", new long[]{1000} ) ) );

        assertThat( getAllNodesWithProperty( "a" ), equalTo( asList( 1L, 2L ) ) );
        assertThat( getAllNodes(), equalTo( asList( 1L, 2L, 3L ) ) );
    }

    @Test
    public void testIndexRangeSeekByNumberWithDuplicates() throws Exception
    {
        updateAndCommit( asList(
                NodePropertyUpdate.add( 1L, PROPERTY_KEY_ID, -5, new long[]{1000} ),
                NodePropertyUpdate.add( 2L, PROPERTY_KEY_ID, -5, new long[]{1000} ),
                NodePropertyUpdate.add( 3L, PROPERTY_KEY_ID, 0, new long[]{1000} ),
                NodePropertyUpdate.add( 4L, PROPERTY_KEY_ID, 5, new long[]{1000} ),
                NodePropertyUpdate.add( 5L, PROPERTY_KEY_ID, 5, new long[]{1000} ) ) );

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
                NodePropertyUpdate.add( 1L, PROPERTY_KEY_ID, "Anna", new long[]{1000} ),
                NodePropertyUpdate.add( 2L, PROPERTY_KEY_ID, "Anna", new long[]{1000} ),
                NodePropertyUpdate.add( 3L, PROPERTY_KEY_ID, "Bob", new long[]{1000} ),
                NodePropertyUpdate.add( 4L, PROPERTY_KEY_ID, "William", new long[]{1000} ),
                NodePropertyUpdate.add( 5L, PROPERTY_KEY_ID, "William", new long[]{1000} ) ) );

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
                NodePropertyUpdate.add( 1L, PROPERTY_KEY_ID, "a", new long[]{1000} ),
                NodePropertyUpdate.add( 2L, PROPERTY_KEY_ID, "A", new long[]{1000} ),
                NodePropertyUpdate.add( 3L, PROPERTY_KEY_ID, "apa", new long[]{1000} ),
                NodePropertyUpdate.add( 4L, PROPERTY_KEY_ID, "apa", new long[]{1000} ),
                NodePropertyUpdate.add( 5L, PROPERTY_KEY_ID, "apa", new long[]{1000} ) ) );

        assertThat( getAllNodesFromIndexSeekByPrefix( "a" ), equalTo( asList( 1L, 3L, 4L, 5L ) ) );
        assertThat( getAllNodesFromIndexSeekByPrefix( "apa" ), equalTo( asList( 3L, 4L, 5L ) ) );
    }
}
