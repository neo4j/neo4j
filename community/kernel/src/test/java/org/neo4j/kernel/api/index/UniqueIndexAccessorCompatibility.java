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

import org.neo4j.kernel.api.schema_new.IndexQuery;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.api.schema_new.IndexQuery.exact;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " SchemaIndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class UniqueIndexAccessorCompatibility extends IndexAccessorCompatibility
{
    public UniqueIndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite )
    {
        super( testSuite, NewIndexDescriptorFactory.uniqueForLabel( 1000, 100 ), true );
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
