/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

import static java.util.Arrays.asList;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " SchemaIndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class UniqueIndexAccessorCompatibility extends IndexProviderCompatibilityTestSuite.Compatibility
{
    private static final int PROPERTY_KEY_ID = 100;

    private IndexAccessor accessor;

    public UniqueIndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite )
    {
        super( testSuite );
    }

    @Test
    public void closingAnOnlineIndexUpdaterMustNotThrowEvenIfItHasBeenFedConflictingData() throws Exception
    {
        // The reason is that we use and close IndexUpdaters in commit - not in prepare - and therefor
        // we cannot have them go around and throw exceptions, because that could potentially break
        // recovery.
        // Conflicting data can happen because of faulty data coercion. These faults are resolved by
        // the exact-match filtering we do on index lookups in StateHandlingStatementOperations.

        updateAndCommit( asList(
                NodePropertyUpdate.add( 1L, PROPERTY_KEY_ID, "a", new long[]{1000} ),
                NodePropertyUpdate.add( 2L, PROPERTY_KEY_ID, "a", new long[]{1000} ) ) );

        assertThat( getAllNodes( "a" ), equalTo( asList( 1L, 2L ) ) );
    }

    @Before
    public void before() throws IOException
    {
        IndexConfiguration config = new IndexConfiguration( true );
        IndexPopulator populator = indexProvider.getPopulator( 17, descriptor, config );
        populator.create();
        populator.close( true );
        accessor = indexProvider.getOnlineAccessor( 17, config );
    }

    @After
    public void after() throws IOException
    {
        accessor.drop();
        accessor.close();
    }

    private List<Long> getAllNodes( String propertyValue ) throws IOException
    {
        try ( IndexReader reader = accessor.newReader() )
        {
            List<Long> list = new LinkedList<>();
            for ( PrimitiveLongIterator iterator = reader.lookup( propertyValue ); iterator.hasNext(); )
            {
                list.add( iterator.next() );
            }
            Collections.sort( list );
            return list;
        }
    }

    private void updateAndCommit( List<NodePropertyUpdate> updates ) throws IOException, IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( NodePropertyUpdate update : updates )
            {
                updater.process( update );
            }
        }
    }
}
