/*
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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;

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

    @Ignore( "Invalid assumption since we currently must rely on close throwing exception for injected"
            + "transactions that violate a constraint" )
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
    public void before() throws Exception
    {
        IndexConfiguration indexConfig = new IndexConfiguration( true );
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( new Config() );
        IndexPopulator populator = indexProvider.getPopulator( 17, descriptor, indexConfig, indexSamplingConfig );
        populator.create();
        populator.close( true );
        accessor = indexProvider.getOnlineAccessor( 17, indexConfig, indexSamplingConfig );
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

    private void updateAndCommit( List<NodePropertyUpdate> updates )
            throws IOException, IndexEntryConflictException, IndexCapacityExceededException
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
