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

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.api.index.InternalIndexState.FAILED;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.add;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " SchemaIndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class NonUniqueIndexPopulatorCompatibility extends IndexProviderCompatibilityTestSuite.Compatibility
{
    public NonUniqueIndexPopulatorCompatibility( IndexProviderCompatibilityTestSuite testSuite )
    {
        super( testSuite );
    }

    @Test
    public void shouldProvidePopulatorThatAcceptsDuplicateEntries() throws Exception
    {
        // when
        IndexConfiguration config = new IndexConfiguration( false );
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( new Config() );
        IndexPopulator populator = indexProvider.getPopulator( 17, descriptor, config, indexSamplingConfig );
        populator.create();
        populator.add( 1, "value1" );
        populator.add( 2, "value1" );
        populator.close( true );

        // then
        IndexAccessor accessor = indexProvider.getOnlineAccessor( 17, config, indexSamplingConfig );
        try ( IndexReader reader = accessor.newReader() )
        {
            PrimitiveLongIterator nodes = reader.seek( "value1" );
            assertEquals( asSet( 1l, 2l ), asSet( nodes ) );
        }
        accessor.close();
    }

    @Test
    public void shouldStorePopulationFailedForRetrievalFromProviderLater() throws Exception
    {
        // GIVEN
        IndexConfiguration config = new IndexConfiguration( false );
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( new Config() );
        IndexPopulator populator = indexProvider.getPopulator( 17, descriptor, config, indexSamplingConfig );
        String failure = "The contrived failure";
        populator.create();

        // WHEN
        populator.markAsFailed( failure );

        // THEN
        assertEquals( failure, indexProvider.getPopulationFailure( 17 ) );
    }

    @Test
    public void shouldReportInitialStateAsFailedIfPopulationFailed() throws Exception
    {
        // GIVEN
        IndexConfiguration config = new IndexConfiguration( false );
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( new Config() );
        IndexPopulator populator = indexProvider.getPopulator( 17, descriptor, config, indexSamplingConfig );
        String failure = "The contrived failure";
        populator.create();

        // WHEN
        populator.markAsFailed( failure );

        // THEN
        assertEquals( FAILED, indexProvider.getInitialState( 17 ) );
    }

    @Test
    public void shouldBeAbleToDropAClosedIndexPopulator() throws Exception
    {
        // GIVEN
        IndexConfiguration config = new IndexConfiguration( false );
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( new Config() );
        IndexPopulator populator = indexProvider.getPopulator( 17, descriptor, config, indexSamplingConfig );
        populator.close( false );

        // WHEN
        populator.drop();

        // THEN - no exception should be thrown (it's been known to!)
    }

    @Test
    public void shouldApplyUpdatesIdempotently() throws Exception
    {
        // GIVEN
        IndexConfiguration config = new IndexConfiguration( false );
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( new Config() );
        IndexPopulator populator = indexProvider.getPopulator( 17, descriptor, config, indexSamplingConfig );
        populator.create();
        long nodeId = 1;
        int propertyKeyId = 10, labelId = 11; // Can we just use arbitrary ids here?
        final String propertyValue = "value1";
        PropertyAccessor propertyAccessor = new PropertyAccessor()
        {
            @Override
            public Property getProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException, PropertyNotFoundException
            {
                return Property.stringProperty( propertyKeyId, propertyValue );
            }
        };

        // this update (using add())...
        populator.add( nodeId, propertyValue );
        // ...is the same as this update (using update())
        try ( IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor ) )
        {
            updater.process( add( nodeId, propertyKeyId, propertyValue, new long[]{labelId} ) );
        }

        populator.close( true );



        // then
        IndexAccessor accessor = indexProvider.getOnlineAccessor( 17, new IndexConfiguration( false ), indexSamplingConfig );
        try ( IndexReader reader = accessor.newReader() )
        {
            PrimitiveLongIterator nodes = reader.seek( propertyValue );
            assertEquals( asSet( 1l ), asSet( nodes ) );
        }
        accessor.close();
    }
}
