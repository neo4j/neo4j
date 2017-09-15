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

import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.add;
import static org.neo4j.kernel.api.index.InternalIndexState.FAILED;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " SchemaIndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class SimpleIndexPopulatorCompatibility extends IndexProviderCompatibilityTestSuite.Compatibility
{
    public SimpleIndexPopulatorCompatibility(
            IndexProviderCompatibilityTestSuite testSuite, IndexDescriptor descriptor )
    {
        super( testSuite, descriptor );
    }

    @Test
    public void shouldStorePopulationFailedForRetrievalFromProviderLater() throws Exception
    {
        // GIVEN
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );
        withPopulator( indexProvider.getPopulator( 17, descriptor, indexSamplingConfig ), p ->
        {
            String failure = "The contrived failure";
            p.create();

            // WHEN
            p.markAsFailed( failure );
            p.close( false );

            // THEN
            assertThat( indexProvider.getPopulationFailure( 17 ), containsString( failure ) );
        } );
    }

    @Test
    public void shouldReportInitialStateAsFailedIfPopulationFailed() throws Exception
    {
        // GIVEN
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );
        withPopulator( indexProvider.getPopulator( 17, descriptor, indexSamplingConfig ), p ->
        {
            String failure = "The contrived failure";
            p.create();

            // WHEN
            p.markAsFailed( failure );

            // THEN
            assertEquals( FAILED, indexProvider.getInitialState( 17, descriptor ) );
        } );
    }

    @Test
    public void shouldBeAbleToDropAClosedIndexPopulator() throws Exception
    {
        // GIVEN
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );
        withPopulator( indexProvider.getPopulator( 17, descriptor, indexSamplingConfig ), p ->
        {
            p.close( false );

            // WHEN
            p.drop();

            // THEN - no exception should be thrown (it's been known to!)
        } );
    }

    @Test
    public void shouldApplyUpdatesIdempotently() throws Exception
    {
        // GIVEN
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );
        final Value propertyValue = Values.of( "value1" );
        withPopulator( indexProvider.getPopulator( 17, descriptor, indexSamplingConfig ), p ->
        {
            p.create();
            long nodeId = 1;
            PropertyAccessor propertyAccessor =
                    ( nodeId1, propertyKeyId ) -> propertyValue;

            // this update (using add())...
            p.add( singletonList( IndexEntryUpdate.add( nodeId, descriptor.schema(), propertyValue ) ) );
            // ...is the same as this update (using update())
            try ( IndexUpdater updater = p.newPopulatingUpdater( propertyAccessor ) )
            {
                updater.process( add( nodeId, descriptor.schema(), propertyValue ) );
            }

            p.close( true );
        } );

        // then
        try ( IndexAccessor accessor = indexProvider.getOnlineAccessor( 17, descriptor, indexSamplingConfig ) )
        {
            try ( IndexReader reader = accessor.newReader() )
            {
                int propertyKeyId = descriptor.schema().getPropertyId();
                PrimitiveLongIterator nodes = reader.query( IndexQuery.exact( propertyKeyId, propertyValue ) );
                assertEquals( asSet( 1L ), PrimitiveLongCollections.toSet( nodes ) );
            }
            accessor.close();
        }
    }

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class General extends SimpleIndexPopulatorCompatibility
    {
        public General( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, IndexDescriptorFactory.forLabel( 1000, 100 ) );
        }

        @Test
        public void shouldProvidePopulatorThatAcceptsDuplicateEntries() throws Exception
        {
            // when
            IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );
            Value value = Values.of( "value1" );
            withPopulator( indexProvider.getPopulator( 17, descriptor, indexSamplingConfig ), p ->
            {
                p.create();
                p.add( Arrays.asList(
                        IndexEntryUpdate.add( 1, descriptor.schema(), value ),
                        IndexEntryUpdate.add( 2, descriptor.schema(), value ) ) );
                p.close( true );
            } );

            // then
            try ( IndexAccessor accessor = indexProvider.getOnlineAccessor( 17, descriptor, indexSamplingConfig ) )
            {
                try ( IndexReader reader = accessor.newReader() )
                {
                    PrimitiveLongIterator nodes = reader.query( IndexQuery.exact( 1, value ) );
                    assertEquals( asSet( 1L, 2L ), PrimitiveLongCollections.toSet( nodes ) );
                }
                accessor.close();
            }
        }
    }

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class Unique extends SimpleIndexPopulatorCompatibility
    {
        public Unique( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, IndexDescriptorFactory.uniqueForLabel( 1000, 100 ) );
        }

        /**
         * This is also checked by the UniqueConstraintCompatibility test, only not on this abstraction level.
         */
        @Test
        public void shouldProvidePopulatorThatEnforcesUniqueConstraints() throws Exception
        {
            // when
            Value value = Values.of( "value1" );
            int nodeId1 = 1;
            int nodeId2 = 2;

            IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );
            withPopulator( indexProvider.getPopulator( 17, descriptor, indexSamplingConfig ), p ->
            {
                p.create();
                p.add( Arrays.asList(
                        IndexEntryUpdate.add( nodeId1, descriptor.schema(), value ),
                        IndexEntryUpdate.add( nodeId2, descriptor.schema(), value ) ) );
                try
                {
                    NodePropertyAccessor propertyAccessor =
                            new NodePropertyAccessor( nodeId1, descriptor.schema(), value );
                    propertyAccessor.addNode( nodeId2, descriptor.schema(), value );
                    p.verifyDeferredConstraints( propertyAccessor );

                    fail( "expected exception" );
                }
                // then
                catch ( IndexEntryConflictException conflict )
                {
                    assertEquals( nodeId1, conflict.getExistingNodeId() );
                    assertEquals( ValueTuple.of( value ), conflict.getPropertyValues() );
                    assertEquals( nodeId2, conflict.getAddedNodeId() );
                }
            } );
        }
    }
}
