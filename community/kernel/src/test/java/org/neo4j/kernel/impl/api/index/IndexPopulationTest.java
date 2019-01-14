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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;

import java.util.function.IntPredicate;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class IndexPopulationTest
{
    @Test
    public void mustFlipToFailedIfFailureToApplyLastBatchWhileFlipping() throws Exception
    {
        // given
        NullLogProvider logProvider = NullLogProvider.getInstance();
        IndexStoreView storeView = emptyIndexStoreViewThatProcessUpdates();
        IndexPopulator.Adapter populator = emptyPopulatorWithThrowingUpdater();
        FailedIndexProxy failedProxy = failedIndexProxy( storeView, populator );
        OnlineIndexProxy onlineProxy = onlineIndexProxy( storeView );
        FlippableIndexProxy flipper = new FlippableIndexProxy();
        flipper.setFlipTarget( () -> onlineProxy );
        MultipleIndexPopulator multipleIndexPopulator = new MultipleIndexPopulator( storeView, logProvider,
                                                                                    mock( SchemaState.class ) );
        MultipleIndexPopulator.IndexPopulation indexPopulation =
                multipleIndexPopulator.addPopulator( populator, 0, dummyMeta(), flipper, t -> failedProxy, "userDescription" );
        multipleIndexPopulator.queue( someUpdate() );
        multipleIndexPopulator.indexAllNodes().run();

        // when
        indexPopulation.flip();

        // then
        assertTrue( "flipper should have flipped to failing proxy", flipper.getState() == InternalIndexState.FAILED );
    }

    private OnlineIndexProxy onlineIndexProxy( IndexStoreView storeView )
    {
        return new OnlineIndexProxy( 0, dummyMeta(), IndexAccessor.EMPTY, storeView, false );
    }

    private FailedIndexProxy failedIndexProxy( IndexStoreView storeView, IndexPopulator.Adapter populator )
    {
        return new FailedIndexProxy( dummyMeta(), "userDescription", populator, IndexPopulationFailure
                .failure( "failure" ), new IndexCountsRemover( storeView, 0 ), NullLogProvider.getInstance() );
    }

    private IndexPopulator.Adapter emptyPopulatorWithThrowingUpdater()
    {
        return new IndexPopulator.Adapter()
        {
            @Override
            public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor )
            {
                return new IndexUpdater()
                {
                    @Override
                    public void process( IndexEntryUpdate<?> update ) throws IndexEntryConflictException
                    {
                        throw new IndexEntryConflictException( 0, 1, Values.numberValue( 0 ) );
                    }

                    @Override
                    public void close()
                    {
                    }
                };
            }
        };
    }

    private IndexStoreView.Adaptor emptyIndexStoreViewThatProcessUpdates()
    {
        return new IndexStoreView.Adaptor()
        {
            @Override
            public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds, IntPredicate propertyKeyIdFilter,
                    Visitor<NodeUpdates,FAILURE> propertyUpdateVisitor, Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor, boolean forceStoreScan )
            {
                //noinspection unchecked
                return new StoreScan()
                {

                    @Override
                    public void run()
                    {
                    }

                    @Override
                    public void stop()
                    {
                    }

                    @Override
                    public PopulationProgress getProgress()
                    {
                        return null;
                    }

                    @Override
                    public void acceptUpdate( MultipleIndexPopulator.MultipleIndexUpdater updater, IndexEntryUpdate update, long currentlyIndexedNodeId )
                    {
                        updater.process( update );
                    }
                };
            }
        };
    }

    private IndexMeta dummyMeta()
    {
        return new IndexMeta( 0, SchemaIndexDescriptorFactory.forLabel( 0, 0 ),
                TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR, IndexCapability.NO_CAPABILITY );
    }

    private IndexEntryUpdate<LabelSchemaDescriptor> someUpdate()
    {
        return IndexEntryUpdate.add( 0, SchemaDescriptorFactory.forLabel( 0, 0 ), Values.numberValue( 0 ) );
    }
}
