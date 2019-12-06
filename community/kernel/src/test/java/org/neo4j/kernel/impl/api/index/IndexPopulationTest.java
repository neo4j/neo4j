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

import org.junit.jupiter.api.Test;

import java.util.function.IntPredicate;

import org.neo4j.common.EntityType;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDropper;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodeLabelUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

class IndexPopulationTest
{
    @Test
    void mustFlipToFailedIfFailureToApplyLastBatchWhileFlipping() throws Exception
    {
        // given
        NullLogProvider logProvider = NullLogProvider.getInstance();
        IndexStoreView storeView = emptyIndexStoreViewThatProcessUpdates();
        IndexPopulator.Adapter populator = emptyPopulatorWithThrowingUpdater();
        IndexStatisticsStore indexStatisticsStore = mock( IndexStatisticsStore.class );
        FailedIndexProxy failedProxy = failedIndexProxy( populator, indexStatisticsStore );
        OnlineIndexProxy onlineProxy = onlineIndexProxy( indexStatisticsStore );
        FlippableIndexProxy flipper = new FlippableIndexProxy();
        flipper.setFlipTarget( () -> onlineProxy );
        MultipleIndexPopulator multipleIndexPopulator =
                new MultipleIndexPopulator( storeView, logProvider, EntityType.NODE, mock( SchemaState.class ), indexStatisticsStore,
                        mock( JobScheduler.class ) );

        MultipleIndexPopulator.IndexPopulation indexPopulation =
                multipleIndexPopulator.addPopulator( populator, dummyMeta(), flipper, t -> failedProxy, "userDescription" );
        multipleIndexPopulator.queueConcurrentUpdate( someUpdate() );
        multipleIndexPopulator.indexAllEntities().run();

        // when
        indexPopulation.flip( false );

        // then
        assertSame( flipper.getState(), InternalIndexState.FAILED, "flipper should have flipped to failing proxy" );
    }

    private OnlineIndexProxy onlineIndexProxy( IndexStatisticsStore indexStatisticsStore )
    {
        return new OnlineIndexProxy( dummyMeta(), IndexAccessor.EMPTY, indexStatisticsStore, false );
    }

    private FailedIndexProxy failedIndexProxy( IndexDropper dropper, IndexStatisticsStore indexStatisticsStore )
    {
        return new FailedIndexProxy( dummyMeta(), "userDescription", dropper, IndexPopulationFailure
                .failure( "failure" ), indexStatisticsStore, NullLogProvider.getInstance() );
    }

    private IndexPopulator.Adapter emptyPopulatorWithThrowingUpdater()
    {
        return new IndexPopulator.Adapter()
        {
            @Override
            public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
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
                    Visitor<EntityUpdates,FAILURE> propertyUpdateVisitor, Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor, boolean forceStoreScan )
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
                    public void acceptUpdate( MultipleIndexPopulator.MultipleIndexUpdater updater, IndexEntryUpdate update, long currentlyIndexedNodeId )
                    {
                        if ( update.getEntityId() <= currentlyIndexedNodeId )
                        {
                            updater.process( update );
                        }
                    }

                    @Override
                    public PopulationProgress getProgress()
                    {
                        return null;
                    }
                };
            }
        };
    }

    private IndexDescriptor dummyMeta()
    {
        return TestIndexDescriptorFactory.forLabel( 0, 0 );
    }

    private IndexEntryUpdate<LabelSchemaDescriptor> someUpdate()
    {
        return IndexEntryUpdate.add( 0, SchemaDescriptor.forLabel( 0, 0 ), Values.numberValue( 0 ) );
    }
}
