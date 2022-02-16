/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.test.InMemoryTokens;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.neo4j.common.Subject.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.EmptyVersionContextSupplier.EMPTY;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class IndexPopulationTest
{
    private final IndexStatisticsStore indexStatisticsStore = mock( IndexStatisticsStore.class );
    private final InMemoryTokens tokens = new InMemoryTokens();
    private static final CursorContextFactory CONTEXT_FACTORY = new CursorContextFactory( PageCacheTracer.NULL, EMPTY );

    @Test
    void mustFlipToFailedIfFailureToApplyLastBatchWhileFlipping() throws Exception
    {
        // given
        NullLogProvider logProvider = NullLogProvider.getInstance();
        IndexStoreView storeView = emptyIndexStoreViewThatProcessUpdates();
        IndexPopulator.Adapter populator = emptyPopulatorWithThrowingUpdater();
        FailedIndexProxy failedProxy = failedIndexProxy( populator );
        OnlineIndexProxy onlineProxy = onlineIndexProxy();
        FlippableIndexProxy flipper = new FlippableIndexProxy();
        flipper.setFlipTarget( () -> onlineProxy );

        try ( JobScheduler scheduler = JobSchedulerFactory.createInitialisedScheduler();
                MultipleIndexPopulator multipleIndexPopulator = new MultipleIndexPopulator( storeView, logProvider, EntityType.NODE, mock( SchemaState.class ),
                        scheduler, tokens, CONTEXT_FACTORY, INSTANCE, "", AUTH_DISABLED, Config.defaults() ) )
        {
            MultipleIndexPopulator.IndexPopulation indexPopulation = multipleIndexPopulator.addPopulator( populator, dummyIndex(), flipper, t -> failedProxy );
            multipleIndexPopulator.queueConcurrentUpdate( someUpdate() );
            multipleIndexPopulator.createStoreScan( CONTEXT_FACTORY ).run( StoreScan.NO_EXTERNAL_UPDATES );

            // when
            indexPopulation.flip( CursorContext.NULL_CONTEXT );

            // then
            assertSame( InternalIndexState.FAILED, flipper.getState(), "flipper should have flipped to failing proxy" );
        }
    }

    private OnlineIndexProxy onlineIndexProxy()
    {
        return new OnlineIndexProxy( dummyIndex(), IndexAccessor.EMPTY, false );
    }

    private FailedIndexProxy failedIndexProxy( MinimalIndexAccessor minimalIndexAccessor )
    {
        return new FailedIndexProxy( dummyIndex(), minimalIndexAccessor, IndexPopulationFailure
                .failure( "failure" ), NullLogProvider.getInstance() );
    }

    private static IndexPopulator.Adapter emptyPopulatorWithThrowingUpdater()
    {
        return new IndexPopulator.Adapter()
        {
            @Override
            public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor, CursorContext cursorContext )
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

    private static IndexStoreView.Adaptor emptyIndexStoreViewThatProcessUpdates()
    {
        return new IndexStoreView.Adaptor()
        {
            @Override
            public StoreScan visitNodes( int[] labelIds, IntPredicate propertyKeyIdFilter, PropertyScanConsumer propertyScanConsumer,
                    TokenScanConsumer labelScanConsumer, boolean forceStoreScan, boolean parallelWrite, CursorContextFactory contextFactory,
                    MemoryTracker memoryTracker )
            {
                return new StoreScan()
                {
                    @Override
                    public void run( ExternalUpdatesCheck externalUpdatesCheck )
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
                };
            }
        };
    }

    private IndexProxyStrategy dummyIndex()
    {
        return new ValueIndexProxyStrategy( TestIndexDescriptorFactory.forLabel( 0, 0 ), indexStatisticsStore, tokens );
    }

    private static ValueIndexEntryUpdate<SchemaDescriptorSupplier> someUpdate()
    {
        return IndexEntryUpdate.add( 0, () -> SchemaDescriptors.forLabel( 0, 0 ), Values.numberValue( 0 ) );
    }
}
