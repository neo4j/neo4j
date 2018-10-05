/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.Race;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.storageengine.api.schema.IndexDescriptorFactory.forSchema;
import static org.neo4j.test.Race.throwing;

public abstract class IndexPopulationStressTest
{
    private static final IndexProviderDescriptor PROVIDER = new IndexProviderDescriptor( "provider", "1.0" );
    @Rule
    public final RandomRule random = new RandomRule();
    @Rule
    public final PageCacheAndDependenciesRule rules =
            new PageCacheAndDependenciesRule( DefaultFileSystemRule::new, this.getClass() );

    protected final StoreIndexDescriptor descriptor = forSchema( forLabel( 0, 0 ), PROVIDER ).withId( 0 );
    protected final StoreIndexDescriptor descriptor2 = forSchema( forLabel( 1, 0 ), PROVIDER ).withId( 1 );
    private final IndexSamplingConfig samplingConfig = new IndexSamplingConfig( 1000, 0.2, true );
    private final NodePropertyAccessor nodePropertyAccessor = mock( NodePropertyAccessor.class );
    private final boolean canHandlePopulationConcurrentlyWithAdd;
    private final boolean hasValues;

    private IndexPopulator populator;
    private IndexProvider indexProvider;
    private boolean prevAccessCheck;

    protected IndexPopulationStressTest( boolean canHandlePopulationConcurrentlyWithAdd, boolean hasValues )
    {
        this.canHandlePopulationConcurrentlyWithAdd = canHandlePopulationConcurrentlyWithAdd;
        this.hasValues = hasValues;
    }

    /**
     * Create the provider to stress.
     */
    abstract IndexProvider newProvider( IndexDirectoryStructure.Factory directory );

    /**
     * Generate a random value to populate the index with.
     */
    abstract Value randomValue( RandomValues random );

    @Before
    public void setup() throws IOException, EntityNotFoundException
    {
        File storeDir = rules.directory().databaseDir();
        IndexDirectoryStructure.Factory directory = directoriesBySubProvider( directoriesByProvider( storeDir ).forProvider( PROVIDER ) );

        indexProvider = newProvider( directory );

        rules.fileSystem().mkdirs( indexProvider.directoryStructure().rootDirectory() );

        populator = indexProvider.getPopulator( descriptor, samplingConfig );
        when( nodePropertyAccessor.getNodePropertyValue( anyLong(), anyInt() ) ).thenThrow( UnsupportedOperationException.class );
        prevAccessCheck = UnsafeUtil.exchangeNativeAccessCheckEnabled( false );
    }

    @After
    public void teardown()
    {
        UnsafeUtil.exchangeNativeAccessCheckEnabled( prevAccessCheck );
        if ( populator != null )
        {
            populator.close( true );
        }
    }

    @Test
    public void stressIt() throws Throwable
    {
        Race race = new Race();
        int threads = 50;
        AtomicReferenceArray<List<? extends IndexEntryUpdate<?>>> lastBatches = new AtomicReferenceArray<>( threads );
        Generator[] generators = new Generator[threads];

        populator.create();
        int maxBatchSize = 100;
        int batchesPerThread = 100;
        int worstCaseEntriesPerThread = batchesPerThread * maxBatchSize;
        CountDownLatch insertersDone = new CountDownLatch( threads );
        ReadWriteLock updateLock = new ReentrantReadWriteLock( true );
        for ( int i = 0; i < threads; i++ )
        {
            int slot = i;
            race.addContestant( throwing( () ->
            {
                try
                {
                    Generator generator = generators[slot] = new Generator( maxBatchSize, random.seed(), slot * worstCaseEntriesPerThread );
                    for ( int j = 0; j < batchesPerThread; j++ )
                    {
                        List<? extends IndexEntryUpdate<?>> batch = generator.batch();
                        updateLock.readLock().lock();
                        try
                        {
                            populator.add( batch );
                        }
                        finally
                        {
                            updateLock.readLock().unlock();
                        }
                        lastBatches.set( slot, batch );
                    }
                }
                finally
                {
                    // This helps the updater know when to stop updating
                    insertersDone.countDown();
                }
            } ), 1 );
        }
        Collection<IndexEntryUpdate<?>> updates = new ArrayList<>();
        race.addContestant( throwing( () ->
        {
            Generator generator = new Generator( -1, random.seed() + threads, -1 );
            while ( insertersDone.getCount() > 0 )
            {
                // Do updates now and then
                Thread.sleep( 10 );
                if ( !canHandlePopulationConcurrentlyWithAdd )
                {
                    updateLock.writeLock().lock();
                }
                try ( IndexUpdater updater = populator.newPopulatingUpdater( nodePropertyAccessor ) )
                {
                    for ( int i = 0; i < threads; i++ )
                    {
                        List<? extends IndexEntryUpdate<?>> batch = lastBatches.get( i );
                        if ( batch != null )
                        {
                            IndexEntryUpdate<?> removal = batch.get( generator.random.nextInt( batch.size() ) );
                            IndexEntryUpdate<StoreIndexDescriptor> change = random.nextBoolean()
                                    ? IndexEntryUpdate.change( removal.getEntityId(), descriptor, removal.values(), generator.single() )
                                    : IndexEntryUpdate.remove( removal.getEntityId(), descriptor, removal.values() );
                            updater.process( change );
                            updates.add( change );
                        }
                    }
                }
                finally
                {
                    if ( !canHandlePopulationConcurrentlyWithAdd )
                    {
                        updateLock.writeLock().unlock();
                    }
                }
            }
        } ) );

        race.go();
        populator.close( true );
        populator = null; // to let the after-method know that we've closed it ourselves

        // then assert that a tree built by a single thread ends up exactly the same
        buildReferencePopulatorSingleThreaded( generators, batchesPerThread, updates );
        try ( IndexAccessor accessor = indexProvider.getOnlineAccessor( descriptor, samplingConfig );
              IndexAccessor referenceAccessor = indexProvider.getOnlineAccessor( descriptor2, samplingConfig );
              IndexReader reader = accessor.newReader();
              IndexReader referenceReader = referenceAccessor.newReader() )
        {
            NodeValueIterator entries = new NodeValueIterator();
            NodeValueIterator referenceEntries = new NodeValueIterator();
            reader.query( entries, IndexOrder.NONE, hasValues, IndexQuery.exists( 0 ) );
            referenceReader.query( referenceEntries, IndexOrder.NONE, hasValues, IndexQuery.exists( 0 ) );
            while ( referenceEntries.hasNext() )
            {
                assertTrue( entries.hasNext() );
                assertEquals( referenceEntries.next(), entries.next() );
                if ( hasValues )
                {
                    assertEquals( ValueTuple.of( referenceEntries.getValues() ), ValueTuple.of( entries.getValues() ) );
                }
            }
            assertFalse( entries.hasNext() );
        }
    }

    private void buildReferencePopulatorSingleThreaded( Generator[] generators, int batchesPerThread, Collection<IndexEntryUpdate<?>> updates )
            throws IndexEntryConflictException
    {
        IndexPopulator referencePopulator = indexProvider.getPopulator( descriptor2, samplingConfig );
        referencePopulator.create();
        boolean referenceSuccess = false;
        try
        {
            for ( Generator generator : generators )
            {
                generator.reset();
                for ( int i = 0; i < batchesPerThread; i++ )
                {
                    referencePopulator.add( generator.batch() );
                }
            }
            // Updates can be applied afterwards due to each generator having its own entityId ID space and index is non-unique
            try ( IndexUpdater updater = referencePopulator.newPopulatingUpdater( nodePropertyAccessor ) )
            {
                for ( IndexEntryUpdate<?> update : updates )
                {
                    updater.process( update );
                }
            }
            referenceSuccess = true;
        }
        finally
        {
            referencePopulator.close( referenceSuccess );
        }
    }

    private class Generator
    {
        private final int maxBatchSize;
        private final long seed;
        private final long startEntityId;

        private Random random;
        private RandomValues randomValues;
        private long nextEntityId;

        Generator( int maxBatchSize, long seed, long startEntityId )
        {
            this.startEntityId = startEntityId;
            this.nextEntityId = startEntityId;
            this.maxBatchSize = maxBatchSize;
            this.seed = seed;
            reset();
        }

        private void reset()
        {
            random = new Random( seed );
            randomValues = RandomValues.create( random );
            nextEntityId = startEntityId;
        }

        Value[] single()
        {
            return new Value[]{randomValue( randomValues )};
        }

        List<? extends IndexEntryUpdate<?>> batch()
        {
            int n = random.nextInt( maxBatchSize ) + 1;
            List<IndexEntryUpdate<?>> updates = new ArrayList<>( n );
            for ( int i = 0; i < n; i++ )
            {
                updates.add( IndexEntryUpdate.add( nextEntityId++, descriptor, single() ) );
            }
            return updates;
        }
    }
}
