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
package org.neo4j.kernel.impl.index.schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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
import java.util.function.Function;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.SimpleNodeValueClient;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

import static org.apache.commons.lang3.ArrayUtils.toArray;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;
import static org.neo4j.kernel.impl.index.schema.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.storageengine.api.IndexEntryUpdate.change;
import static org.neo4j.storageengine.api.IndexEntryUpdate.remove;
import static org.neo4j.test.Race.throwing;

@PageCacheExtension
@ExtendWith( RandomExtension.class )
abstract class IndexPopulationStressTest
{
    private static final IndexProviderDescriptor PROVIDER = new IndexProviderDescriptor( "provider", "1.0" );
    private static final int THREADS = 50;
    private static final int MAX_BATCH_SIZE = 100;
    private static final int BATCHES_PER_THREAD = 100;

    @Inject
    private RandomRule random;
    @Inject
    PageCache pageCache;
    @Inject
    FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private final String name;
    private final boolean hasValues;
    private final Function<RandomValues, Value> valueGenerator;
    private final Function<IndexPopulationStressTest, IndexProvider> providerCreator;

    private IndexDescriptor descriptor;
    private IndexDescriptor descriptor2;
    private final IndexSamplingConfig samplingConfig = new IndexSamplingConfig( 1000, 0.2, true );
    private final NodePropertyAccessor nodePropertyAccessor = mock( NodePropertyAccessor.class );
    private IndexPopulator populator;
    private IndexProvider indexProvider;
    private boolean prevAccessCheck;

    IndexPopulationStressTest( String name, boolean hasValues,
        Function<RandomValues, Value> valueGenerator,
        Function<IndexPopulationStressTest, IndexProvider> providerCreator )
    {
        this.name = name;
        this.hasValues = hasValues;
        this.valueGenerator = valueGenerator;
        this.providerCreator = providerCreator;
    }

    IndexDirectoryStructure.Factory directory()
    {
        File storeDir = testDirectory.storeDir();
        return directoriesBySubProvider( directoriesByProvider( storeDir ).forProvider( PROVIDER ) );
    }

    @BeforeEach
    void setup() throws IOException, EntityNotFoundException
    {
        indexProvider = providerCreator.apply( this );
        descriptor = indexProvider.completeConfiguration( forSchema( forLabel( 0, 0 ), PROVIDER ).withName( "index_0" ).materialise( 0 ) );
        descriptor2 = indexProvider.completeConfiguration( forSchema( forLabel( 1, 0 ), PROVIDER ).withName( "index_1" ).materialise( 1 ) );
        fs.mkdirs( indexProvider.directoryStructure().rootDirectory() );
        populator = indexProvider.getPopulator( descriptor, samplingConfig, heapBufferFactory( (int) kibiBytes( 40 ) ) );
        when( nodePropertyAccessor.getNodePropertyValue( anyLong(), anyInt() ) ).thenThrow( UnsupportedOperationException.class );
        prevAccessCheck = UnsafeUtil.exchangeNativeAccessCheckEnabled( false );
    }

    @AfterEach
    void teardown()
    {
        UnsafeUtil.exchangeNativeAccessCheckEnabled( prevAccessCheck );
        if ( populator != null )
        {
            populator.close( true );
        }
    }

    @Test
    void stressIt() throws Throwable
    {
        Race race = new Race();
        AtomicReferenceArray<List<? extends IndexEntryUpdate<?>>> lastBatches = new AtomicReferenceArray<>( THREADS );
        Generator[] generators = new Generator[THREADS];

        populator.create();
        CountDownLatch insertersDone = new CountDownLatch( THREADS );
        ReadWriteLock updateLock = new ReentrantReadWriteLock( true );
        for ( int i = 0; i < THREADS; i++ )
        {
            race.addContestant( inserter( lastBatches, generators, insertersDone, updateLock, i ), 1 );
        }
        Collection<IndexEntryUpdate<?>> updates = new ArrayList<>();
        race.addContestant( updater( lastBatches, insertersDone, updateLock, updates ) );

        race.go();
        populator.close( true );
        populator = null; // to let the after-method know that we've closed it ourselves

        // then assert that a tree built by a single thread ends up exactly the same
        buildReferencePopulatorSingleThreaded( generators, updates );
        try ( IndexAccessor accessor = indexProvider.getOnlineAccessor( descriptor, samplingConfig );
            IndexAccessor referenceAccessor = indexProvider.getOnlineAccessor( descriptor2, samplingConfig );
            IndexReader reader = accessor.newReader();
            IndexReader referenceReader = referenceAccessor.newReader() )
        {
            SimpleNodeValueClient entries = new SimpleNodeValueClient();
            SimpleNodeValueClient referenceEntries = new SimpleNodeValueClient();
            reader.query( NULL_CONTEXT, entries, IndexOrder.NONE, hasValues, IndexQuery.exists( 0 ) );
            referenceReader.query( NULL_CONTEXT, referenceEntries, IndexOrder.NONE, hasValues, IndexQuery.exists( 0 ) );
            while ( referenceEntries.next() )
            {
                assertTrue( entries.next() );
                assertEquals( referenceEntries.reference, entries.reference );
                if ( hasValues )
                {
                    assertEquals( ValueTuple.of( referenceEntries.values ), ValueTuple.of( entries.values ) );
                }
            }
            assertFalse( entries.next() );
        }
    }

    private Runnable updater( AtomicReferenceArray<List<? extends IndexEntryUpdate<?>>> lastBatches, CountDownLatch insertersDone, ReadWriteLock updateLock,
        Collection<IndexEntryUpdate<?>> updates )
    {
        return throwing( () ->
        {
            // Entity ids that have been removed, so that additions can reuse them
            List<Long> removed = new ArrayList<>();
            RandomValues randomValues = RandomValues.create( new Random( random.seed() + THREADS ) );
            while ( insertersDone.getCount() > 0 )
            {
                // Do updates now and then
                Thread.sleep( 10 );
                updateLock.writeLock().lock();
                try ( IndexUpdater updater = populator.newPopulatingUpdater( nodePropertyAccessor ) )
                {
                    for ( int i = 0; i < THREADS; i++ )
                    {
                        List<? extends IndexEntryUpdate<?>> batch = lastBatches.get( i );
                        if ( batch != null )
                        {
                            IndexEntryUpdate<?> update = null;
                            switch ( randomValues.nextInt( 3 ) )
                            {
                            case 0: // add
                                if ( !removed.isEmpty() )
                                {
                                    Long id = removed.remove( randomValues.nextInt( removed.size() ) );
                                    update = add( id, descriptor, valueGenerator.apply( randomValues ) );
                                }
                                break;
                            case 1: // remove
                                IndexEntryUpdate<?> removal = batch.get( randomValues.nextInt( batch.size() ) );
                                update = remove( removal.getEntityId(), descriptor, removal.values() );
                                removed.add( removal.getEntityId() );
                                break;
                            case 2: // change
                                removal = batch.get( randomValues.nextInt( batch.size() ) );
                                change( removal.getEntityId(), descriptor, removal.values(), toArray( valueGenerator.apply( randomValues ) ) );
                                break;
                            default:
                                throw new IllegalArgumentException();
                            }
                            if ( update != null )
                            {
                                updater.process( update );
                                updates.add( update );
                            }
                        }
                    }
                }
                finally
                {
                    updateLock.writeLock().unlock();
                }
            }
        } );
    }

    private Runnable inserter( AtomicReferenceArray<List<? extends IndexEntryUpdate<?>>> lastBatches, Generator[] generators, CountDownLatch insertersDone,
        ReadWriteLock updateLock, int slot )
    {
        int worstCaseEntriesPerThread = BATCHES_PER_THREAD * MAX_BATCH_SIZE;
        return throwing( () ->
        {
            try
            {
                Generator generator = generators[slot] = new Generator( MAX_BATCH_SIZE, random.seed() + slot, slot * worstCaseEntriesPerThread );
                for ( int j = 0; j < BATCHES_PER_THREAD; j++ )
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
        } );
    }

    private void buildReferencePopulatorSingleThreaded( Generator[] generators, Collection<IndexEntryUpdate<?>> updates )
        throws IndexEntryConflictException
    {
        IndexPopulator referencePopulator = indexProvider.getPopulator( descriptor2, samplingConfig, heapBufferFactory( (int) kibiBytes( 40 ) ) );
        referencePopulator.create();
        boolean referenceSuccess = false;
        try
        {
            for ( Generator generator : generators )
            {
                generator.reset();
                for ( int i = 0; i < BATCHES_PER_THREAD; i++ )
                {
                    referencePopulator.add( generator.batch() );
                }
            }
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
            randomValues = RandomValues.create( new Random( seed ) );
            nextEntityId = startEntityId;
        }

        List<? extends IndexEntryUpdate<?>> batch()
        {
            int n = randomValues.nextInt( maxBatchSize ) + 1;
            List<IndexEntryUpdate<?>> updates = new ArrayList<>( n );
            for ( int i = 0; i < n; i++ )
            {
                updates.add( add( nextEntityId++, descriptor, valueGenerator.apply( randomValues ) ) );
            }
            return updates;
        }
    }
}
