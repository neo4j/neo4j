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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.helpers.collection.Iterables.safeForAll;
import static org.neo4j.io.IOUtils.closeAll;

/**
 * Takes a somewhat high-level approach to parallelizing index population. It could be done lower level and more efficiently,
 * but this may be good enough. Basically since multiple threads comes in and call add w/ batches of updates, each thread
 * builds its own tree and in the end a single thread merges all trees, which will be reasonably fast since everything is sorted,
 * into one complete tree.
 *
 * @param <KEY> keys in the tree backing this index
 * @param <VALUE> values the tree backing this index
 */
class ParallelNativeIndexPopulator<KEY extends NativeIndexKey<KEY>,VALUE extends NativeIndexValue> implements IndexPopulator, ConsistencyCheckableIndexPopulator
{
    private final IndexLayout<KEY,VALUE> layout;
    private final ThreadLocal<ThreadLocalPopulator> threadLocalPopulators;
    // Just a complete list of all part populators, because we can't ask ThreadLocal to provide this to us.
    private final List<ThreadLocalPopulator> partPopulators = new CopyOnWriteArrayList<>();
    private final AtomicInteger nextPartId = new AtomicInteger();
    private NativeIndexPopulator<KEY,VALUE> completePopulator;
    private String failure;
    // There are various access points considered to be the "first" after population is completed,
    // be it verifyDeferredConstraints, sampleResult or some other call. Regardless all of those methods
    // have to be able to merge the parts into the real index. This is what this flag is all about.
    private boolean merged;
    // First thing in close(boolean)/drop call is to set this flag with the sole purpose of preventing new parts
    // from being created beyond that point. Accessing methods are synchronized.
    private boolean closed;

    ParallelNativeIndexPopulator( File baseIndexFile, IndexLayout<KEY,VALUE> layout, NativeIndexPopulatorPartSupplier<KEY,VALUE> partSupplier )
    {
        this.layout = layout;
        this.threadLocalPopulators = ThreadLocal.withInitial( () -> newPartPopulator( baseIndexFile, partSupplier ) );
        this.completePopulator = partSupplier.part( baseIndexFile );
    }

    private synchronized ThreadLocalPopulator newPartPopulator( File baseIndexFile, NativeIndexPopulatorPartSupplier<KEY,VALUE> partSupplier )
    {
        if ( closed )
        {
            throw new IllegalStateException( "Already closed" );
        }
        if ( merged )
        {
            throw new IllegalStateException( "Already merged" );
        }

        File file = new File( baseIndexFile + "-part-" + nextPartId.getAndIncrement() );
        NativeIndexPopulator<KEY,VALUE> populator = partSupplier.part( file );
        ThreadLocalPopulator tlPopulator = new ThreadLocalPopulator( populator );
        partPopulators.add( tlPopulator );
        populator.create();
        return tlPopulator;
    }

    @Override
    public void create()
    {
        // Do the "create" logic that the populator normally does, which may include archiving of the existing failed index etc.
        completePopulator.create();
    }

    @Override
    public synchronized void drop()
    {
        closed = true;
        try
        {
            closeAndDropAllParts();
        }
        finally
        {
            completePopulator.drop();
        }
    }

    private void closeAndDropAllParts()
    {
        safeForAll( p -> p.populator.drop(), partPopulators );
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> scanBatch ) throws IndexEntryConflictException
    {
        ThreadLocalPopulator tlPopulator = threadLocalPopulators.get();

        // First check if there are external updates to apply
        tlPopulator.applyQueuedUpdates();

        // Then apply the updates from the scan
        tlPopulator.populator.add( scanBatch );
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
    {
        ensureMerged();
        completePopulator.verifyDeferredConstraints( nodePropertyAccessor );
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
    {
        // Native index populators don't make use of NodePropertyAccessor, so just ignore it

        // Don't have an explicit updatesPopulator, instead record these updates and then each populator will have to apply next time they notice.
        return new CollectingIndexUpdater( updates ->
        {
            // Ensure there's at least one part populator active. This is for a case where an index population is started
            // and the only data coming in is from the populating updater.
            if ( partPopulators.isEmpty() )
            {
                threadLocalPopulators.get();
            }
            partPopulators.forEach( p -> p.updates.add( updates ) );
        } );
    }

    @Override
    public synchronized void close( boolean populationCompletedSuccessfully )
    {
        closed = true;
        try
        {
            if ( populationCompletedSuccessfully )
            {
                ensureMerged();
                completePopulator.close( true );
            }
            else
            {
                // We shouldn't be merged at this point, so clean up all the things
                if ( failure != null )
                {
                    // failure can be null e.g. when dropping an index while it's populating, in that case it's just a close(false) call.
                    completePopulator.markAsFailed( failure );
                }
                completePopulator.close( false );
            }
        }
        finally
        {
            closeAndDropAllParts();
        }
    }

    @Override
    public void markAsFailed( String failure )
    {
        this.failure = failure;
        completePopulator.markAsFailed( failure );
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        completePopulator.includeSample( update );
    }

    @Override
    public IndexSample sampleResult()
    {
        ensureMerged();
        return completePopulator.sampleResult();
    }

    @Override
    public void consistencyCheck()
    {
        ensureMerged();
        completePopulator.consistencyCheck();
    }

    /**
     * Will ensure that the merge have been done. This is a method which is called in several places because depending on population
     * parameters and scenarios different methods of this populator will be considered the first method after population to operate
     * on the complete index.
     */
    private synchronized void ensureMerged()
    {
        if ( !merged )
        {
            merged = true;
            try
            {
                applyAllPendingUpdates();
                mergeParts();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
            catch ( IndexEntryConflictException e )
            {
                throw new IllegalStateException( e );
            }
        }
    }

    private void mergeParts() throws IOException
    {
        KEY from = layout.newKey();
        KEY to = layout.newKey();
        initKeysAsLowestAndHighest( from, to );
        try ( Writer<KEY,VALUE> writer = completePopulator.tree.writer();
              CombinedPartSeeker<KEY,VALUE> combinedPartSeeker = new CombinedPartSeeker<>( layout, partSeekers( from, to ) ) )
        {
            while ( combinedPartSeeker.next() )
            {
                writer.put( combinedPartSeeker.key(), combinedPartSeeker.value() );
            }
        }
    }

    private List<RawCursor<Hit<KEY,VALUE>,IOException>> partSeekers( KEY from, KEY to ) throws IOException
    {
        List<RawCursor<Hit<KEY,VALUE>,IOException>> seekers = new ArrayList<>();
        boolean success = false;
        try
        {
            for ( ThreadLocalPopulator partPopulator : partPopulators )
            {
                seekers.add( partPopulator.populator.tree.seek( from, to ) );
            }
            success = true;
            return seekers;
        }
        finally
        {
            if ( !success )
            {
                closeAll( seekers );
            }
        }
    }

    private void initKeysAsLowestAndHighest( KEY low, KEY high )
    {
        low.initialize( Long.MIN_VALUE );
        low.initValuesAsLowest();
        high.initialize( Long.MAX_VALUE );
        high.initValuesAsHighest();
    }

    private void applyAllPendingUpdates() throws IndexEntryConflictException
    {
        for ( ThreadLocalPopulator part : partPopulators )
        {
            part.applyQueuedUpdates();
        }
    }

    @VisibleForTesting
    NativeIndexReader<KEY,VALUE> newReader()
    {
        return completePopulator.newReader();
    }

    /**
     * A thread-local NativeIndexPopulator with a queue of batched external updates. We keep these per thread because we
     * don't want the index populator main thread to apply updates to all the parts.
     */
    private class ThreadLocalPopulator
    {
        private final NativeIndexPopulator<KEY,VALUE> populator;
        // Main populator thread adds and the thread owning this thread-local populator polls
        private final Queue<Collection<IndexEntryUpdate<?>>> updates = new ConcurrentLinkedQueue<>();

        ThreadLocalPopulator( NativeIndexPopulator<KEY,VALUE> populator )
        {
            this.populator = populator;
        }

        void applyQueuedUpdates() throws IndexEntryConflictException
        {
            if ( !updates.isEmpty() )
            {
                try ( IndexUpdater updater = populator.newPopulatingUpdater() )
                {
                    Collection<IndexEntryUpdate<?>> batch;
                    while ( (batch = updates.poll()) != null )
                    {
                        for ( IndexEntryUpdate<?> update : batch )
                        {
                            updater.process( update );
                        }
                    }
                }
            }
        }
    }
}
