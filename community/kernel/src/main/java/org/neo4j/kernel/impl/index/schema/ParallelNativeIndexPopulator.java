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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexSample;

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
    private NativeIndexPopulator<KEY,VALUE> completePopulator;
    private String failure;
    private volatile NodePropertyAccessor propertyAccessor;
    // There are various access points considered to be the "first" after population is completed,
    // be it verifyDeferredConstraints, sampleResult or some other call. Regardless all of those methods
    // have to be able to merge the parts into the real index. This is what this flag is all about.
    private volatile boolean merged;
    // First thing in close(boolean) call is to set this flag with the sole purpose of preventing new parts
    // from being created beyond that point.
    private volatile boolean closed;

    ParallelNativeIndexPopulator( File baseIndexFile, IndexLayout<KEY,VALUE> layout, Function<File,NativeIndexPopulator<KEY,VALUE>> populatorSupplier )
    {
        this.layout = layout;
        this.threadLocalPopulators = new ThreadLocal<ThreadLocalPopulator>()
        {
            @Override
            protected synchronized ThreadLocalPopulator initialValue()
            {
                if ( closed )
                {
                    throw new IllegalStateException( "Already closed" );
                }

                File file = new File( baseIndexFile + "-part-" + partPopulators.size() );
                NativeIndexPopulator<KEY,VALUE> populator = populatorSupplier.apply( file );
                ThreadLocalPopulator tlPopulator = new ThreadLocalPopulator( populator );
                partPopulators.add( tlPopulator );
                populator.create();
                return tlPopulator;
            }
        };
        this.completePopulator = populatorSupplier.apply( baseIndexFile );
    }

    @Override
    public void create()
    {
        // Do the "create" logic that the populator normally does, which may include archiving of the existing failed index etc.
        completePopulator.create();
    }

    @Override
    public void drop()
    {
        partPopulators.forEach( p -> p.populator.drop() );
        completePopulator.drop();
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
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor )
    {
        ensureMerged();
        partPopulators.forEach( p -> p.populator.verifyDeferredConstraints( nodePropertyAccessor ) );
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
    {
        // Don't have an explicit updatesPopulator, instead record these updates and then each populator will have to apply next time they notice.
        propertyAccessor = accessor;
        return new CollectingIndexUpdater<KEY,VALUE>()
        {
            @Override
            protected void apply( Collection<IndexEntryUpdate<?>> updates )
            {
                // Ensure there's at least one part populator active. This is for a case where an index population is started
                // and the only data coming in is from the populating updater.
                if ( partPopulators.isEmpty() )
                {
                    threadLocalPopulators.get();
                }
                partPopulators.forEach( p -> p.updates.add( updates ) );
            }
        };
    }

    @Override
    public void close( boolean populationCompletedSuccessfully )
    {
        closed = true;
        try
        {
            if ( populationCompletedSuccessfully )
            {
                // We're already merged at this point, so it's only to close the complete tree
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
            partPopulators.forEach( p -> p.populator.drop() );
        }
    }

    private void mergeParts() throws IOException, IndexEntryConflictException
    {
        KEY low = layout.newKey();
        low.initialize( Long.MIN_VALUE );
        low.initValuesAsLowest();
        KEY high = layout.newKey();
        high.initialize( Long.MAX_VALUE );
        high.initValuesAsHighest();
        KEY end = layout.newKey();
        NativeIndexReader<KEY,VALUE>[] partReaders = new NativeIndexReader[partPopulators.size()];
        RawCursor<Hit<KEY,VALUE>,IOException>[] partCursors = new RawCursor[partPopulators.size()];
        Object[] partHeads = new Object[partPopulators.size()];
        int ended = 0;
        for ( int i = 0; i < partPopulators.size(); i++ )
        {
            ThreadLocalPopulator tlPopulator = partPopulators.get( i );
            // Apply pending updates in this populator thread
            tlPopulator.applyQueuedUpdates();
            NativeIndexReader<KEY,VALUE> reader = tlPopulator.populator.newReader();
            partReaders[i] = reader;
            partCursors[i] = reader.makeIndexSeeker( low, high, IndexOrder.ASCENDING );
        }

        try ( Writer<KEY,VALUE> writer = completePopulator.tree.writer() )
        {
            // An idea how to parallelize the below loop:
            // - Have one thread running ahead, making comparisons and leaving a trail of candidateIndexes behind it.
            // - The thread doing the merge gets batches of candidate indexes and picks and writes w/o comparing

            // As long there's stuff left to merge
            while ( ended < partCursors.length )
            {
                // Pick lowest among all candidates
                KEY lowestCandidate = null;
                int lowestCandidateIndex = -1;
                for ( int i = 0; i < partCursors.length; i++ )
                {
                    KEY candidate = (KEY) partHeads[i];
                    if ( candidate == end )
                    {
                        continue;
                    }

                    if ( candidate == null )
                    {
                        if ( partCursors[i].next() )
                        {
                            partHeads[i] = candidate = partCursors[i].get().key();
                        }
                        else
                        {
                            partHeads[i] = end;
                            ended++;
                        }
                    }
                    if ( candidate != null )
                    {
                        if ( lowestCandidate == null || layout.compare( candidate, lowestCandidate ) < 0 )
                        {
                            lowestCandidate = candidate;
                            lowestCandidateIndex = i;
                        }
                    }
                }

                if ( lowestCandidate != null )
                {
                    // Oh, we have something to insert
                    writer.put( lowestCandidate, partCursors[lowestCandidateIndex].get().value() );
                    partHeads[lowestCandidateIndex] = null;
                }
            }
        }
        finally
        {
            for ( NativeIndexReader<KEY,VALUE> partReader : partReaders )
            {
                partReader.close();
            }
        }
    }

    @Override
    public void markAsFailed( String failure )
    {
        this.failure = failure;
        partPopulators.forEach( p -> p.populator.markAsFailed( failure ) );
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
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

    private void ensureMerged()
    {
        if ( !merged )
        {
            merged = true;
            try
            {
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

    /**
     * A thread-local NativeIndexPopulator with a queue of batched external updates. We keep these per thread because we
     * don't want the index populator main thread to apply updates to all the parts.
     */
    private class ThreadLocalPopulator
    {
        private final NativeIndexPopulator<KEY,VALUE> populator;
        // Main populator thread adds and the thread owning this thread-local populator polls
        private final Queue<Collection<IndexEntryUpdate<?>>> updates = new ConcurrentLinkedDeque<>();

        ThreadLocalPopulator( NativeIndexPopulator<KEY,VALUE> populator )
        {
            this.populator = populator;
        }

        void applyQueuedUpdates() throws IndexEntryConflictException
        {
            if ( !updates.isEmpty() )
            {
                try ( IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor ) )
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
