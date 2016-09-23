/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index.persson;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.BTreeHit;
import org.neo4j.index.SCIndex;
import org.neo4j.index.SCIndexDescription;
import org.neo4j.index.SCInserter;
import org.neo4j.index.btree.Index;
import org.neo4j.index.btree.RangePredicate;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static java.lang.Long.min;
import static java.lang.System.arraycopy;

import static org.neo4j.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.index.SCIndex.indexFileName;
import static org.neo4j.index.btree.RangePredicate.equalTo;

public class NativeLabelScanStore implements LabelScanStore
{
    private final SCIndex index;
    private final File indexFile;

    public NativeLabelScanStore( PageCache pageCache, File storeDir ) throws IOException
    {
        String name = "labelscan.db";
        indexFile = new File( storeDir, indexFileName( name ) );
        this.index = new Index( pageCache, indexFile,
                new SCIndexDescription( "", "", "", Direction.BOTH, "", null ), pageCache.pageSize() );
    }

    @Override
    public LabelScanReader newReader()
    {
        return new LabelScanReader()
        {
            @Override
            public void close()
            {
                // No
            }

            @Override
            public PrimitiveLongIterator nodesWithLabel( int labelId )
            {
                RangePredicate fromPredicate = equalTo( labelId, 0L );
                RangePredicate toPredicate = equalTo( labelId, Long.MAX_VALUE );
                Cursor<BTreeHit> cursor;
                try
                {
                    cursor = index.seek( fromPredicate, toPredicate );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }

                return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
                {
                    @Override
                    protected boolean fetchNext()
                    {
                        if ( !cursor.next() )
                        {
                            cursor.close();
                            return false;
                        }
                        final long nodeId = cursor.get().value()[0];
                        return next( nodeId );
                    }
                };
            }

            @Override
            public PrimitiveLongIterator labelsForNode( long nodeId )
            {
                throw new UnsupportedOperationException( "Use your db..." );
            }
        };
    }

    private static final Comparator<NodeLabelUpdate> UPDATE_SORTER = new Comparator<NodeLabelUpdate>()
    {
        @Override
        public int compare( NodeLabelUpdate o1, NodeLabelUpdate o2 )
        {
            return Long.compare( o1.getNodeId(), o2.getNodeId() );
        }
    };

    @Override
    public LabelScanWriter newWriter()
    {
        final SCInserter inserter;
        try
        {
            inserter = index.inserter();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        // TODO: There are lots of logic in here to be clever about allocations and
        // to sort and batch updates to favor the underlying index implementation.
        // It would be grand if we could extract this to package-access pieces of code
        // which could be tested individually.
        return new LabelScanWriter()
        {
            private final long[] key = new long[2];
            private final long[] value = new long[2];
            private long[] tmpBefore = new long[10];
            private long[] tmpAfter = new long[10];
            private final NodeLabelUpdate[] pendingUpdates = new NodeLabelUpdate[1000];
            private int pendingUpdatesCursor;
            private int pendingUpdatesCount; // there may be many per NodeLabelUpdate
            private long lowestLabelId = Long.MAX_VALUE;

            @Override
            public void write( NodeLabelUpdate update ) throws IOException
            {
                if ( pendingUpdatesCursor == pendingUpdates.length )
                {
                    flushPendingChanges();
                }

                pendingUpdates[pendingUpdatesCursor++] = update;
                convertToAdditionsAndRemovals( update );
                if ( update.getLabelsBefore().length > 0 )
                {
                    lowestLabelId = min( lowestLabelId, update.getLabelsBefore()[0] );
                }
                if ( update.getLabelsAfter().length > 0 )
                {
                    lowestLabelId = min( lowestLabelId, update.getLabelsAfter()[0] );
                }
            }

            /**
             * Converts physical before/after state to logical add/remove state. This conversion
             * reuses the existing long[] arrays in {@link NodeLabelUpdate}, merely shuffles numbers
             * around and possible terminates them with -1 because the logical change set can be
             * equally big or smaller than the physical change set.
             *
             * The change to logical add/remove state favors the batch loop when flushing later.
             * The logic is a bit complicated and should be tested in isolation.
             */
            private void convertToAdditionsAndRemovals( NodeLabelUpdate update )
            {
                int beforeLength = update.getLabelsBefore().length;
                int afterLength = update.getLabelsAfter().length;
                if ( tmpBefore.length < update.getLabelsBefore().length )
                {
                    tmpBefore = new long[update.getLabelsBefore().length];
                }
                if ( tmpAfter.length < update.getLabelsAfter().length )
                {
                    tmpAfter = new long[update.getLabelsAfter().length];
                }
                arraycopy( update.getLabelsBefore(), 0, tmpBefore, 0, beforeLength );
                arraycopy( update.getLabelsAfter(), 0, tmpAfter, 0, afterLength );

                int bc = 0, ac = 0;
                for ( int bi = 0, ai = 0; bi < beforeLength || ai < afterLength; )
                {
                    long beforeId = bi < beforeLength ? tmpBefore[bi] : -1;
                    long afterId = ai < afterLength ? tmpAfter[ai] : -1;
                    if ( beforeId == afterId )
                    {   // no change
                        bi++;
                        ai++;
                    }
                    else if ( beforeId < afterId || bi == -1 )
                    {   // only found in afterId == addition
                        update.getLabelsAfter()[ac++] = afterId;
                        pendingUpdatesCount++;
                        ai++;
                    }
                    else if ( afterId < beforeId || ai == -1 )
                    {   // only found in beforeId == removal
                        update.getLabelsBefore()[bc++] = beforeId;
                        pendingUpdatesCount++;
                        bi++;
                    }
                }

                terminateWithMinusOneIfNeeded( update.getLabelsBefore(), bc );
                terminateWithMinusOneIfNeeded( update.getLabelsAfter(), ac );
            }

            private void terminateWithMinusOneIfNeeded( long[] labelIds, int bc )
            {
                if ( bc < labelIds.length )
                {
                    labelIds[bc] = -1;
                }
            }

            private void flushPendingChanges() throws IOException
            {
                Arrays.sort( pendingUpdates, 0, pendingUpdatesCursor, UPDATE_SORTER );

                long currentLabelId = lowestLabelId;
                while ( pendingUpdatesCount > 0 )
                {
                    long nextLabelId = Long.MAX_VALUE;
                    for ( int i = 0; i < pendingUpdatesCursor; i++ )
                    {
                        NodeLabelUpdate update = pendingUpdates[i];
                        final long nodeId = update.getNodeId();
                        // Additions
                        for ( long labelId : update.getLabelsAfter() )
                        {
                            if ( labelId == -1 )
                            {
                                break;
                            }

                            if ( labelId > currentLabelId && labelId < nextLabelId )
                            {
                                nextLabelId = labelId;
                            }

                            if ( labelId == currentLabelId )
                            {
                                key[0] = labelId;
                                key[1] = nodeId;
                                value[0] = nodeId;
                                inserter.insert( key, value );
                                pendingUpdatesCount--;
                                break;
                            }
                        }
                        // Removals
                        for ( long labelId : update.getLabelsBefore() )
                        {
                            if ( labelId == -1 )
                            {
                                break;
                            }

                            if ( labelId > currentLabelId && labelId < nextLabelId )
                            {
                                nextLabelId = labelId;
                            }

                            if ( labelId == currentLabelId )
                            {
                                key[0] = labelId;
                                key[1] = nodeId;
                                value[0] = nodeId;
                                inserter.remove( key );
                                pendingUpdatesCount--;
                                break;
                            }
                        }
                    }
                    currentLabelId = nextLabelId;
                }
                pendingUpdatesCursor = 0;
            }

            @Override
            public void close() throws IOException
            {
                flushPendingChanges();
                inserter.close();
            }
        };
    }

    @Override
    public void force() throws UnderlyingStorageException
    {
        // No need, this call was made with Lucene in mind. Before call to LabelScanStore#force()
        // the page cache is also forced, so ignore this.
    }

    @Override
    public AllEntriesLabelScanReader allNodeLabelRanges()
    {
        return null;
    }

    @Override
    public ResourceIterator<File> snapshotStoreFiles() throws IOException
    {
        return asResourceIterator( iterator( indexFile ) );
    }

    @Override
    public void init() throws IOException
    {
    }

    @Override
    public void start() throws IOException
    {
    }

    @Override
    public void stop() throws IOException
    {
    }

    @Override
    public void shutdown() throws IOException
    {
        index.close();
    }
}
