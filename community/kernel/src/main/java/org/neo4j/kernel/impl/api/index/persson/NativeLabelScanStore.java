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
import org.neo4j.index.SCIndexDescription;
import org.neo4j.index.SCInserter;
import org.neo4j.index.ValueAmender;
import org.neo4j.index.btree.CompactLabelScanLayout;
import org.neo4j.index.btree.Index;
import org.neo4j.index.btree.LabelScanKey;
import org.neo4j.index.btree.LabelScanValue;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static java.lang.Long.min;
import static java.lang.Math.toIntExact;
import static java.lang.System.arraycopy;

import static org.neo4j.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.helpers.collection.Iterators.iterator;

public class NativeLabelScanStore implements LabelScanStore
{
    private final Index<LabelScanKey,LabelScanValue> index;
    private final File indexFile;
    private ValueAmender<LabelScanValue> addAmender;
    private ValueAmender<LabelScanValue> removeAmender;
    private int rangeSize;

    public NativeLabelScanStore( PageCache pageCache, File storeDir ) throws IOException
    {
        String name = "labelscan.db";
        indexFile = new File( storeDir, name );
        rangeSize = Integer.SIZE;
        this.index = new Index<>( pageCache, indexFile, new CompactLabelScanLayout( rangeSize ),
                new SCIndexDescription( "", "", "", Direction.BOTH, "", null ), pageCache.pageSize() );
        this.addAmender = new ValueAmender<LabelScanValue>()
        {
            @Override
            public LabelScanValue amend( LabelScanValue value, LabelScanValue withValue )
            {
                // every set bit means set
                value.bits |= withValue.bits;
                return value;
            }
        };
        this.removeAmender = new ValueAmender<LabelScanValue>()
        {
            @Override
            public LabelScanValue amend( LabelScanValue value, LabelScanValue withValue )
            {
                // every set bit means clear
                value.bits &= ~withValue.bits;
                return value;
            }
        };
    }

    @Override
    public LabelScanReader newReader()
    {
        return new LabelScanReader()
        {
            private Cursor<BTreeHit<LabelScanKey,LabelScanValue>> cursor;

            @Override
            public void close()
            {
                if ( cursor != null )
                {
                    cursor.close();
                }
            }

            @Override
            public PrimitiveLongIterator nodesWithLabel( int labelId )
            {
                LabelScanKey from = new LabelScanKey().set( labelId, 0 );
                LabelScanKey to = new LabelScanKey().set( labelId, Long.MAX_VALUE );
                try
                {
                    if ( cursor != null )
                    {
                        cursor.close();
                    }
                    cursor = index.seek( from, to );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }

                return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
                {
                    private long baseNodeId;
                    private long bits;

                    @Override
                    protected boolean fetchNext()
                    {
                        while ( true )
                        {
                            if ( bits != 0 )
                            {
                                return nextFromCurrent();
                            }

                            if ( !cursor.next() )
                            {
                                return false;
                            }

                            BTreeHit<LabelScanKey,LabelScanValue> hit = cursor.get();
                            baseNodeId = hit.key().nodeId * rangeSize;
                            bits = hit.value().bits;
                        }
                    }

                    private boolean nextFromCurrent()
                    {
                        int delta = Long.numberOfTrailingZeros( bits );
                        bits &= bits-1;
                        return next( baseNodeId + delta );
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
        final SCInserter<LabelScanKey,LabelScanValue> inserter;
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
            private final LabelScanKey key = new LabelScanKey();
            private final LabelScanValue value = new LabelScanValue();
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
                        continue;
                    }

                    if ( smaller( beforeId, afterId ) )
                    {
                        while ( smaller( beforeId, afterId ) && bi < beforeLength )
                        {
                            // looks like there's an id in before which isn't in after ==> REMOVE
                            update.getLabelsBefore()[bc++] = beforeId;
                            pendingUpdatesCount++;
                            bi++;
                            beforeId = bi < beforeLength ? tmpBefore[bi] : -1;
                        }
                    }
                    else if ( smaller( afterId, beforeId ) )
                    {
                        while ( smaller( afterId, beforeId ) && ai < afterLength )
                        {
                            // looks like there's an id in after which isn't in before ==> ADD
                            update.getLabelsAfter()[ac++] = afterId;
                            pendingUpdatesCount++;
                            ai++;
                            afterId = ai < afterLength ? tmpAfter[ai] : -1;
                        }
                    }
                }

                terminateWithMinusOneIfNeeded( update.getLabelsBefore(), bc );
                terminateWithMinusOneIfNeeded( update.getLabelsAfter(), ac );
                if ( bc > 0 )
                {
                    lowestLabelId = min( lowestLabelId, update.getLabelsBefore()[0] );
                }
                if ( ac > 0 )
                {
                    lowestLabelId = min( lowestLabelId, update.getLabelsAfter()[0] );
                }
            }

            private boolean smaller( long id, long otherId )
            {
                if ( id == -1 )
                {
                    return false;
                }
                if ( otherId == -1 )
                {
                    return true;
                }
                return id < otherId;
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
                        long[] labelsAfter = update.getLabelsAfter();
                        for ( int li = 0; li < labelsAfter.length; li++ )
                        {
                            long labelId = labelsAfter[li];
                            if ( labelId == -1 )
                            {
                                break;
                            }

                            // Have this check here so that we can pick up the next labelId in our change set
                            if ( labelId == currentLabelId )
                            {
                                inserter.insert( key.set( toIntExact( labelId ), rangeOf( nodeId ) ),
                                        nodeValue( nodeId ), addAmender );
                                pendingUpdatesCount--;

                                // We can do a little shorter check for next labelId here straight away,
                                // we just check the next if it's less than what we currently think is next labelId
                                // and then break right after
                                if ( li+1 < labelsAfter.length && labelsAfter[li+1] != -1 )
                                {
                                    nextLabelId = min( nextLabelId, labelsAfter[li+1] );
                                }
                                break;
                            }
                            else if ( labelId > currentLabelId )
                            {
                                nextLabelId = min( nextLabelId, labelId );
                            }
                        }
                        // Removals
                        long[] labelsBefore = update.getLabelsBefore();
                        for ( int li = 0; li < labelsBefore.length; li++ )
                        {
                            long labelId = labelsBefore[li];
                            if ( labelId == -1 )
                            {
                                break;
                            }

                            if ( labelId == currentLabelId )
                            {
                                // A removal is now actually an insert (with custom amender)
                                inserter.insert( key.set( toIntExact( labelId ), rangeOf( nodeId ) ),
                                        nodeValue( nodeId ), removeAmender );
                                // TODO: special case -- if tree node now is empty, then consider removing
                                pendingUpdatesCount--;

                                // We can do a little shorter check for next labelId here straight away,
                                // we just check the next if it's less than what we currently think is next labelId
                                // and then break right after
                                if ( li+1 < labelsBefore.length && labelsBefore[li+1] != -1 )
                                {
                                    nextLabelId = min( nextLabelId, labelsBefore[li+1] );
                                }
                            }
                            else if ( labelId > currentLabelId && labelId < nextLabelId )
                            {
                                nextLabelId = min( nextLabelId, labelId );
                            }
                        }
                    }
                    currentLabelId = nextLabelId;
                }
                pendingUpdatesCursor = 0;
            }

            private LabelScanValue nodeValue( long nodeId )
            {
                int rest = (int) nodeId % rangeSize;
                value.bits = (1L << rest);
                return value;
            }

            @Override
            public void close() throws IOException
            {
                flushPendingChanges();
                inserter.close();
            }
        };
    }

    protected long rangeOf( long nodeId )
    {
        return nodeId / rangeSize;
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
