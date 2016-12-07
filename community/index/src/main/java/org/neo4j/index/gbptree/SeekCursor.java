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
package org.neo4j.index.gbptree;

import java.io.IOException;
import java.util.function.LongSupplier;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.Hit;
import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.gbptree.PageCursorUtil.checkOutOfBounds;

/**
 * {@link RawCursor} over tree leaves, making keys/values accessible to user. Given a starting leaf
 * and key range this cursor traverses each leaf and its right siblings as long as visited keys are within
 * key range. Each visited key within the key range can be accessible using {@link #get()}.
 * The key/value instances provided by {@link Hit} instance are mutable and overwritten with new values
 * for every call to {@link #next()} so user cannot keep references to key/value instances, expecting them
 * to keep their values intact.
 * <p>
 * Concurrent writes can happen in the visited nodes and tree structure may change. This implementation
 * guards for that by re-reading if change happens underneath, but will not provide a consistent view of
 * the data as it were when the seek starts, i.e. doesn't support MVCC-style.
 * <p>
 * Implementation note: there are assumptions that keys are unique in the tree.
 */
class SeekCursor<KEY,VALUE> implements RawCursor<Hit<KEY,VALUE>,IOException>, Hit<KEY,VALUE>
{
    private final PageCursor cursor;
    private final KEY mutableKey;
    private final VALUE mutableValue;
    private final KEY fromInclusive;
    private final KEY toExclusive;
    private final Layout<KEY,VALUE> layout;
    private final TreeNode<KEY,VALUE> bTreeNode;
    private final KEY prevKey;
    private final LongSupplier generationSupplier;
    private boolean first = true;
    private long stableGeneration;
    private long unstableGeneration;

    // data structures for the current b-tree node
    private int pos;
    private int keyCount;
    private boolean rereadHeader;
    private boolean rediscoverKeyPosition;

    SeekCursor( PageCursor leafCursor, KEY mutableKey, VALUE mutableValue, TreeNode<KEY,VALUE> bTreeNode,
            KEY fromInclusive, KEY toExclusive, Layout<KEY,VALUE> layout,
            long stableGeneration, long unstableGeneration, LongSupplier generationSupplier ) throws IOException
    {
        this.cursor = leafCursor;
        this.mutableKey = mutableKey;
        this.mutableValue = mutableValue;
        this.fromInclusive = fromInclusive;
        this.toExclusive = toExclusive;
        this.layout = layout;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
        this.generationSupplier = generationSupplier;
        this.bTreeNode = bTreeNode;
        this.prevKey = layout.newKey();

        traverseDownToFirstLeaf();
    }

    private void traverseDownToFirstLeaf() throws IOException
    {
        long newGen;
        boolean isInternal;
        int keyCount;
        long childId = 0; // initialized to satisfy compiler
        int pos;
        do
        {
            do
            {
                newGen = bTreeNode.newGen( cursor, stableGeneration, unstableGeneration );
                isInternal = bTreeNode.isInternal( cursor );
                // Find the left-most key within from-range
                keyCount = bTreeNode.keyCount( cursor );
                int search = KeySearch.search( cursor, bTreeNode, fromInclusive, mutableKey, keyCount );
                pos = KeySearch.positionOf( search );

                // Assuming unique keys
                if ( isInternal && KeySearch.isHit( search ) )
                {
                    pos++;
                }

                if ( isInternal )
                {
                    childId = bTreeNode.childAt( cursor, pos, stableGeneration, unstableGeneration );
                }
            }
            while ( cursor.shouldRetry() );
            checkOutOfBounds( cursor );

            if ( pointerCheckingWithGenerationCatchup( newGen, true ) )
            {
                continue;
            }
            else if ( TreeNode.isNode( newGen ) )
            {
                bTreeNode.goTo( cursor, "new gen", newGen, stableGeneration, unstableGeneration );
                continue;
            }

            if ( isInternal )
            {
                if ( pointerCheckingWithGenerationCatchup( childId, false ) )
                {
                    continue;
                }

                bTreeNode.goTo( cursor, "child", childId, stableGeneration, unstableGeneration );
            }
        }
        while ( isInternal && keyCount > 0 );

        // We've now come to the first relevant leaf, initialize the state for the coming leaf scan
        this.pos = pos - 1;
        this.keyCount = keyCount;
    }

    @Override
    public Hit<KEY,VALUE> get()
    {
        return this;
    }

    @Override
    public boolean next() throws IOException
    {
        while ( true )
        {
            pos++;
            long newGen;
            long rightSibling = -1; // initialized to satisfy the compiler
            do
            {
                newGen = bTreeNode.newGen( cursor, stableGeneration, unstableGeneration );
                if ( rereadHeader )
                {
                    rereadCurrentNodeHeader();
                }
                if ( rediscoverKeyPosition )
                {
                    rediscoverKeyPosition();
                }
                // There's a condition in here, choosing between go to next sibling or value,
                // this condition is mirrored outside the shouldRetry loop to act upon the data
                // which has by then been consistently read. No decision can be made in here directly.
                if ( pos >= keyCount )
                {
                    // Read right sibling
                    rightSibling = bTreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
                }
                else
                {
                    // Read the next value in this leaf
                    bTreeNode.keyAt( cursor, mutableKey, pos );
                    bTreeNode.valueAt( cursor, mutableValue, pos );
                }
            }
            while ( rediscoverKeyPosition = rereadHeader = cursor.shouldRetry() );
            checkOutOfBounds( cursor );

            // Go to newGen if read successfully and
            if ( pointerCheckingWithGenerationCatchup( newGen, true ) )
            {
                // Reading newGen pointer resulted in a bad read, but generation had changed (a checkpoint has
                // occurred since we started this cursor) so the generation fields in this cursor are now updated
                // with the latest, so let's try that read again.
                rereadHeader = rediscoverKeyPosition = true;
                continue;
            }
            else if ( TreeNode.isNode( newGen ) )
            {
                // We ended up on a node which has a newGen set, let's go to it and read from that one instead.
                bTreeNode.goTo( cursor, "new gen", newGen, stableGeneration, unstableGeneration );
                rereadHeader = rediscoverKeyPosition = true;
                continue;
            }

            if ( pos >= keyCount )
            {
                // We've exhausted this node, it's time to see if there's a right sibling to go to.

                if ( pointerCheckingWithGenerationCatchup( rightSibling, true ) )
                {
                    // Reading rightSibling pointer resulted in a bad read, but generation had changed
                    // (a checkpoint has occurred since we started this cursor) so the generation fields in this
                    // cursor are now updated with the latest, so let's try that read again.
                    rereadHeader = rediscoverKeyPosition = true;
                    continue;
                }
                else if ( TreeNode.isNode( rightSibling ) )
                {
                    // TODO: Check if rightSibling is within expected range before calling next.
                    // TODO: Possibly by getting highest expected from IdProvider
                    bTreeNode.goTo( cursor, "right sibling", rightSibling, stableGeneration, unstableGeneration );
                    if ( first )
                    {
                        // Have not yet found first hit among leaves.
                        // First hit can be several leaves to the right.
                        // Continue to use binary search in right leaf
                        rereadHeader = rediscoverKeyPosition = true;
                    }
                    else
                    {
                        // It is likely that first key in right sibling is a next hit.
                        // Continue using scan
                        pos = -1;
                        rereadHeader = true;
                    }
                    continue; // in the outer loop, with the position reset to the beginning of the right sibling
                }
            }
            else if ( layout.compare( mutableKey, toExclusive ) < 0 )
            {
                if ( layout.compare( mutableKey, fromInclusive ) < 0 )
                {
                    // too far to the left possibly because page reuse
                    rereadHeader = rediscoverKeyPosition = true;
                    continue;
                }
                else if ( !first && layout.compare( prevKey, mutableKey ) >= 0 )
                {
                    // We've come across a bad read in the middle of a split
                    // This is outlined in InternalTreeLogic, skip this value (it's fine)
                    rereadHeader = true;
                    continue;
                }

                // A hit, it's within the range we search for
                if ( first )
                {
                    // Setting first to false include an additional check for coming potential
                    // hits so that we cannot go backwards in our result. Going backwards can
                    // happen when reading through concurrent splits or similar and is a benign
                    // temporary observed state.
                    first = false;
                }
                layout.copyKey( mutableKey, prevKey );
                return true;
            }
            // We've come too far and so this means the end of the result set
            return false;
        }
    }

    private void rereadCurrentNodeHeader()
    {
        keyCount = bTreeNode.keyCount( cursor );
    }

    private void rediscoverKeyPosition()
    {
        // Keys could have been moved to the left so we need to make sure we are not missing any keys by
        // moving position back until we find previously returned key
        if ( !first )
        {
            int searchResult = KeySearch.search( cursor, bTreeNode, prevKey, mutableKey, keyCount );
            pos = KeySearch.positionOf( searchResult );
        }
        else
        {
            pos = 0;
        }
    }

    private boolean pointerCheckingWithGenerationCatchup( long pointer, boolean allowNoNode )
    {
        if ( !GenSafePointerPair.isSuccess( pointer ) )
        {
            // An unexpected sibling read, this could have been caused by a concurrent checkpoint
            // where generation has been incremented. Re-read generation and, if changed since this
            // seek started then update generation locally
            long newGeneration = generationSupplier.getAsLong();
            long newStableGeneration = Generation.stableGeneration( newGeneration );
            long newUnstableGeneration = Generation.unstableGeneration( newGeneration );
            if ( newStableGeneration != stableGeneration || newUnstableGeneration != unstableGeneration )
            {
                stableGeneration = newStableGeneration;
                unstableGeneration = newUnstableGeneration;
                return true;
            }
            PointerChecking.checkPointer( pointer, allowNoNode );
        }
        return false;
    }

    @Override
    public KEY key()
    {
        return mutableKey;
    }

    @Override
    public VALUE value()
    {
        return mutableValue;
    }

    @Override
    public void close()
    {
        cursor.close();
    }
}
