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
import java.util.function.Supplier;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.Hit;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.Integer.max;

import static org.neo4j.index.gbptree.PageCursorUtil.checkOutOfBounds;
import static org.neo4j.index.gbptree.TreeNode.NODE_TYPE_TREE_NODE;

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
    /**
     * Cursor for reading from tree nodes and also will be moved around when following pointers.
     */
    private final PageCursor cursor;

    /**
     * Key instance to use for reading keys from current node.
     */
    private final KEY mutableKey;

    /**
     * Value instances to use for reading values from current node.
     */
    private final VALUE mutableValue;

    /**
     * Provided when constructing the {@link SeekCursor}, marks the low end of the key range to seek.
     */
    private final KEY fromInclusive;

    /**
     * Provided when constructing the {@link SeekCursor}, marks the high end (exclusive) of the key range to seek.
     */
    private final KEY toExclusive;

    /**
     * {@link Layout} instance used to perform some functions around keys, like copying and comparing.
     */
    private final Layout<KEY,VALUE> layout;

    /**
     * Logic for reading data from tree nodes.
     */
    private final TreeNode<KEY,VALUE> bTreeNode;

    /**
     * Contains the highest returned key, i.e. from the last call to {@link #next()} returning {@code true}.
     */
    private final KEY prevKey;

    /**
     * Retrieves latest generation, only used when noticing that reading given a stale generation.
     */
    private final LongSupplier generationSupplier;

    /**
     * Retrieves latest root id and generation, moving the {@link PageCursor} to the root id and returning
     * the root generation. This is used when a query is re-traversing from the root, due to e.g. ending up
     * on a reused tree node and not knowing how to proceed from there.
     */
    private final Supplier<Root> rootCatchup;

    /**
     * Whether or not some result has been found, i.e. if {@code true} if there have been no call to
     * {@link #next()} returning {@code true}, otherwise {@code false}. If {@code false} then value in
     * {@link #prevKey} can be used and trusted.
     */
    private boolean first = true;

    /**
     * Current stable generation from this seek cursor's POV. Can be refreshed using {@link #generationSupplier}.
     */
    private long stableGeneration;

    /**
     * Current stable generation from this seek cursor's POV. Can be refreshed using {@link #generationSupplier}.
     */
    private long unstableGeneration;

    // *** Data structures for the current b-tree node ***

    /**
     * Position in current node, this is used when scanning the values of a leaf, each call to {@link #next()}
     * incrementing this position and reading the next key/value.
     */
    private int pos;

    /**
     * Number of keys in the current leaf, this value is cached and only re-read every time there's
     * a {@link PageCursor#shouldRetry() retry due to concurrent write}.
     */
    private int keyCount;

    /**
     * Set if the position of the last returned key need to be found again.
     */
    private boolean rediscoverKeyPosition;

    /**
     * {@link TreeNode#gen(PageCursor) generation} of the current leaf node, read every call to {@link #next()}.
     */
    private long currentNodeGen;

    /**
     * Generation of the pointer which was last followed, either a
     * {@link TreeNode#rightSibling(PageCursor, long, long) sibling} during scan or otherwise following
     * {@link TreeNode#newGen(PageCursor, long, long) newGen} or
     * {@link TreeNode#childAt(PageCursor, int, long, long) child}.
     */
    private long lastFollowedPointerGen;

    /**
     * Cached {@link TreeNode#gen(PageCursor) generation} of the current leaf node, read every time a pointer
     * is followed to a new node. Used to ensure that a node hasn't been reused between two calls to {@link #next()}.
     */
    private long expectedCurrentNodeGen;

    /**
     * Max of leaf/internal key count that a tree node can have at most. This is used to sanity check
     * key counts read from {@link TreeNode#keyCount(PageCursor)}.
     */
    private final int maxKeyCount;

    SeekCursor( PageCursor leafCursor, KEY mutableKey, VALUE mutableValue, TreeNode<KEY,VALUE> bTreeNode,
            KEY fromInclusive, KEY toExclusive, Layout<KEY,VALUE> layout,
            long stableGeneration, long unstableGeneration, LongSupplier generationSupplier,
            Supplier<Root> rootCatchup, long lastFollowedPointerGen ) throws IOException
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
        this.rootCatchup = rootCatchup;
        this.lastFollowedPointerGen = lastFollowedPointerGen;
        this.prevKey = layout.newKey();
        this.maxKeyCount = max( bTreeNode.internalMaxKeyCount(), bTreeNode.leafMaxKeyCount() );

        traverseDownToFirstLeaf();
    }

    private void traverseDownToFirstLeaf() throws IOException
    {
        // some variables below are initialized to satisfy the compiler
        byte nodeType;
        long newGen = -1;
        boolean isInternal = false;
        int keyCount = -1;
        long childId = -1;
        int pos = -1;
        long newGenGen = -1;
        long childIdGen = -1;
        do
        {
            do
            {
                nodeType = TreeNode.nodeType( cursor );
                if ( nodeType != TreeNode.NODE_TYPE_TREE_NODE )
                {
                    // If this node doesn't even look like a tree node then anything we read from it
                    // will be just random data when looking at it as if it were a tree node.
                    continue;
                }

                currentNodeGen = bTreeNode.gen( cursor );

                newGen = bTreeNode.newGen( cursor, stableGeneration, unstableGeneration );
                if ( GenSafePointerPair.isSuccess( newGen ) )
                {
                    newGenGen = bTreeNode.pointerGen( cursor, newGen );
                }
                isInternal = bTreeNode.isInternal( cursor );
                // Find the left-most key within from-range
                keyCount = bTreeNode.keyCount( cursor );
                if ( !keyCountIsSane() )
                {
                    continue;
                }

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
                    if ( GenSafePointerPair.isSuccess( childId ) )
                    {
                        childIdGen = bTreeNode.pointerGen( cursor, childId );
                    }
                }
            }
            while ( cursor.shouldRetry() );
            checkOutOfBounds( cursor );

            if ( nodeType != NODE_TYPE_TREE_NODE || !saneKeyCountRead( keyCount ) || !verifyNodeGenInvariants() )
            {
                // This node has been reused. Restart seek from root.
                generationCatchup();
                lastFollowedPointerGen = rootCatchup.get().goTo( cursor );

                // Force true in loop
                isInternal = true;
                keyCount = 1;
                continue;
            }

            if ( pointerCheckingWithGenerationCatchup( newGen, true ) )
            {
                continue;
            }
            else if ( TreeNode.isNode( newGen ) )
            {
                bTreeNode.goTo( cursor, "new gen", newGen );
                lastFollowedPointerGen = newGenGen;
                continue;
            }

            if ( isInternal )
            {
                if ( pointerCheckingWithGenerationCatchup( childId, false ) )
                {
                    continue;
                }

                bTreeNode.goTo( cursor, "child", childId );
                lastFollowedPointerGen = childIdGen;
            }
        }
        while ( isInternal && keyCount > 0 );

        // We've now come to the first relevant leaf, initialize the state for the coming leaf scan
        this.pos = pos - 1;
        this.keyCount = keyCount;
    }

    /**
     * {@link TreeNode#keyCount(PageCursor) keyCount} is the only value read inside a do-shouldRetry loop
     * which is used as data fed into another read. Because of that extra assertions are made around
     * keyCount, both inside do-shouldRetry (requesting one more round in the loop) and outside
     * (calling this method, which may throw exception).
     *
     * @param keyCount key count read from {@link TreeNode#keyCount(PageCursor)} and has "survived"
     * a do-shouldRetry loop.
     */
    private boolean saneKeyCountRead( int keyCount )
    {
        return keyCount <= maxKeyCount;
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
            // some variables below are initialized to satisfy the compiler
            byte nodeType;
            long newGen = -1;
            long rightSibling = -1;
            long newGenGen = -1;
            long rightSiblingGen = -1;
            do
            {
                nodeType = TreeNode.nodeType( cursor );
                if ( nodeType != TreeNode.NODE_TYPE_TREE_NODE )
                {
                    // If this node doesn't even look like a tree node then anything we read from it
                    // will be just random data when looking at it as if it were a tree node.
                    continue;
                }

                currentNodeGen = bTreeNode.gen( cursor );
                newGen = bTreeNode.newGen( cursor, stableGeneration, unstableGeneration );
                keyCount = bTreeNode.keyCount( cursor );
                if ( !keyCountIsSane() )
                {
                    continue;
                }

                if ( GenSafePointerPair.isSuccess( newGen ) )
                {
                    newGenGen = bTreeNode.pointerGen( cursor, newGen );
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
                    if ( GenSafePointerPair.isSuccess( rightSibling ) )
                    {
                        rightSiblingGen = bTreeNode.pointerGen( cursor, rightSibling );
                    }
                }
                else
                {
                    // Read the next value in this leaf
                    bTreeNode.keyAt( cursor, mutableKey, pos );
                    bTreeNode.valueAt( cursor, mutableValue, pos );
                }
            }
            while ( rediscoverKeyPosition = cursor.shouldRetry() );
            checkOutOfBounds( cursor );

            if ( nodeType != TreeNode.NODE_TYPE_TREE_NODE || !saneKeyCountRead( keyCount ) )
            {
                // This node has been reused for something else than a tree node. Restart seek from root.
                restartSeekFromRoot();
                continue;
            }

            if ( !verifyNodeGenInvariants() )
            {
                // The node generation is newer than expected. This node has probably been reused during
                // seekers lifetime. Restart seek from root.
                restartSeekFromRoot();
                continue;
            }

            // Go to newGen if read successfully and
            if ( pointerCheckingWithGenerationCatchup( newGen, true ) )
            {
                // Reading newGen pointer resulted in a bad read, but generation had changed (a checkpoint has
                // occurred since we started this cursor) so the generation fields in this cursor are now updated
                // with the latest, so let's try that read again.
                rediscoverKeyPosition = true;
                continue;
            }
            else if ( TreeNode.isNode( newGen ) )
            {
                // We ended up on a node which has a newGen set, let's go to it and read from that one instead.
                bTreeNode.goTo( cursor, "new gen", newGen );
                rediscoverKeyPosition = true;
                lastFollowedPointerGen = newGenGen;
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
                    rediscoverKeyPosition = true;
                    continue;
                }
                else if ( TreeNode.isNode( rightSibling ) )
                {
                    // TODO: Check if rightSibling is within expected range before calling next.
                    // TODO: Possibly by getting highest expected from IdProvider
                    bTreeNode.goTo( cursor, "right sibling", rightSibling );
                    lastFollowedPointerGen = rightSiblingGen;
                    if ( first )
                    {
                        // Have not yet found first hit among leaves.
                        // First hit can be several leaves to the right.
                        // Continue to use binary search in right leaf
                        rediscoverKeyPosition = true;
                    }
                    else
                    {
                        // It is likely that first key in right sibling is a next hit.
                        // Continue using scan
                        pos = -1;
                    }
                    continue; // in the outer loop, with the position reset to the beginning of the right sibling
                }
            }
            else if ( layout.compare( mutableKey, toExclusive ) < 0 )
            {
                if ( layout.compare( mutableKey, fromInclusive ) < 0 )
                {
                    // too far to the left possibly because page reuse
                    rediscoverKeyPosition = true;
                    continue;
                }
                else if ( !first && layout.compare( prevKey, mutableKey ) >= 0 )
                {
                    // We've come across a bad read in the middle of a split
                    // This is outlined in InternalTreeLogic, skip this value (it's fine)
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

    private boolean keyCountIsSane()
    {
        // if keyCount is out of bounds of what a tree node can hold, it must be that we're
        // reading from an evicted page that just happened to look like a tree node.
        return keyCount >= 0 && keyCount <= maxKeyCount;
    }

    private void restartSeekFromRoot() throws IOException
    {
        generationCatchup();
        lastFollowedPointerGen = rootCatchup.get().goTo( cursor );
        if ( !first )
        {
            layout.copyKey( prevKey, fromInclusive );
        }
        traverseDownToFirstLeaf();
    }

    private boolean verifyNodeGenInvariants()
    {
        if ( lastFollowedPointerGen != 0 )
        {
            if ( currentNodeGen > lastFollowedPointerGen )
            {
                // We've just followed a pointer to a new node, we have arrived there and made
                // the first read on it. It looks like the node we arrived at have a higher generation
                // than the pointer generation, this means that this node node have been reused between
                // following the pointer and reading the node after getting there.
                return false;
            }
            lastFollowedPointerGen = 0;
            expectedCurrentNodeGen = currentNodeGen;
        }
        else if ( currentNodeGen != expectedCurrentNodeGen )
        {
            // We've read more than once from this node and between reads the node generation has changed.
            // This means the node has been reused.
            return false;
        }
        return true;
    }

    private void rediscoverKeyPosition()
    {
        // Keys could have been moved to the left so we need to make sure we are not missing any keys by
        // moving position back until we find previously returned key
        KEY keyToRediscover = first ? fromInclusive : prevKey;
        int searchResult = KeySearch.search( cursor, bTreeNode, keyToRediscover, mutableKey, keyCount );
        pos = KeySearch.positionOf( searchResult );
    }

    private boolean pointerCheckingWithGenerationCatchup( long pointer, boolean allowNoNode )
    {
        if ( !GenSafePointerPair.isSuccess( pointer ) )
        {
            // An unexpected sibling read, this could have been caused by a concurrent checkpoint
            // where generation has been incremented. Re-read generation and, if changed since this
            // seek started then update generation locally
            if ( generationCatchup() )
            {
                return true;
            }
            PointerChecking.checkPointer( pointer, allowNoNode );
        }
        return false;
    }

    private boolean generationCatchup()
    {
        long newGeneration = generationSupplier.getAsLong();
        long newStableGeneration = Generation.stableGeneration( newGeneration );
        long newUnstableGeneration = Generation.unstableGeneration( newGeneration );
        if ( newStableGeneration != stableGeneration || newUnstableGeneration != unstableGeneration )
        {
            stableGeneration = newStableGeneration;
            unstableGeneration = newUnstableGeneration;
            return true;
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
