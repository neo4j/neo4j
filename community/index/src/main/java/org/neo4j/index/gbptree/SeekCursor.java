/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
 * Seek can be performed forwards or backwards, returning hits in ascending or descending order respectively
 * (as defined by {@link Layout#compare(Object, Object)}). Direction is decided on relation between
 * {@link #fromInclusive} and {@link #toExclusive}. Backwards seek is expected to be slower than forwards seek
 * because extra care needs to be taken to make sure no keys are skipped when keys are move to the right in the tree.
 * <pre>
 * If fromInclusive <= toExclusive, then seek forwards, otherwise seek backwards
 * </pre>
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
     * Provided when constructing the {@link SeekCursor}, marks the start (inclusive) of the key range to seek.
     * Comparison with {@link #toExclusive} decide if seeking forwards or backwards.
     */
    private final KEY fromInclusive;

    /**
     * Provided when constructing the {@link SeekCursor}, marks the end (exclusive) of the key range to seek.
     * Comparison with {@link #fromInclusive} decide if seeking forwards or backwards.
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
     * Max of leaf/internal key count that a tree node can have at most. This is used to sanity check
     * key counts read from {@link TreeNode#keyCount(PageCursor)}.
     */
    private final int maxKeyCount;

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
    private boolean concurrentWriteHappened;

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
     * Decide if seeker is configured to seek forwards or backwards.
     * <p>
     * {@code true} if {@code layout.compare(fromInclusive, toExclusive) <= 0}, otherwise false.
     */
    private final boolean seekForward;

    /**
     * Add to {@link #pos} to move this {@code SeekCursor} forward in the seek direction.
     */
    private final int stride;

    /**
     * Set within should retry loop.
     * <p>
     * Is node a {@link TreeNode#NODE_TYPE_TREE_NODE} or something else?
     */

    private byte nodeType;
    /**
     * Set within should retry loop.
     * <p>
     * Pointer to new generation of node.
     */
    private long newGen;

    /**
     * Set within should retry loop.
     * <p>
     * Generation of new gen pointer
     */
    private long newGenGen;

    /**
     * Set within should retry loop.
     * <p>
     * Is node internal or leaf?
     */
    private boolean isInternal;

    /**
     * Set within should retry loop.
     * <p>
     * Used to store next child pointer to follow while traversing down the tree
     * or next sibling pointer to follow if traversing along the leaves.
     */
    private long pointerId;

    /**
     * Set within should retry loop.
     * <p>
     * Generation of {@link #pointerId}.
     */
    private long pointerGen;

    // ┌── Special variables for backwards seek ──┐
    // v                                          v

    /**
     * Set within should retry loop.
     * <p>
     * Pointer to sibling opposite to seek direction. Only used when seeking backwards.
     */
    private long prevSiblingId;

    /**
     * Set within should retry loop.
     * <p>
     * Generation of {@link #prevSiblingId}.
     */
    private long prevSiblingGen;

    /**
     * Set by linked cursor scouting next sibling to go to when seeking backwards.
     * If first key when reading from next sibling node is not equal to this we
     * may have missed some keys that was moved passed us and we need to start
     * over from previous node.
     */
    private final KEY expectedFirstAfterGoToNext;

    /**
     * Key on pos 0 if traversing forward, pos {@code keyCount - 1} if traversing backwards.
     * To be compared with {@link #expectedFirstAfterGoToNext}.
     */
    private final KEY firstKeyInNode;

    /**
     * {@code true} to indicate that first key in node needs to be verified to ensure no keys
     * was moved passed us while we where changing nodes.
     */
    private boolean verifyExpectedFirstAfterGoToNext;

    SeekCursor( PageCursor cursor, TreeNode<KEY,VALUE> bTreeNode, KEY fromInclusive, KEY toExclusive,
            Layout<KEY,VALUE> layout, long stableGeneration, long unstableGeneration, LongSupplier generationSupplier,
            Supplier<Root> rootCatchup, long lastFollowedPointerGen ) throws IOException
    {
        this.cursor = cursor;
        this.fromInclusive = fromInclusive;
        this.toExclusive = toExclusive;
        this.layout = layout;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
        this.generationSupplier = generationSupplier;
        this.bTreeNode = bTreeNode;
        this.rootCatchup = rootCatchup;
        this.lastFollowedPointerGen = lastFollowedPointerGen;
        this.mutableKey = layout.newKey();
        this.mutableValue = layout.newValue();
        this.prevKey = layout.newKey();
        this.maxKeyCount = max( bTreeNode.internalMaxKeyCount(), bTreeNode.leafMaxKeyCount() );
        this.seekForward = layout.compare( fromInclusive, toExclusive ) <= 0;
        this.stride = seekForward ? 1 : -1;
        this.expectedFirstAfterGoToNext = layout.newKey();
        this.firstKeyInNode = layout.newKey();

        traverseDownToFirstLeaf();
    }

    private void traverseDownToFirstLeaf() throws IOException
    {
        do
        {
            // Read
            do
            {
                // Where we are
                if ( !readHeader() )
                {
                    continue;
                }

                // Where we're going
                pos = searchKey( fromInclusive );
                if ( isInternal )
                {
                    pointerId = bTreeNode.childAt( cursor, pos, stableGeneration, unstableGeneration );
                    readPointerGenOnSuccess();
                }
            }
            while ( cursor.shouldRetry() );
            checkOutOfBounds( cursor );

            // Act
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

            if ( goToNewGen() )
            {
                continue;
            }

            if ( isInternal )
            {
                goToNext( "child" );
            }
        }
        while ( isInternal && keyCount > 0 );

        // We've now come to the first relevant leaf, initialize the state for the coming leaf scan
        pos -= stride;
        if ( !seekForward )
        {
            // The tree traversal is best effort when seeking backwards
            // need to trigger search for key in next
            concurrentWriteHappened = true;
        }
    }

    @Override
    public boolean next() throws IOException
    {
        while ( true )
        {
            pos += stride;
            // Read
            do
            {

                // Where we are
                if ( !readHeader() )
                {
                    continue;
                }

                if ( verifyExpectedFirstAfterGoToNext )
                {
                    pos = seekForward ? 0 : keyCount - 1;
                    bTreeNode.keyAt( cursor,firstKeyInNode, pos );
                }

                if ( concurrentWriteHappened )
                {
                    // Keys could have been moved so we need to make sure we are not missing any keys by
                    // moving position back until we find previously returned key
                    pos = searchKey( first ? fromInclusive : prevKey );
                    if ( !seekForward && pos >= keyCount )
                    {
                        // We may need to go to previous sibling to find correct place to start seeking from
                        prevSiblingId = readPrevSibling();
                        readPrevSiblingPointerGenOnSuccess();
                    }
                }

                // Next result
                if ( (seekForward && pos >= keyCount) || (!seekForward && pos <= 0) )
                {
                    // Read right sibling
                    pointerId = readNextSibling();
                    readPointerGenOnSuccess();
                }
                if ( 0 <= pos && pos < keyCount )
                {
                    // Read the next value in this leaf
                    bTreeNode.keyAt( cursor, mutableKey, pos );
                    bTreeNode.valueAt( cursor, mutableValue, pos );
                }
            }
            while ( concurrentWriteHappened = cursor.shouldRetry() );
            checkOutOfBounds( cursor );

            // Act
            if ( nodeType != TreeNode.NODE_TYPE_TREE_NODE || !saneKeyCountRead( keyCount ) )
            {
                // This node has been reused for something else than a tree node. Restart seek from root.
                restartSeekFromRoot();
                continue;
            }

            if ( !verifyFirstKeyInNodeIsExpectedAfterGoTo() )
            {
                continue;
            }

            if ( !verifyNodeGenInvariants() )
            {
                // The node generation is newer than expected. This node has probably been reused during
                // seekers lifetime. Restart seek from root.
                restartSeekFromRoot();
                continue;
            }

            if ( goToNewGen() )
            {
                continue;
            }

            if ( !seekForward && pos >= keyCount )
            {
                if ( goToPrevSibling() )
                {
                    continue; // in the read loop above so that we can continue reading from previous sibling
                }
            }

            if ( (seekForward && pos >= keyCount) || (!seekForward && pos <= 0 && !insidePrevKey()) )
            {
                if ( goToNextSibling() )
                {
                    continue; // in the read loop above so that we can continue reading from next sibling
                }
            }
            else if ( insideEndRange() )
            {
                if ( isResultKey() )
                {
                    layout.copyKey( mutableKey, prevKey );
                    return true; // which marks this read a hit that user can see
                }
                continue;
            }

            // We've come too far and so this means the end of the result set
            return false;
        }
    }

    private boolean insideEndRange()
    {
        return seekForward ? layout.compare( mutableKey, toExclusive ) < 0
                           : layout.compare( mutableKey, toExclusive ) > 0;
    }

    private boolean insideStartRange()
    {
        return seekForward ? layout.compare( mutableKey, fromInclusive ) >= 0
                           : layout.compare( mutableKey, fromInclusive ) <= 0;
    }

    private boolean insidePrevKey()
    {
        if ( first )
        {
            return insideStartRange();
        }
        else
        {
            return seekForward ? layout.compare( mutableKey, prevKey ) > 0
                               : layout.compare( mutableKey, prevKey ) < 0;
        }
    }

    private void goToNext( String type ) throws IOException
    {
        if ( !pointerCheckingWithGenerationCatchup( pointerId, false ) )
        {
            bTreeNode.goTo( cursor, type, pointerId );
            lastFollowedPointerGen = pointerGen;
        }
    }

    private boolean goToNewGen() throws IOException
    {
        if ( pointerCheckingWithGenerationCatchup( newGen, true ) )
        {
            concurrentWriteHappened = true;
            return true;
        }
        else if ( TreeNode.isNode( newGen ) )
        {
            bTreeNode.goTo( cursor, "new gen", newGen );
            lastFollowedPointerGen = newGenGen;
            concurrentWriteHappened = true;
            return true;
        }
        return false;
    }

    private void readPointerGenOnSuccess()
    {
        if ( GenSafePointerPair.isSuccess( pointerId ) )
        {
            pointerGen = bTreeNode.pointerGen( cursor, pointerId );
        }
    }

    private boolean verifyFirstKeyInNodeIsExpectedAfterGoTo()
    {
        if ( verifyExpectedFirstAfterGoToNext && layout.compare( firstKeyInNode, expectedFirstAfterGoToNext ) != 0 )
        {
            concurrentWriteHappened = true;
            verifyExpectedFirstAfterGoToNext = false;
            return false;
        }
        verifyExpectedFirstAfterGoToNext = false;
        return true;
    }

    private void readPrevSiblingPointerGenOnSuccess()
    {
        if ( GenSafePointerPair.isSuccess( prevSiblingId ) )
        {
            prevSiblingGen = bTreeNode.pointerGen( cursor, prevSiblingId );
        }
    }

    private long readPrevSibling()
    {
        return seekForward ?
               bTreeNode.leftSibling( cursor, stableGeneration, unstableGeneration ) :
               bTreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
    }

    private long readNextSibling()
    {
        return seekForward ?
               bTreeNode.rightSibling( cursor, stableGeneration, unstableGeneration ) :
               bTreeNode.leftSibling( cursor, stableGeneration, unstableGeneration );
    }

    private int searchKey( KEY key )
    {
        int search = KeySearch.search( cursor, bTreeNode, key, mutableKey, keyCount );
        int pos = KeySearch.positionOf( search );

        // Assuming unique keys
        if ( seekForward && isInternal && KeySearch.isHit( search ) )
        {
            pos++;
        }
        return pos;
    }

    /**
     * @return {@code true} if header was read and looks sane, otherwise {@code false} meaning that node doesn't look
     * like a tree node or we can expect a shouldRetry to take place.
     */
    private boolean readHeader()
    {
        nodeType = TreeNode.nodeType( cursor );
        if ( nodeType != TreeNode.NODE_TYPE_TREE_NODE )
        {
            // If this node doesn't even look like a tree node then anything we read from it
            // will be just random data when looking at it as if it were a tree node.
            return false;
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
        if ( !keyCountIsSane( keyCount ) )
        {
            return false;
        }

        return true;
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

    private boolean goToPrevSibling() throws IOException
    {
        if ( pointerCheckingWithGenerationCatchup( prevSiblingId, true ) )
        {
            concurrentWriteHappened = true;
            return true;
        }
        else if ( TreeNode.isNode( prevSiblingId ) )
        {
            bTreeNode.goTo( cursor, "sibling", prevSiblingId );
            lastFollowedPointerGen = prevSiblingGen;

            // We go back to previous sibling because we potentially missed some keys that was moved
            // in opposite direction to which we seek. So when we continue to search for continuation
            // point in our previous sibling we use binary search.
            concurrentWriteHappened = true;
            return true;
        }

        // There is no prev sibling to go to.
        return false;
    }

    /**
     * @return {@code true} if we should read more after this call, otherwise {@code false} to mark the end.
     * @throws IOException on {@link PageCursor} error.
     */
    private boolean goToNextSibling() throws IOException
    {
        if ( pointerCheckingWithGenerationCatchup( pointerId, true ) )
        {
            // Reading rightSibling pointer resulted in a bad read, but generation had changed
            // (a checkpoint has occurred since we started this cursor) so the generation fields in this
            // cursor are now updated with the latest, so let's try that read again.
            concurrentWriteHappened = true;
            return true;
        }
        else if ( TreeNode.isNode( pointerId ) )
        {
            if ( seekForward )
            {
                // TODO: Check if rightSibling is within expected range before calling next.
                // TODO: Possibly by getting highest expected from IdProvider
                bTreeNode.goTo( cursor, "sibling", pointerId );
                lastFollowedPointerGen = pointerGen;
                if ( first )
                {
                    // Have not yet found first hit among leaves.
                    // First hit can be several leaves to the right.
                    // Continue to use binary search in right leaf
                    concurrentWriteHappened = true;
                }
                else
                {
                    // It is likely that first key in right sibling is a next hit.
                    // Continue using scan
                    pos = -1;
                }
                return true;
            }
            else
            {
                // Need to scout next sibling because we are seeking backwards
                if ( scoutNextSibling() )
                {
                    bTreeNode.goTo( cursor, "sibling", pointerId );
                    verifyExpectedFirstAfterGoToNext = true;
                    lastFollowedPointerGen = pointerGen;
                }
                else
                {
                    concurrentWriteHappened = true;
                }
                return true;
            }
        }

        // The current node is exhausted and it had no sibling to read more from.
        return false;
    }

    private boolean scoutNextSibling() throws IOException
    {
        // Read header but to local variables and not global once
        byte nodeType;
        int keyCount = -1;
        try ( PageCursor scout = this.cursor.openLinkedCursor( GenSafePointerPair.pointer( pointerId ) ) )
        {
            scout.next();
            nodeType = TreeNode.nodeType( scout );
            if ( nodeType == TreeNode.NODE_TYPE_TREE_NODE )
            {
                keyCount = bTreeNode.keyCount( scout );
                if ( keyCountIsSane( keyCount ) )
                {
                    int firstPos = seekForward ? 0 : keyCount - 1;
                    bTreeNode.keyAt( scout, expectedFirstAfterGoToNext, firstPos );
                }
            }

            if ( this.cursor.shouldRetry() )
            {
                // We scouted next sibling but either next sibling or current node has been changed
                // since we left shouldRetry loop, this means keys could have been moved passed us
                // and we need to start over.
                // Because we also need to restart read on current node there is no use to loop
                // on shouldRetry here.
                return false;
            }
            checkOutOfBounds( this.cursor );
        }
        if ( nodeType != TreeNode.NODE_TYPE_TREE_NODE )
        {
            // If this node doesn't even look like a tree node then anything we read from it
            // will be just random data when looking at it as if it were a tree node.
            return false;
        }
        if ( !keyCountIsSane( keyCount ) )
        {
            return false;
        }
        return true;
    }

    private boolean isResultKey()
    {
        if ( !insideStartRange() )
        {
            // Key is outside start range, possibly because page reuse
            concurrentWriteHappened = true;
            return false;
        }
        else if ( !first && !insidePrevKey() )
        {
            // We've come across a bad read in the middle of a split
            // This is outlined in InternalTreeLogic, skip this value (it's fine)
            return false;
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
        return true;
    }

    private boolean keyCountIsSane( int keyCount )
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
