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
package org.neo4j.index.internal.gbptree;

import java.io.IOException;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.cursor.RawCursor;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.Integer.max;

import static org.neo4j.index.internal.gbptree.PageCursorUtil.checkOutOfBounds;

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
 * See detailed documentation about difficult cases below.
 * <pre>
 * If fromInclusive <= toExclusive, then seek forwards, otherwise seek backwards
 * </pre>
 * <p>
 * Implementation note: there are assumptions that keys are unique in the tree.
 * <p>
 * <strong>Note on backwards seek</strong>
 * <p>
 * To allow for lock free concurrency control the GBPTree agree to only move keys between nodes in 'forward' direction,
 * from left to right. This means when we do normal forward seek we never risk having keys moved 'passed' us and
 * therefore we can always be sure that we never miss any keys when traversing the leaf nodes or end up in the middle
 * of the range when traversing down the internal nodes. This assumption, of course, does not hold when seeking
 * backwards.
 * There are two complicated cases where we risk missing keys.
 * Case 1 - Split in current node
 * Case 2 - Split in next node while seeker is moving to that node
 * <p>
 * <strong>Case 1 - Split in current node</strong>
 * <p>
 * <em>Forward seek</em>
 * <p>
 * Here, the seeker is seeking forward and has read K0, K1, K2 and is about to read K3.
 * Then K4 is suddenly inserted and a split happens.
 * Seeker will now wake up and find that he is now outside the range of (previously returned key, end of range).
 * But he knows that he can continue to read forward until he hits the previously returned key, which is K2 and
 * then continue to return the next key, K3.
 * <pre>
 *     Seeker->
 *        v
 * [K0 K1 K2 K3]
 *
 *     Seeker->
 *        v
 * [K0 K1 __ __]<->[K2 K3 K4 __]
 * </pre>
 * <em>Backward seek</em>
 * <p>
 * Here, the seeking is seeking backwards and has only read K3 and is about to read K2.
 * Again, K4 is inserted, causing the same split as above.
 * Seeker now wakes up and find that the next key he can return is K1. What he do not see is that K2 has been
 * moved to the previous sibling and so, because he can not find the previously returned key, K3, in the current
 * node and because the next to return, K1, is located to the far right in the current node he need to jump back to
 * the previous sibling to find where to start again.
 * <pre>
 *      <-Seeker
 *           v
 * [K0 K1 K2 K3]
 *
 *      <-Seeker (WRONG)
 *           v
 * [K0 K1 __ __]<->[K2 K3 K4 __]
 *
 *                <-Seeker (RIGHT)
 *                     v
 * [K0 K1 __ __]<->[K2 K3 K4 __]
 * </pre>
 * <p>
 * <strong>Case 2 - Split in next node while seeker is moving to that node</strong>
 * <p>
 * <em>Forward seek</em>
 * <p>
 * Seeker has read K0, K1 on node 1  and has just moved to right sibling, node 2.
 * Now, K6 is inserted and a split  happens in node 2, right sibling of node 1.
 * Seeker wakes up and continues to read on node 2. Everything is fine.
 * <pre>
 *                   Seeker->
 *                      v
 * 1:[K0 K1 __ __]<->2:[K2 K3 K4 K5]
 *
 *                   Seeker->
 *                      v
 * 1:[K0 K1 __ __]<->2:[K2 K3 __ __]<->3:[K4 K5 K6 __]
 * </pre>
 * <em>Backward seek</em>
 * <p>
 * Seeker has read K4, K5 and has just moved to left sibling, node 1.
 * Insert K6, split in node 2. Note that keys are move to node 3, passed our seeker.
 * Seeker wakes up and see K1 as next key but misses K3 and K2.
 * <pre>
 *       <--Seeker
 *             v
 * 1:[K0 K1 K2 K3]<->2:[K4 K5 __ __]
 *
 *        <-Seeker
 *             v
 * 1:[K0 K1 __ __]<->3:[K2 K3 K4 __]<->2:[K5 K6 __ __]
 * </pre>
 * To guard for this, seeker 'scout' next sibling before moving there and read first key that he expect to see, K3
 * in this case. By using a linked cursor to 'scout' we create a consistent read over the node gap. If there us
 * suddenly another key when he goes there he knows that he could have missed some keys and he needs to go back until
 * he find the place where he left off, K4.
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
    private final LongSupplier genSupplier;

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
     * Current stable generation from this seek cursor's POV. Can be refreshed using {@link #genSupplier}.
     */
    private long stableGen;

    /**
     * Current stable generation from this seek cursor's POV. Can be refreshed using {@link #genSupplier}.
     */
    private long unstableGen;

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

    /**
     * Result from {@link KeySearch#search(PageCursor, TreeNode, Object, Object, int)}.
     */
    private int searchResult;

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
            Layout<KEY,VALUE> layout, long stableGen, long unstableGen, LongSupplier genSupplier,
            Supplier<Root> rootCatchup, long lastFollowedPointerGen ) throws IOException
    {
        this.cursor = cursor;
        this.fromInclusive = fromInclusive;
        this.toExclusive = toExclusive;
        this.layout = layout;
        this.stableGen = stableGen;
        this.unstableGen = unstableGen;
        this.genSupplier = genSupplier;
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

    /**
     * Traverses from the root down to the leaf containing the next key that we're looking for, or the first
     * one provided in the constructor if this no result have yet been returned.
     * <p>
     * This method is called when constructing the cursor, but also if this traversal itself or leaf scan
     * later on ends up on an unexpected tree node (typically due to concurrent changes,
     * checkpoint and tree node reuse).
     * <p>
     * Before calling this method the caller is expected to place the {@link PageCursor} at the root, by using
     * {@link #rootCatchup}. After this method returns the {@link PageCursor} is placed on the leaf containing
     * the next result and {@link #pos} is also initialized correctly.
     *
     * @throws IOException on {@link PageCursor} error.
     */
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

                searchResult = searchKey( fromInclusive );
                if ( !KeySearch.isSuccess( searchResult ) )
                {
                    continue;
                }
                pos = positionOf( searchResult );

                if ( isInternal )
                {
                    pointerId = bTreeNode.childAt( cursor, pos, stableGen, unstableGen );
                    pointerGen = readPointerGenOnSuccess( pointerId );
                }
            }
            while ( cursor.shouldRetry() );
            checkOutOfBounds( cursor );

            // Act
            if ( !endedUpOnExpectedNode() )
            {
                prepareToStartFromRoot();
                isInternal = true;
                continue;
            }
            else if ( !saneRead() )
            {
                throw new TreeInconsistencyException( "Read inconsistent tree node %d%n" +
                        "  nodeType:%d%n  currentNodeGen:%d%n  newGen:%d%n  newGenGen:%d%n  isInternal:%b%n" +
                        "  keyCount:%d%n  maxKeyCount:%d%n  searchResult:%d%n  pos:%d%n  childId:%d%n  childIdGen:%d",
                        cursor.getCurrentPageId(), nodeType, currentNodeGen, newGen, newGenGen,
                        isInternal, keyCount, maxKeyCount, searchResult, pos, pointerId, pointerGen );
            }

            if ( goToNewGen() )
            {
                continue;
            }

            if ( isInternal )
            {
                goTo( pointerId, pointerGen, "child", false );
            }
        }
        while ( isInternal );

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
                    searchResult = searchKey( first ? fromInclusive : prevKey );
                    if ( !KeySearch.isSuccess( searchResult ) )
                    {
                        continue;
                    }
                    pos = positionOf( searchResult );

                    if ( !seekForward && pos >= keyCount )
                    {
                        // We may need to go to previous sibling to find correct place to start seeking from
                        prevSiblingId = readPrevSibling();
                        prevSiblingGen = readPointerGenOnSuccess( prevSiblingId );
                    }
                }

                // Next result
                if ( (seekForward && pos >= keyCount) || (!seekForward && pos <= 0) )
                {
                    // Read right sibling
                    pointerId = readNextSibling();
                    pointerGen = readPointerGenOnSuccess( pointerId );
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
            if ( !endedUpOnExpectedNode() )
            {
                // This node has been reused for something else than a tree node. Restart seek from root.
                prepareToStartFromRoot();
                traverseDownToFirstLeaf();
                continue;
            }
            else if ( !saneRead() )
            {
                throw new TreeInconsistencyException( "Read inconsistent tree node %d%n" +
                        "  nodeType:%d%n  currentNodeGen:%d%n  newGen:%d%n  newGenGen:%d%n" +
                        "  keyCount:%d%n  maxKeyCount:%d%n  searchResult:%d%n  pos:%d%n" +
                        "  rightSibling:%d%n  rightSiblingGen:%d",
                        cursor.getCurrentPageId(), nodeType, currentNodeGen, newGen, newGenGen,
                        keyCount, maxKeyCount, searchResult, pos, pointerId, pointerGen );
            }

            if ( !verifyFirstKeyInNodeIsExpectedAfterGoTo() )
            {
                continue;
            }

            if ( goToNewGen() )
            {
                continue;
            }

            if ( !seekForward && pos >= keyCount )
            {
                goTo( prevSiblingId, prevSiblingGen, "prev sibling", true );
                // Continue in the read loop above so that we can continue reading from previous sibling
                // or on next position
                continue;
            }

            if ( (seekForward && pos >= keyCount) || (!seekForward && pos <= 0 && !insidePrevKey()) )
            {
                if ( goToNextSibling() )
                {
                    continue; // in the read loop above so that we can continue reading from next sibling
                }
            }
            else if ( 0 <= pos && pos < keyCount && insideEndRange() )
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

    /**
     * @return whether or not the read key ({@link #mutableKey}) is "before" the end of the key range
     * ({@link #toExclusive}) of this seek.
     */
    private boolean insideEndRange()
    {
        return seekForward ? layout.compare( mutableKey, toExclusive ) < 0
                           : layout.compare( mutableKey, toExclusive ) > 0;
    }

    /**
     * @return whether or not the read key ({@link #mutableKey}) is "after" the start of the key range
     * ({@link #fromInclusive}) of this seek.
     */
    private boolean insideStartRange()
    {
        return seekForward ? layout.compare( mutableKey, fromInclusive ) >= 0
                           : layout.compare( mutableKey, fromInclusive ) <= 0;
    }

    /**
     * @return whether or not the read key ({@link #mutableKey}) is "after" the last returned key of this seek
     * ({@link #prevKey}), or if no result has been returned the start of the key range ({@link #fromInclusive}).
     */
    private boolean insidePrevKey()
    {
        if ( first )
        {
            return insideStartRange();
        }
        return seekForward ? layout.compare( mutableKey, prevKey ) > 0
                           : layout.compare( mutableKey, prevKey ) < 0;
    }

    /**
     * Tries to move the {@link PageCursor} to the tree node specified inside {@code pointerId},
     * also setting the pointer generation expectation on the next read on that new tree node.
     * <p>
     * As with all pointers, the generation is checked for sanity and if generation looks to be in the future,
     * there's a generation catch-up made and the read will have to be re-attempted.
     *
     * @param pointerId read result containing pointer id to go to.
     * @param pointerGen generation of {@code pointerId}.
     * @param type type of pointer, e.g. "child" or "sibling" or so.
     * @return {@code true} if context was updated or {@link PageCursor} was moved, both cases meaning that
     * caller should retry its most recent read, otherwise {@code false} meaning that nothing happened.
     * @throws IOException on {@link PageCursor} error.
     * @throws TreeInconsistencyException if {@code allowNoNode} is {@code true} and {@code pointerId}
     * contains a "null" tree node id.
     */
    private boolean goTo( long pointerId, long pointerGen, String type, boolean allowNoNode ) throws IOException
    {
        if ( pointerCheckingWithGenCatchup( pointerId, allowNoNode ) )
        {
            concurrentWriteHappened = true;
            return true;
        }
        else if ( !allowNoNode || TreeNode.isNode( pointerId ) )
        {
            bTreeNode.goTo( cursor, type, pointerId );
            lastFollowedPointerGen = pointerGen;
            concurrentWriteHappened = true;
            return true;
        }
        return false;
    }

    /**
     * Calls {@link #goTo(long, long, String, boolean)} with newGen fields.
     */
    private boolean goToNewGen() throws IOException
    {
        return goTo( newGen, newGenGen, "new gen", true );
    }

    /**
     * @return generation of {@code pointerId}, if the pointer id was successfully read.
     */
    private long readPointerGenOnSuccess( long pointerId )
    {
        if ( GenSafePointerPair.isSuccess( pointerId ) )
        {
            return bTreeNode.pointerGen( cursor, pointerId );
        }
        return -1; // this value doesn't matter
    }

    /**
     * @return {@code false} if there was a set expectancy on first key in tree node which weren't met,
     * otherwise {@code true}. Caller should
     */
    private boolean verifyFirstKeyInNodeIsExpectedAfterGoTo()
    {
        boolean result = true;
        if ( verifyExpectedFirstAfterGoToNext && layout.compare( firstKeyInNode, expectedFirstAfterGoToNext ) != 0 )
        {
            concurrentWriteHappened = true;
            result = false;
        }
        verifyExpectedFirstAfterGoToNext = false;
        return result;
    }

    /**
     * @return the read previous sibling, depending on the direction this seek is going.
     */
    private long readPrevSibling()
    {
        return seekForward ?
               bTreeNode.leftSibling( cursor, stableGen, unstableGen ) :
               bTreeNode.rightSibling( cursor, stableGen, unstableGen );
    }

    /**
     * @return the read next sibling, depending on the direction this seek is going.
     */
    private long readNextSibling()
    {
        return seekForward ?
               bTreeNode.rightSibling( cursor, stableGen, unstableGen ) :
               bTreeNode.leftSibling( cursor, stableGen, unstableGen );
    }

    /**
     * Does a binary search for the given {@code key} in the current tree node and returns its position.
     *
     * @return position of the {@code key} in the current tree node, or position of the closest key.
     */
    private int searchKey( KEY key )
    {
        return KeySearch.search( cursor, bTreeNode, key, mutableKey, keyCount );
    }

    private int positionOf( int searchResult )
    {
        int pos = KeySearch.positionOf( searchResult );

        // Assuming unique keys
        if ( isInternal && KeySearch.isHit( searchResult ) )
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

        newGen = bTreeNode.newGen( cursor, stableGen, unstableGen );
        if ( GenSafePointerPair.isSuccess( newGen ) )
        {
            newGenGen = bTreeNode.pointerGen( cursor, newGen );
        }
        isInternal = TreeNode.isInternal( cursor );
        // Find the left-most key within from-range
        keyCount = bTreeNode.keyCount( cursor );

        return keyCountIsSane( keyCount );
    }

    private boolean endedUpOnExpectedNode()
    {
        return nodeType == TreeNode.NODE_TYPE_TREE_NODE && verifyNodeGenInvariants();
    }

    /**
     * @return the key/value found from the most recent call to {@link #next()} returning {@code true}.
     * @throws IllegalStateException if no {@link #next()} call which returned {@code true} has been made yet.
     */
    @Override
    public Hit<KEY,VALUE> get()
    {
        if ( first )
        {
            throw new IllegalStateException( "There has been no successful call to next() yet" );
        }

        return this;
    }

    /**
     * Moves {@link PageCursor} to next sibling (read before this call into {@link #pointerId}).
     * Also, on backwards seek, calls {@link #scoutNextSibling()} to be able to verify consistent read on
     * new sibling even on concurrent writes.
     * <p>
     * As with all pointers, the generation is checked for sanity and if generation looks to be in the future,
     * there's a generation catch-up made and the read will have to be re-attempted.
     *
     * @return {@code true} if we should read more after this call, otherwise {@code false} to mark the end.
     * @throws IOException on {@link PageCursor} error.
     */
    private boolean goToNextSibling() throws IOException
    {
        if ( pointerCheckingWithGenCatchup( pointerId, true ) )
        {
            // Reading sibling pointer resulted in a bad read, but generation had changed
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

    /**
     * Reads first key on next sibling, without moving the main {@link PageCursor} to that sibling.
     * This to be able to guard for, and retry read if, concurrent writes moving keys in the "wrong" direction.
     * The first key read here will be matched after actually moving the main {@link PageCursor} to
     * the next sibling.
     * <p>
     * May only be called if {@link #pointerId} points to next sibling.
     *
     * @return {@code true} if first key in next sibling was read successfully, otherwise {@code false},
     * which means that caller should retry most recent read.
     * @throws IOException on {@link PageCursor} error.
     */
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
        if ( nodeType != TreeNode.NODE_TYPE_TREE_NODE || !keyCountIsSane( keyCount ) )
        {
            return false;
        }
        return true;
    }

    /**
     * @return whether or not the read {@link #mutableKey} is one that should be included in the result.
     * If this method returns {@code true} then {@link #next()} will return {@code true}.
     * Returns {@code false} if this happened to be a bad read in the middle of a split or merge or so.
     */
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

    /**
     * {@link TreeNode#keyCount(PageCursor) keyCount} is the only value read inside a do-shouldRetry loop
     * which is used as data fed into another read. Because of that extra assertions are made around
     * keyCount, both inside do-shouldRetry (requesting one more round in the loop) and outside
     * (calling this method, which may throw exception).
     *
     * @param keyCount key count of a tree node.
     * @return {@code true} if key count is sane, i.e. positive and within max expected key count on a tree node.
     */
    private boolean keyCountIsSane( int keyCount )
    {
        // if keyCount is out of bounds of what a tree node can hold, it must be that we're
        // reading from an evicted page that just happened to look like a tree node.
        return keyCount >= 0 && keyCount <= maxKeyCount;
    }

    private boolean saneRead()
    {
        return keyCountIsSane( keyCount ) && KeySearch.isSuccess( searchResult );
    }

    /**
     * Perform a generation catchup, updates current root and update range to start from
     * previously returned key. Should be followed by a call to {@link #traverseDownToFirstLeaf()}
     * or if already in that method just loop again.
     * <p>
     * Caller should retry most recent read after calling this method.
     *
     * @throws IOException on {@link PageCursor}.
     */
    private void prepareToStartFromRoot() throws IOException
    {
        genCatchup();
        lastFollowedPointerGen = rootCatchup.get().goTo( cursor );
        if ( !first )
        {
            layout.copyKey( prevKey, fromInclusive );
        }
    }

    /**
     * Verifies that the generation of the tree node arrived at matches the generation of the pointer
     * pointing to the tree node. Generation of the node cannot be higher than the generation of the pointer -
     * if it is then it means that the tree node has been removed (or made obsolete) and reused since we read the
     * pointer pointing to it and that the seek is now in an invalid location and needs to be restarted from the root.
     *
     * @return {@code true} if generation matches, otherwise {@code false} if seek needs to be restarted from root.
     */
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

    /**
     * Checks the provided pointer read and if not successful performs a generation catch-up with
     * {@link #genSupplier} to allow reading that same pointer again given the updated generation context.
     *
     * @param pointer read result to check for success.
     * @param allowNoNode whether or not pointer is allowed to be "null".
     * @return {@code true} if there was a generation catch-up called and generation was actually updated,
     * this means that caller should retry its most recent read.
     */
    private boolean pointerCheckingWithGenCatchup( long pointer, boolean allowNoNode )
    {
        if ( !GenSafePointerPair.isSuccess( pointer ) )
        {
            // An unexpected sibling read, this could have been caused by a concurrent checkpoint
            // where generation has been incremented. Re-read generation and, if changed since this
            // seek started then update generation locally
            if ( genCatchup() )
            {
                return true;
            }
            PointerChecking.checkPointer( pointer, allowNoNode );
        }
        return false;
    }

    /**
     * Updates generation using the {@link #genSupplier}. If there has been a generation change
     * since construction of this seeker or since last calling this method the generation context in this
     * seeker is updated.
     *
     * @return {@code true} if generation was updated, which means that caller should retry its most recent read.
     */
    private boolean genCatchup()
    {
        long newGen = genSupplier.getAsLong();
        long newStableGen = Gen.stableGen( newGen );
        long newUnstableGen = Gen.unstableGen( newGen );
        if ( newStableGen != stableGen || newUnstableGen != unstableGen )
        {
            stableGen = newStableGen;
            unstableGen = newUnstableGen;
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
