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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.Direction;

import static java.lang.Math.toIntExact;

/**
 * Caches of parts of node store and relationship group store. A crucial part of batch import where
 * any random access must be covered by this cache. All I/O, both read and write must be sequential.
 *
 * <pre>
 * Main array (index into array is nodeId):
 * [ID,DEGREE]
 *
 * ID means:
 * - DEGREE >= THRESHOLD: pointer into RelationshipGroupCache array
 *   RelationshipGroupCache array:
 *   [NEXT,OUT_ID,OUT_DEGREE,IN_ID,IN_DEGREE,LOOP_ID,LOOP_DEGREE]
 * - DEGREE < THRESHOLD: last seen relationship id for this node
 * </pre>
 *
 * This class is designed to be thread safe if callers are coordinated such that different threads owns different
 * parts of the main cache array, with the constraint that a thread which accesses item N must continue doing
 * so in order to make further changes to N, if another thread accesses N the semantics will no longer hold.
 *
 * Since multiple threads are making changes external memory synchronization is also required in between
 * a phase of making changes using {@link #getAndPutRelationship(long, Direction, long, boolean)} and e.g
 * {@link #visitChangedNodes(NodeChangeVisitor, boolean)}.
 */
public class NodeRelationshipCache implements MemoryStatsVisitor.Visitable
{
    private static final int CHUNK_SIZE = 1_000_000;
    private static final long EMPTY = -1;
    private static final long MAX_RELATIONSHIP_ID = (1L << 48/*6B*/) - 2/*reserving -1 as legal default value*/;
    // if count goes beyond this max count then count is redirected to bigCounts and index into that array
    // is stored as value in count offset
    static final int MAX_SMALL_COUNT = (1 << 29/*3 change bits*/) - 2/*reserving -1 as legal default value*/;
    // this max count is pessimistic in that it's what community format can hold, still pretty big.
    // we can make this as big as our storage needs them later on
    static final long MAX_COUNT = (1L << 35) - 1;

    // Sizes and offsets of values in each sparse node ByteArray item
    private static final int ID_SIZE = 6;
    private static final int COUNT_SIZE = 4;
    private static final int ID_AND_COUNT_SIZE = ID_SIZE + COUNT_SIZE;
    private static final int SPARSE_ID_OFFSET = 0;
    private static final int SPARSE_COUNT_OFFSET = ID_SIZE;

    // Masking for tracking changes per node
    private static final int DENSE_NODE_CHANGED_MASK = 0x80000000;
    private static final int SPARSE_NODE_CHANGED_MASK = 0x40000000;
    private static final int BIG_COUNT_MASK = 0x20000000;
    private static final int COUNT_FLAGS_MASKS = DENSE_NODE_CHANGED_MASK | SPARSE_NODE_CHANGED_MASK | BIG_COUNT_MASK;
    private static final int COUNT_MASK = ~COUNT_FLAGS_MASKS;

    private ByteArray array;
    private byte[] chunkChangedArray;
    private final int denseNodeThreshold;
    private final RelGroupCache relGroupCache;
    private long highNodeId;
    // This cache participates in scans backwards and forwards, marking entities as changed in the process.
    // When going forward (forward==true) changes are marked with a set bit, a cleared bit when going bachwards.
    // This way there won't have to be a clearing of the change bits in between the scans.
    private volatile boolean forward = true;
    private final int chunkSize;
    private final NumberArrayFactory arrayFactory;
    private final LongArray bigCounts;
    private final AtomicInteger bigCountsCursor = new AtomicInteger();

    public NodeRelationshipCache( NumberArrayFactory arrayFactory, int denseNodeThreshold )
    {
        this( arrayFactory, denseNodeThreshold, CHUNK_SIZE, 0 );
    }

    NodeRelationshipCache( NumberArrayFactory arrayFactory, int denseNodeThreshold, int chunkSize, long base )
    {
        this.arrayFactory = arrayFactory;
        this.chunkSize = chunkSize;
        this.denseNodeThreshold = denseNodeThreshold;
        this.bigCounts = arrayFactory.newDynamicLongArray( 1_000, 0 );
        this.relGroupCache = new RelGroupCache( arrayFactory, chunkSize, base );
    }

    private static byte[] minusOneBytes( int length )
    {
        byte[] bytes = new byte[length];
        Arrays.fill( bytes, (byte) -1 );
        return bytes;
    }

    /**
     * Increment relationship count for {@code nodeId}.
     * @param nodeId node to increment relationship count for.
     * @return count after the increment.
     */
    public long incrementCount( long nodeId )
    {
        return incrementCount( array, nodeId, SPARSE_COUNT_OFFSET );
    }

    /**
     * Should only be used by tests
     */
    void setCount( long nodeId, long count, Direction direction )
    {
        if ( isDense( nodeId ) )
        {
            long relGroupId = all48Bits( array, nodeId, SPARSE_ID_OFFSET );
            relGroupCache.getAndSetCount( relGroupId, direction, count );
        }
        else
        {
            setCount( array, nodeId, SPARSE_COUNT_OFFSET, count );
        }
    }

    /**
     * This method sets count (node degree, really). It's somewhat generic in that it accepts
     * array and offset to set the count into. This is due to there being multiple places where
     * we store counts. Simplest one is for sparse nodes, which live in the main
     * NodeRelationshipCache.array at the dedicated offset. Other counts live in RelGroupCache.array
     * which contain three counts, one for each direction. That's covered by array and offset,
     * the count field works the same in all those scenarios. It's an integer which happens to have
     * some other flags at msb, so it's the 29 lsb bits which represents the count. 2^29 is merely
     * 1/2bn and so the count field has its 30th bit marking whether or not it's a "big count",
     * if it is then the 29 count bits instead point to an array index/slot into bigCounts array
     * which has much bigger space per count. This is of course quite rare, but nice to support.
     *
     * <pre>
     * "small" count, i.e. < 2^29
     * [  0c,cccc][cccc,cccc][cccc,cccc][cccc,cccc]
     *    │└──────────────────┬──────────────────┘
     *    │       bits containing actual count
     *  0 marking that this is a small count
     *
     * "big" count, i.e. >= 2^29
     * [  1i,iiii][iiii,iiii][iiii,iiii][iiii,iiii]
     *    │└──────────────────┬──────────────────┘
     *    │    bits containing array index into bigCounts array which contains the actual count
     *  1 marking that this is a big count
     * </pre>
     *
     * so the bigCounts array is shared between all different types of counts, because big counts are so rare
     *
     * @param array {@link ByteArray} to set count in
     * @param nodeId node id, i.e. array index
     * @param offset offset on that array index (a ByteArray feature)
     * @param count count to set at this position
     */
    private void setCount( ByteArray array, long nodeId, int offset, long count )
    {
        assertValidCount( nodeId, count );

        if ( count > MAX_SMALL_COUNT )
        {
            int rawCount = array.getInt( nodeId, offset );
            int slot;
            if ( rawCount == -1 || !isBigCount( rawCount ) )
            {
                // Allocate a slot in the bigCounts array
                slot = bigCountsCursor.getAndIncrement();
                array.setInt( nodeId, offset, BIG_COUNT_MASK | slot );
            }
            else
            {
                slot = countValue( rawCount );
            }
            bigCounts.set( slot, count );
        }
        else
        {   // We can simply set it
            array.setInt( nodeId, offset, toIntExact( count ) );
        }
    }

    private static void assertValidCount( long nodeId, long count )
    {
        if ( count > MAX_COUNT )
        {
            // Meaning there are bits outside of this mask, meaning this value is too big
            throw new IllegalStateException( "Tried to increment count of node id " + nodeId + " to " + count +
                    ", which is too big in one single import" );
        }
    }

    private static boolean isBigCount( int storedCount )
    {
        return (storedCount & BIG_COUNT_MASK) != 0;
    }

    /**
     * Called by the one calling {@link #incrementCount(long)} after all nodes have been added.
     * Done like this since currently it's just overhead trying to maintain a high id in the face
     * of current updates, whereas it's much simpler to do this from the code incrementing the counts.
     *
     * @param nodeId high node id in the store, e.g. the highest node id + 1
     */
    public void setHighNodeId( long nodeId )
    {
        this.highNodeId = nodeId;
        this.array = arrayFactory.newByteArray( highNodeId, minusOneBytes( ID_AND_COUNT_SIZE ) );
        this.chunkChangedArray = new byte[chunkOf( nodeId ) + 1];
    }

    public long getHighNodeId()
    {
        return this.highNodeId;
    }

    /**
     * @see #setCount(ByteArray, long, int, long) setCount for description on how bigCounts work
     */
    private long getCount( ByteArray array, long index, int offset )
    {
        int rawCount = array.getInt( index, offset );
        int count = countValue( rawCount );
        if ( count == COUNT_MASK )
        {
            // All bits 1, i.e. default initialized field
            return 0;
        }

        if ( isBigCount( rawCount ) )
        {
            // 'count' means index into bigCounts in this context
            return bigCounts.get( count );
        }

        return count;
    }

    private static int countValue( int rawCount )
    {
        return rawCount & COUNT_MASK;
    }

    private long incrementCount( ByteArray array, long nodeId, int offset )
    {
        array = array.at( nodeId );
        long count = getCount( array, nodeId, offset ) + 1;
        setCount( array, nodeId, offset, count );
        return count;
    }

    /**
     * @param nodeId node to check whether dense or not.
     * @return whether or not the given {@code nodeId} is dense. A node is sparse if it has less relationships,
     * e.g. has had less calls to {@link #incrementCount(long)}, then the given dense node threshold.
     */
    public boolean isDense( long nodeId )
    {
        return isDense( array, nodeId );
    }

    private boolean isDense( ByteArray array, long nodeId )
    {
        if ( denseNodeThreshold == EMPTY )
        {   // We haven't initialized the rel group cache yet
            return false;
        }

        return getCount( array, nodeId, SPARSE_COUNT_OFFSET ) >= denseNodeThreshold;
    }

    /**
     * Puts a relationship id to be the head of a relationship chain. If the node is sparse then
     * the head is set directly in the cache, else if dense which head to update will depend on
     * the {@code direction}.
     *
     * @param nodeId node to update relationship head for.
     * @param direction {@link Direction} this node represents for this relationship.
     * @param firstRelId the relationship id which is now the head of this chain.
     * @param incrementCount as side-effect also increment count for this chain.
     * @return the previous head of the updated relationship chain.
     */
    public long getAndPutRelationship( long nodeId, Direction direction, long firstRelId,
            boolean incrementCount )
    {
        if ( firstRelId > MAX_RELATIONSHIP_ID )
        {
            throw new IllegalArgumentException( "Illegal relationship id, max is " + MAX_RELATIONSHIP_ID );
        }

        /*
         * OK so the story about counting goes: there's an initial pass for counting number of relationships
         * per node, globally, not per type/direction. After that the relationship group cache is initialized
         * and the relationship stage is executed where next pointers are constructed. That forward pass should
         * not increment the global count, but it should increment the type/direction counts.
         */

        ByteArray array = this.array.at( nodeId );
        long existingId = all48Bits( array, nodeId, SPARSE_ID_OFFSET );
        boolean dense = isDense( array, nodeId );
        boolean wasChanged = markAsChanged( array, nodeId, changeMask( dense ) );
        markChunkAsChanged( nodeId, dense );
        if ( dense )
        {
            if ( existingId == EMPTY )
            {
                existingId = relGroupCache.allocate();
                setRelationshipId( array, nodeId, existingId );
                wasChanged = false; // no need to clear when we just allocated it
            }
            return relGroupCache.putRelationship( existingId, direction, firstRelId, incrementCount, wasChanged );
        }

        // Don't increment count for sparse node since that has already been done in a previous pass
        setRelationshipId( array, nodeId, firstRelId );
        return wasChanged ? EMPTY : existingId;
    }

    private void markChunkAsChanged( long nodeId, boolean dense )
    {
        byte mask = chunkChangeMask( dense );
        if ( !chunkHasChange( nodeId, mask ) )
        {
            int chunk = chunkOf( nodeId );
            if ( (chunkChangedArray[chunk] & mask) == 0 )
            {
                // Multiple threads may update this chunk array, synchronized performance-wise is fine on change since
                // it'll only happen at most a couple of times for each chunk (1M).
                synchronized ( chunkChangedArray )
                {
                    chunkChangedArray[chunk] |= mask;
                }
            }
        }
    }

    private int chunkOf( long nodeId )
    {
        return toIntExact( nodeId / chunkSize );
    }

    private static byte chunkChangeMask( boolean dense )
    {
        return (byte) (1 << (dense ? 1 : 0));
    }

    private boolean markAsChanged( ByteArray array, long nodeId, int mask )
    {
        int bits = array.getInt( nodeId, SPARSE_COUNT_OFFSET );
        boolean changeBitIsSet = (bits & mask) != 0;
        boolean changeBitWasFlipped = changeBitIsSet != forward;
        if ( changeBitWasFlipped )
        {
            bits ^= mask; // flip the mask bit
            array.setInt( nodeId, SPARSE_COUNT_OFFSET, bits );
        }
        return changeBitWasFlipped;
    }

    private static boolean nodeIsChanged( ByteArray array, long nodeId, long mask )
    {
        int bits = array.getInt( nodeId, SPARSE_COUNT_OFFSET );

        // The values in the cache are initialized with -1, i.e. all bits set, i.e. also the
        // change bits set. For nodes that gets at least one call to incrementCount these will be
        // set properly to reflect the count, e.g. 1, 2, 3, a.s.o. Nodes that won't get any call
        // to incrementCount will not see any changes to them either, so for this matter we check
        // if the count field is -1 as a whole and if so we can tell we've just run into such a node
        // and we can safely say it hasn't been changed.
        if ( bits == 0xFFFFFFFF )
        {
            return false;
        }
        return (bits & mask) != 0;
    }

    private void setRelationshipId( ByteArray array, long nodeId, long firstRelId )
    {
        array.set6ByteLong( nodeId, SPARSE_ID_OFFSET, firstRelId );
    }

    private static long getRelationshipId( ByteArray array, long nodeId )
    {
        return array.get6ByteLong( nodeId, SPARSE_ID_OFFSET );
    }

    private static long all48Bits( ByteArray array, long index, int offset )
    {
        return all48Bits( array.get6ByteLong( index, offset ) );
    }

    private static long all48Bits( long raw )
    {
        return raw == -1L ? raw : raw & 0xFFFFFFFFFFFFL;
    }

    /**
     * Used when setting node nextRel fields. Gets the first relationship for this node,
     * or the relationship group id. As a side effect this method also creates a relationship group
     * if this node is dense, and returns that relationship group record id.
     *
     * @param nodeId id to get first relationship for.
     * @param visitor {@link GroupVisitor} which will be notified with data about group to be created.
     * This visitor is expected to create the group.
     * @return the first relationship if node is sparse, or the result of {@link GroupVisitor} if dense.
     */
    public long getFirstRel( long nodeId, GroupVisitor visitor )
    {
        assert forward : "This should only be done at forward scan";

        ByteArray array = this.array.at( nodeId );
        long id = getRelationshipId( array, nodeId );
        if ( id != EMPTY && isDense( array, nodeId ) )
        {   // Indirection into rel group cache
            return relGroupCache.visitGroup( nodeId, id, visitor );
        }

        return id;
    }

    /**
     * First a note about tracking which nodes have been updated with new relationships by calls to
     * {@link #getAndPutRelationship(long, Direction, long, boolean)}:
     *
     * We use two high bits of the count field in the "main" array to mark whether or not a change
     * have been made to a node. One bit for a sparse node and one for a dense. Sparse and dense nodes
     * now have different import cycles. When importing the relationships, all relationships are imported,
     * one type at a time, but only dense nodes and relationship chains for dense nodes are updated
     * for every type. After all types have been imported the sparse chains and nodes are updated in one pass.
     *
     * Tells this cache which direction it's about to observe changes for. If {@code true} then changes
     * marked as the change-bit set and an unset change-bit means a change is the first one for that node.
     * {@code false} is the opposite. This is so that there won't need to be any clearing of the cache
     * in between forward and backward linking, since the cache can be rather large.
     *
     * @param forward {@code true} if going forward and having change marked as a set bit, otherwise
     * change is marked with an unset bit.
     */
    public void setForwardScan( boolean forward )
    {
        this.forward = forward;
    }

    /**
     * Returns the count (degree) of the requested relationship chain. If node is sparse then the single count
     * for this node is returned, otherwise if the node is dense the count for the chain for the specific
     * direction is returned.
     *
     * For dense nodes the count will be reset after returned here. This is so that the same memory area
     * can be used for the next type import.
     *
     * @param nodeId node to get count for.
     * @param direction {@link Direction} to get count for.
     * @return count (degree) of the requested relationship chain.
     */
    public long getCount( long nodeId, Direction direction )
    {
        ByteArray array = this.array.at( nodeId );
        if ( isDense( array, nodeId ) )
        {   // Indirection into rel group cache
            long id = getRelationshipId( array, nodeId );
            return id == EMPTY ? 0 : relGroupCache.getAndSetCount( id, direction, 0 );
        }

        return getCount( array, nodeId, SPARSE_COUNT_OFFSET );
    }

    public interface GroupVisitor
    {
        /**
         * Visits with data required to create a relationship group.
         * Type can be decided on the outside since there'll be only one type per node.
         *
         * @param nodeId node id.
         * @param next next relationship group.
         * @param out first outgoing relationship id.
         * @param in first incoming relationship id.
         * @param loop first loop relationship id.
         * @return the created relationship group id.
         */
        long visit( long nodeId, long next, long out, long in, long loop );
    }

    public static final GroupVisitor NO_GROUP_VISITOR = (nodeId, next, out, in, loop) -> -1;

    private class RelGroupCache implements AutoCloseable, MemoryStatsVisitor.Visitable
    {
        private static final int NEXT_OFFSET = 0;
        private static final int BASE_IDS_OFFSET = ID_SIZE;

        // Used for testing high id values. Should always be zero in production
        private final long base;
        private final ByteArray array;
        private final AtomicLong nextFreeId;

        RelGroupCache( NumberArrayFactory arrayFactory, long chunkSize, long base )
        {
            this.base = base;
            assert chunkSize > 0;
            this.array = arrayFactory.newDynamicByteArray( chunkSize,
                    minusOneBytes( ID_SIZE/*next*/ + (ID_AND_COUNT_SIZE) * Direction.values().length ) );
            this.nextFreeId = new AtomicLong( base );
        }

        private void clearRelationships( ByteArray array, long relGroupId )
        {
            array.set6ByteLong( relGroupId, directionOffset( Direction.OUTGOING ), EMPTY );
            array.set6ByteLong( relGroupId, directionOffset( Direction.INCOMING ), EMPTY );
            array.set6ByteLong( relGroupId, directionOffset( Direction.BOTH ), EMPTY );
        }

        /**
         * For dense nodes we <strong>reset</strong> count after reading because we only ever need
         * that value once and the piece of memory holding this value will be reused for another
         * relationship chain for this node after this point in time, where the count should
         * restart from 0.
         */
        long getAndSetCount( long id, Direction direction, long newCount )
        {
            id = rebase( id );
            ByteArray array = this.array.at( id );
            if ( id == EMPTY )
            {
                return 0;
            }

            int offset = countOffset( direction );
            long count = getCount( array, id, offset );
            setCount( array, id, offset, newCount );
            return count;
        }

        /**
         * Compensate for test value of index (to avoid allocating all your RAM)
         */
        private long rebase( long index )
        {
            return index - base;
        }

        private long nextFreeId()
        {
            return nextFreeId.getAndIncrement();
        }

        private long visitGroup( long nodeId, long relGroupIndex, GroupVisitor visitor )
        {
            long index = rebase( relGroupIndex );
            ByteArray array = this.array.at( index );
            long out = all48Bits( array, index, directionOffset( Direction.OUTGOING ) );
            long in = all48Bits( array, index, directionOffset( Direction.INCOMING ) );
            long loop = all48Bits( array, index, directionOffset( Direction.BOTH ) );
            long next = all48Bits( array, index, NEXT_OFFSET );
            long nextId = out == EMPTY && in == EMPTY && loop == EMPTY ? EMPTY :
                visitor.visit( nodeId, next, out, in, loop );

            // Save the returned next id for later, when the next group for this node is created
            // then we know what to point this group's next to.
            array.set6ByteLong( index, NEXT_OFFSET, nextId );
            return nextId;
        }

        private int directionOffset( Direction direction )
        {
            return BASE_IDS_OFFSET + (direction.ordinal() * ID_AND_COUNT_SIZE);
        }

        private int countOffset( Direction direction )
        {
            return directionOffset( direction ) + ID_SIZE;
        }

        long allocate()
        {
            return nextFreeId();
        }

        long putRelationship( long relGroupIndex, Direction direction,
                long relId, boolean increment, boolean clear )
        {
            long index = rebase( relGroupIndex );
            ByteArray array = this.array.at( index );
            int directionOffset = directionOffset( direction );
            long previousId;
            if ( clear )
            {
                clearRelationships( array, index );
                previousId = EMPTY;
            }
            else
            {
                previousId = all48Bits( array, index, directionOffset );
            }
            array.set6ByteLong( index, directionOffset, relId );
            if ( increment )
            {
                incrementCount( array, index, countOffset( direction ) );
            }
            return previousId;
        }

        @Override
        public void close()
        {
            if ( array != null )
            {
                array.close();
            }
        }

        @Override
        public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
        {
            nullSafeMemoryStatsVisitor( array, visitor );
        }
    }

    @Override
    public String toString()
    {
        return array.toString();
    }

    public void close()
    {
        if ( array != null )
        {
            array.close();
        }
        if ( relGroupCache != null )
        {
            relGroupCache.close();
        }
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        nullSafeMemoryStatsVisitor( array, visitor );
        relGroupCache.acceptMemoryStatsVisitor( visitor );
    }

    static void nullSafeMemoryStatsVisitor( MemoryStatsVisitor.Visitable visitable, MemoryStatsVisitor visitor )
    {
        if ( visitable != null )
        {
            visitable.acceptMemoryStatsVisitor( visitor );
        }
    }

    private static int changeMask( boolean dense )
    {
        return dense ? DENSE_NODE_CHANGED_MASK : SPARSE_NODE_CHANGED_MASK;
    }

    @FunctionalInterface
    public interface NodeChangeVisitor
    {
        void change( long nodeId, ByteArray array );
    }

    /**
     * Efficiently visits changed nodes, e.g. nodes that have had any relationship chain updated by
     * {@link #getAndPutRelationship(long, Direction, long, boolean)}.
     *
     * @param visitor {@link NodeChangeVisitor} which will be notified about all changes.
     * @param denseNodes {@code true} for visiting changed dense nodes, {@code false} for visiting
     * changed sparse nodes.
     */
    public void visitChangedNodes( NodeChangeVisitor visitor, boolean denseNodes )
    {
        long mask = changeMask( denseNodes );
        byte chunkMask = chunkChangeMask( denseNodes );
        for ( long nodeId = 0; nodeId < highNodeId; )
        {
            if ( !chunkHasChange( nodeId, chunkMask ) )
            {
                nodeId += chunkSize;
                continue;
            }

            ByteArray chunk = array.at( nodeId );
            for ( int i = 0; i < chunkSize && nodeId < highNodeId; i++, nodeId++ )
            {
                if ( isDense( chunk, nodeId ) == denseNodes && nodeIsChanged( chunk, nodeId, mask ) )
                {
                    visitor.change( nodeId, chunk );
                }
            }
        }
    }

    /**
     * Clears the high-level change marks.
     *
     * @param denseNodes {@code true} for clearing marked dense nodes, {@code false} for clearing marked sparse nodes.
     */
    public void clearChangedChunks( boolean denseNodes )
    {
        // Executed by a single thread, so no synchronized required
        byte chunkMask = chunkChangeMask( denseNodes );
        for ( int i = 0; i < chunkChangedArray.length; i++ )
        {
            chunkChangedArray[i] &= ~chunkMask;
        }
    }

    private boolean chunkHasChange( long nodeId, byte chunkMask )
    {
        int chunkId = chunkOf( nodeId );
        return (chunkChangedArray[chunkId] & chunkMask) != 0;
    }
}
