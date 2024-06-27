/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport.cache;

import static java.lang.Long.min;
import static java.lang.Math.toIntExact;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.batchimport.cache.idmapping.string.BigIdTracker;
import org.neo4j.internal.helpers.Numbers;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

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
 * a phase of making changes using {@link #getAndPutRelationship(long, int, Direction, long, boolean)} and e.g
 * {@link #visitChangedNodes(NodeChangeVisitor, int)}.
 */
public class NodeRelationshipCache implements MemoryStatsVisitor.Visitable, AutoCloseable {
    private static final int CHUNK_SIZE = 1_000_000;
    private static final long EMPTY = -1;
    private static final long MAX_RELATIONSHIP_ID = (1L << 48 /*6B*/) - 2 /*reserving -1 as legal default value*/;
    // if count goes beyond this max count then count is redirected to bigCounts and index into that array
    // is stored as value in count offset
    static final int MAX_SMALL_COUNT = (1 << 28 /*4 change bits*/) - 2 /*reserving -1 as legal default value*/;
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
    private static final int EXPLICITLY_DENSE_MASK = 0x10000000;
    private static final int COUNT_FLAGS_MASKS =
            DENSE_NODE_CHANGED_MASK | SPARSE_NODE_CHANGED_MASK | BIG_COUNT_MASK | EXPLICITLY_DENSE_MASK;
    private static final int COUNT_MASK = ~COUNT_FLAGS_MASKS;

    private static final int TYPE_SIZE = 3;
    public static final int GROUP_ENTRY_SIZE =
            TYPE_SIZE + ID_SIZE /*next*/ + ID_AND_COUNT_SIZE * Direction.values().length;

    private ByteArray array;
    private byte[] chunkChangedArray;
    private final int denseNodeThreshold;
    private final MemoryTracker memoryTracker;
    private final RelGroupCache relGroupCache;
    private long highNodeId;
    // This cache participates in scans backwards and forwards, marking entities as changed in the process.
    // When going forward (forward==true) changes are marked with a set bit, a cleared bit when going backwards.
    // This way there won't have to be a clearing of the change bits in between the scans.
    private volatile boolean forward = true;
    private final int chunkSize;
    private final NumberArrayFactory arrayFactory;
    private final LongArray bigCounts;
    private final AtomicInteger bigCountsCursor = new AtomicInteger();
    private long numberOfDenseNodes;

    public NodeRelationshipCache(NumberArrayFactory arrayFactory, int denseNodeThreshold, MemoryTracker memoryTracker) {
        this(arrayFactory, denseNodeThreshold, CHUNK_SIZE, 0, memoryTracker);
    }

    NodeRelationshipCache(
            NumberArrayFactory arrayFactory,
            int denseNodeThreshold,
            int chunkSize,
            long base,
            MemoryTracker memoryTracker) {
        this.arrayFactory = arrayFactory;
        this.chunkSize = chunkSize;
        this.denseNodeThreshold = denseNodeThreshold;
        this.memoryTracker = memoryTracker;
        this.bigCounts = arrayFactory.newDynamicLongArray(1_000, 0, memoryTracker);
        this.relGroupCache = new RelGroupCache(arrayFactory, chunkSize, base, memoryTracker);
    }

    /**
     * Increment relationship count for {@code nodeId}.
     * @param nodeId node to increment relationship count for.
     * @return count after the increment.
     */
    public long incrementCount(long nodeId) {
        return incrementCount(array, nodeId, SPARSE_COUNT_OFFSET);
    }

    @VisibleForTesting
    public void setCount(long nodeId, long count, int typeId, Direction direction) {
        if (isDense(nodeId)) {
            long relGroupId = all48Bits(array, nodeId, SPARSE_ID_OFFSET);
            relGroupCache.setCount(relGroupId, typeId, direction, count);
        } else {
            setCount(array, nodeId, SPARSE_COUNT_OFFSET, count);
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
     * @param index node id, i.e. array index
     * @param offset offset on that array index (a ByteArray feature)
     * @param count count to set at this position
     */
    private void setCount(ByteArray array, long index, int offset, long count) {
        assertValidCount(index, count);

        int rawCount = array.getInt(index, offset);
        if (count > MAX_SMALL_COUNT) {
            int slot;
            if (rawCount == -1 || !isBigCount(rawCount)) {
                // Allocate a slot in the bigCounts array
                slot = bigCountsCursor.getAndIncrement();
                array.setInt(index, offset, (rawCount & COUNT_FLAGS_MASKS) | BIG_COUNT_MASK | slot);
            } else {
                slot = countValue(rawCount);
            }
            bigCounts.set(slot, count);
        } else { // We can simply set it
            array.setInt(index, offset, (rawCount & COUNT_FLAGS_MASKS) | toIntExact(count));
        }
    }

    private static void assertValidCount(long nodeId, long count) {
        if (count > MAX_COUNT) {
            // Meaning there are bits outside of this mask, meaning this value is too big
            throw new IllegalStateException("Tried to increment count of node id " + nodeId + " to " + count
                    + ", which is too big in one single import");
        }
    }

    private static boolean isBigCount(int storedCount) {
        return (storedCount & BIG_COUNT_MASK) != 0;
    }

    /**
     * Called by the one calling {@link #incrementCount(long)} after all nodes have been added.
     * Done like this since currently it's just overhead trying to maintain a high id in the face
     * of current updates, whereas it's much simpler to do this from the code incrementing the counts.
     *
     * @param nodeCount high node id in the store, e.g. the highest node id + 1
     */
    public void setNodeCount(long nodeCount) {
        if (nodeCount - 1 > BigIdTracker.ID_MASK) {
            throw new IllegalArgumentException(
                    String.format("Invalid number of nodes %d. Max is %d", nodeCount, BigIdTracker.ID_MASK));
        }

        this.highNodeId = nodeCount;
        var defaultValue = new byte[ID_AND_COUNT_SIZE];
        Arrays.fill(defaultValue, 0, ID_SIZE, (byte) -1);
        Arrays.fill(defaultValue, ID_SIZE, ID_AND_COUNT_SIZE, (byte) 0);
        this.array = arrayFactory.newByteArray(highNodeId, defaultValue, memoryTracker);
        this.chunkChangedArray = new byte[chunkOf(nodeCount) + 1];
    }

    /**
     * @see #setCount(ByteArray, long, int, long) setCount for description on how bigCounts work
     */
    private long getCount(ByteArray array, long index, int offset) {
        int rawCount = array.getInt(index, offset);
        int count = countValue(rawCount);

        if (isBigCount(rawCount)) {
            // 'count' means index into bigCounts in this context
            return bigCounts.get(count);
        }

        return count;
    }

    private static int countValue(int rawCount) {
        return rawCount & COUNT_MASK;
    }

    private long incrementCount(ByteArray array, long index, int offset) {
        array = array.at(index);
        long count = getCount(array, index, offset) + 1;
        setCount(array, index, offset, count);
        return count;
    }

    /**
     * @param nodeId node to check whether dense or not.
     * @return whether or not the given {@code nodeId} is dense. A node is sparse if it has less relationships,
     * e.g. has had less calls to {@link #incrementCount(long)}, then the given dense node threshold.
     */
    public boolean isDense(long nodeId) {
        return isDense(array, nodeId);
    }

    /**
     * Use for incremental import where letting the importer know that a particular node is dense, even if its relationship count
     * is less than the dense threshold. This will make it cheaper to merge the incremental data later.
     * @param nodeId node ID to mark as being dense.
     */
    public void markAsExplicitlyDense(long nodeId) {
        int bits = array.getInt(nodeId, SPARSE_COUNT_OFFSET);
        array.setInt(nodeId, SPARSE_COUNT_OFFSET, bits | EXPLICITLY_DENSE_MASK);
    }

    private boolean isDense(ByteArray array, long nodeId) {
        if (denseNodeThreshold == EMPTY) { // We haven't initialized the rel group cache yet
            return false;
        }

        var explicitlyDense = (array.getInt(nodeId, SPARSE_COUNT_OFFSET) & EXPLICITLY_DENSE_MASK) != 0;
        return explicitlyDense || getCount(array, nodeId, SPARSE_COUNT_OFFSET) >= denseNodeThreshold;
    }

    /**
     * Puts a relationship id to be the head of a relationship chain. If the node is sparse then
     * the head is set directly in the cache, else if dense which head to update will depend on
     * the {@code direction}.
     *
     * @param nodeId node to update relationship head for.
     * @param typeId relationship type id.
     * @param direction {@link Direction} this node represents for this relationship.
     * @param firstRelId the relationship id which is now the head of this chain.
     * @param incrementCount as side-effect also increment count for this chain.
     * @return the previous head of the updated relationship chain.
     */
    public long getAndPutRelationship(
            long nodeId, int typeId, Direction direction, long firstRelId, boolean incrementCount) {
        if (firstRelId > MAX_RELATIONSHIP_ID) {
            throw new IllegalArgumentException("Illegal relationship id, max is " + MAX_RELATIONSHIP_ID);
        }

        /*
         * OK so the story about counting goes: there's an initial pass for counting number of relationships
         * per node, globally, not per type/direction. After that the relationship group cache is initialized
         * and the relationship stage is executed where next pointers are constructed. That forward pass should
         * not increment the global count, but it should increment the type/direction counts.
         */

        ByteArray array = this.array.at(nodeId);
        long existingId = all48Bits(array, nodeId, SPARSE_ID_OFFSET);
        boolean dense = isDense(array, nodeId);
        boolean wasChanged = markAsChanged(array, nodeId, changeMask(dense));
        markChunkAsChanged(nodeId, dense);
        if (dense) {
            if (existingId == EMPTY) {
                existingId = relGroupCache.allocate(typeId);
                setRelationshipId(array, nodeId, existingId);
            }
            return relGroupCache.getAndPutRelationship(existingId, typeId, direction, firstRelId, incrementCount);
        }

        // Don't increment count for sparse node since that has already been done in a previous pass
        setRelationshipId(array, nodeId, firstRelId);
        return wasChanged ? EMPTY : existingId;
    }

    private void markChunkAsChanged(long nodeId, boolean dense) {
        byte mask = chunkChangeMask(dense);
        if (!chunkHasChange(nodeId, mask)) {
            int chunk = chunkOf(nodeId);
            if ((chunkChangedArray[chunk] & mask) == 0) {
                // Multiple threads may update this chunk array, synchronized performance-wise is fine on change since
                // it'll only happen at most a couple of times for each chunk (1M).
                synchronized (chunkChangedArray) {
                    chunkChangedArray[chunk] |= mask;
                }
            }
        }
    }

    long calculateNumberOfDenseNodes() {
        long count = 0;
        for (long i = 0; i < highNodeId; i++) {
            if (isDense(i)) {
                count++;
            }
        }
        return count;
    }

    private int chunkOf(long nodeId) {
        return toIntExact(nodeId / chunkSize);
    }

    private static byte chunkChangeMask(boolean dense) {
        return (byte) (1 << (dense ? 1 : 0));
    }

    private boolean markAsChanged(ByteArray array, long nodeId, int mask) {
        int bits = array.getInt(nodeId, SPARSE_COUNT_OFFSET);
        boolean changeBitIsSet = (bits & mask) != 0;
        boolean changeBitWasFlipped = changeBitIsSet != forward;
        if (changeBitWasFlipped) {
            bits ^= mask; // flip the mask bit
            array.setInt(nodeId, SPARSE_COUNT_OFFSET, bits);
        }
        return changeBitWasFlipped;
    }

    private boolean nodeIsChanged(ByteArray array, long nodeId, long mask) {
        int bits = array.getInt(nodeId, SPARSE_COUNT_OFFSET);

        // The values in the cache are initialized with -1, i.e. all bits set, i.e. also the
        // change bits set. For nodes that gets at least one call to incrementCount these will be
        // set properly to reflect the count, e.g. 1, 2, 3, a.s.o. Nodes that won't get any call
        // to incrementCount will not see any changes to them either, so for this matter we check
        // if the count field is -1 as a whole and if so we can tell we've just run into such a node
        // and we can safely say it hasn't been changed.
        if (bits == 0xFFFFFFFF) {
            return false;
        }
        boolean changeBitIsSet = (bits & mask) != 0;
        return changeBitIsSet == forward;
    }

    private static void setRelationshipId(ByteArray array, long nodeId, long firstRelId) {
        array.set6ByteLong(nodeId, SPARSE_ID_OFFSET, firstRelId);
    }

    private static long getRelationshipId(ByteArray array, long nodeId) {
        return array.get6ByteLong(nodeId, SPARSE_ID_OFFSET);
    }

    private static long all48Bits(ByteArray array, long index, int offset) {
        return all48Bits(array.get6ByteLong(index, offset));
    }

    private static long all48Bits(long raw) {
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
    public long getFirstRel(long nodeId, GroupVisitor visitor) {
        ByteArray array = this.array.at(nodeId);
        long id = getRelationshipId(array, nodeId);
        if (id != EMPTY && isDense(array, nodeId)) { // Indirection into rel group cache
            return relGroupCache.visitGroup(nodeId, id, visitor);
        }

        return id;
    }

    /**
     * First a note about tracking which nodes have been updated with new relationships by calls to
     * {@link #getAndPutRelationship(long, int, Direction, long, boolean)}:
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
     * @param denseNodes whether or not this is about dense nodes. If so then some additional cache
     * preparation work needs to be done.
     */
    public void setForwardScan(boolean forward, boolean denseNodes) {
        if (this.forward == forward) {
            return;
        }

        // There's some additional preparations to do for dense nodes between each pass,
        // this is because that piece of memory is reused.
        if (denseNodes) {
            if (forward) {
                // Clear relationship group cache and references to it
                visitChangedNodes((nodeId, array) -> setRelationshipId(array, nodeId, EMPTY), NodeType.NODE_TYPE_DENSE);
                clearChangedChunks(true);
                relGroupCache.clear();
            } else {
                // Keep the relationship group cache entries, but clear all relationship chain heads
                relGroupCache.clearRelationshipIds();
            }
        }
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
     * @param typeId relationship type id to get count for.
     * @param direction {@link Direction} to get count for.
     * @param resetCountAfterRead whether to reset the count to 0 after reading and returning it.
     * @return count (degree) of the requested relationship chain.
     */
    public long getCount(long nodeId, int typeId, Direction direction, boolean resetCountAfterRead) {
        ByteArray array = this.array.at(nodeId);
        boolean dense = isDense(array, nodeId);
        if (dense) { // Indirection into rel group cache
            long id = getRelationshipId(array, nodeId);
            return id == EMPTY ? 0 : relGroupCache.getAndResetCount(id, typeId, direction, resetCountAfterRead);
        }

        return getCount(array, nodeId, SPARSE_COUNT_OFFSET);
    }

    public interface GroupVisitor {
        /**
         * Visits with data required to create a relationship group.
         * Type can be decided on the outside since there'll be only one type per node.
         *
         * @param nodeId node id.
         * @param typeId relationship type id.
         * @param out first outgoing relationship id.
         * @param in first incoming relationship id.
         * @param loop first loop relationship id.
         * @return the created relationship group id.
         */
        long visit(long nodeId, int typeId, long out, long in, long loop);
    }

    public static final GroupVisitor NO_GROUP_VISITOR = (nodeId, typeId, out, in, loop) -> -1;

    private class RelGroupCache implements AutoCloseable, MemoryStatsVisitor.Visitable {
        private static final int TYPE_OFFSET = 0;
        private static final int NEXT_OFFSET = TYPE_SIZE;
        private static final int BASE_IDS_OFFSET = NEXT_OFFSET + ID_SIZE;

        // Used for testing high id values. Should always be zero in production
        private final byte[] DEFAULT_VALUE = buildDefaultValue();
        private final long chunkSize;
        private final long base;
        private final ByteArray array;
        private final AtomicLong nextFreeId;

        RelGroupCache(NumberArrayFactory arrayFactory, long chunkSize, long base, MemoryTracker memoryTracker) {
            this.chunkSize = chunkSize;
            this.base = base;
            assert chunkSize > 0;
            this.array = arrayFactory.newDynamicByteArray(chunkSize, DEFAULT_VALUE, memoryTracker);
            this.nextFreeId = new AtomicLong(base);
        }

        private static byte[] buildDefaultValue() {
            // TYPE,NEXT,3x[ID,COUNT]
            // all COUNT should have 0 as default value
            byte[] value = new byte[GROUP_ENTRY_SIZE];
            Arrays.fill(value, (byte) -1);
            for (int i = 0; i < Direction.values().length; i++) {
                var fromIndex = BASE_IDS_OFFSET + (ID_AND_COUNT_SIZE * i) + ID_SIZE;
                Arrays.fill(value, fromIndex, fromIndex + COUNT_SIZE, (byte) 0);
            }
            return value;
        }

        private void clearIndex(ByteArray array, long relGroupId) {
            array.set(relGroupId, DEFAULT_VALUE);
        }

        long getAndResetCount(long relGroupIndex, int typeId, Direction direction, boolean resetCountAfterRead) {
            long index = rebase(relGroupIndex);
            while (index != EMPTY) {
                ByteArray array = this.array.at(index);
                if (getTypeId(array, index) == typeId) {
                    int offset = countOffset(direction);
                    long count = NodeRelationshipCache.this.getCount(array, index, offset);
                    if (resetCountAfterRead) {
                        NodeRelationshipCache.this.setCount(array, index, offset, 0);
                    }
                    return count;
                }
                index = getNext(array, index);
            }
            return 0;
        }

        void setCount(long relGroupIndex, int typeId, Direction direction, long count) {
            long index = rebase(relGroupIndex);
            while (index != EMPTY) {
                ByteArray array = this.array.at(index);
                if (getTypeId(array, index) == typeId) {
                    NodeRelationshipCache.this.setCount(array, index, countOffset(direction), count);
                    break;
                }
                index = getNext(array, index);
            }
        }

        long getNext(ByteArray array, long index) {
            return all48Bits(array, index, NEXT_OFFSET);
        }

        int getTypeId(ByteArray array, long index) {
            return array.get3ByteInt(index, TYPE_OFFSET);
        }

        /**
         * Compensate for test value of index (to avoid allocating all your RAM)
         */
        private long rebase(long index) {
            return index - base;
        }

        private long nextFreeId() {
            return nextFreeId.getAndIncrement();
        }

        private long visitGroup(long nodeId, long relGroupIndex, GroupVisitor visitor) {
            long currentIndex = rebase(relGroupIndex);
            long first = EMPTY;
            while (currentIndex != EMPTY) {
                ByteArray array = this.array.at(currentIndex);
                long out = all48Bits(array, currentIndex, idOffset(Direction.OUTGOING));
                int typeId = getTypeId(array, currentIndex);
                long in = all48Bits(array, currentIndex, idOffset(Direction.INCOMING));
                long loop = all48Bits(array, currentIndex, idOffset(Direction.BOTH));
                long next = getNext(array, currentIndex);
                long nextId = out == EMPTY && in == EMPTY && loop == EMPTY
                        ? EMPTY
                        : visitor.visit(nodeId, typeId, out, in, loop);
                if (first == EMPTY) { // This is the one we return
                    first = nextId;
                }
                currentIndex = next;
            }
            return first;
        }

        private int idOffset(Direction direction) {
            return BASE_IDS_OFFSET + (direction.ordinal() * ID_AND_COUNT_SIZE);
        }

        private int countOffset(Direction direction) {
            return idOffset(direction) + ID_SIZE;
        }

        long allocate(int typeId) {
            long index = nextFreeId();
            long rebasedIndex = rebase(index);
            ByteArray array = this.array.at(rebasedIndex);
            clearIndex(array, rebasedIndex);
            array.set3ByteInt(rebasedIndex, TYPE_OFFSET, Numbers.safeCheck3ByteInt(typeId));
            return index;
        }

        private long getAndPutRelationship(
                long relGroupIndex, int typeId, Direction direction, long relId, boolean incrementCount) {
            long index = rebase(relGroupIndex);
            index = findOrAllocateIndex(index, typeId);
            ByteArray array = this.array.at(index);
            int directionOffset = idOffset(direction);
            long previousId = all48Bits(array, index, directionOffset);
            array.set6ByteLong(index, directionOffset, relId);
            if (incrementCount) {
                incrementCount(array, index, countOffset(direction));
            }
            return previousId;
        }

        private void clearRelationshipIds(ByteArray array, long index) {
            array.set6ByteLong(index, idOffset(Direction.OUTGOING), EMPTY);
            array.set6ByteLong(index, idOffset(Direction.INCOMING), EMPTY);
            array.set6ByteLong(index, idOffset(Direction.BOTH), EMPTY);
        }

        private long findOrAllocateIndex(long index, int typeId) {
            long lastIndex = index;
            ByteArray array = this.array.at(index);
            while (index != EMPTY) {
                lastIndex = index;
                array = this.array.at(index);
                int candidateTypeId = getTypeId(array, index);
                if (candidateTypeId == typeId) {
                    return index;
                }
                index = getNext(array, index);
            }

            // No such found, create at the end
            long newIndex = allocate(typeId);
            array.set6ByteLong(lastIndex, NEXT_OFFSET, newIndex);
            return newIndex;
        }

        @Override
        public void close() {
            if (array != null) {
                array.close();
            }
        }

        @Override
        public void acceptMemoryStatsVisitor(MemoryStatsVisitor visitor) {
            nullSafeMemoryStatsVisitor(array, visitor);
        }

        public void clear() {
            nextFreeId.set(base);
        }

        public void clearRelationshipIds() {
            long highId = rebase(nextFreeId.get());
            for (long i = 0; i < highId; ) {
                ByteArray chunk = array.at(i);
                for (int j = 0; j < chunkSize && i < highId; j++, i++) {
                    clearRelationshipIds(chunk, i);
                }
            }
        }
    }

    @Override
    public String toString() {
        return array.toString();
    }

    @Override
    public void close() {
        if (array != null) {
            array.close();
        }
        if (relGroupCache != null) {
            relGroupCache.close();
        }
    }

    public long highNodeId() {
        return highNodeId;
    }

    @Override
    public void acceptMemoryStatsVisitor(MemoryStatsVisitor visitor) {
        nullSafeMemoryStatsVisitor(array, visitor);
        relGroupCache.acceptMemoryStatsVisitor(visitor);
    }

    static void nullSafeMemoryStatsVisitor(MemoryStatsVisitor.Visitable visitable, MemoryStatsVisitor visitor) {
        if (visitable != null) {
            visitable.acceptMemoryStatsVisitor(visitor);
        }
    }

    private static int changeMask(boolean dense) {
        return dense ? DENSE_NODE_CHANGED_MASK : SPARSE_NODE_CHANGED_MASK;
    }

    @FunctionalInterface
    public interface NodeChangeVisitor {
        void change(long nodeId, ByteArray array);
    }

    public void visitChangedNodes(NodeChangeVisitor visitor, int nodeTypes) {
        visitChangedNodes(visitor, nodeTypes, 0, highNodeId);
    }

    /**
     * Efficiently visits changed nodes, e.g. nodes that have had any relationship chain updated by
     * {@link #getAndPutRelationship(long, int, Direction, long, boolean)}.
     *
     * @param visitor {@link NodeChangeVisitor} which will be notified about all changes.
     * @param nodeTypes which types to visit (dense/sparse).
     */
    public void visitChangedNodes(NodeChangeVisitor visitor, int nodeTypes, long from, long to) {
        long denseMask = changeMask(true);
        long sparseMask = changeMask(false);
        byte denseChunkMask = chunkChangeMask(true);
        byte sparseChunkMask = chunkChangeMask(false);
        for (long nodeId = from; nodeId < to; ) {
            boolean chunkHasChanged = (NodeType.isDense(nodeTypes) && chunkHasChange(nodeId, denseChunkMask))
                    || (NodeType.isSparse(nodeTypes) && chunkHasChange(nodeId, sparseChunkMask));
            if (!chunkHasChanged) {
                nodeId = (chunkOf(nodeId) + 1L) * chunkSize;
                continue;
            }

            boolean nodeHasChanged = (NodeType.isDense(nodeTypes) && nodeIsChanged(array, nodeId, denseMask))
                    || (NodeType.isSparse(nodeTypes) && nodeIsChanged(array, nodeId, sparseMask));

            if (nodeHasChanged && NodeType.matchesDense(nodeTypes, isDense(array, nodeId))) {
                visitor.change(nodeId, array);
            }
            nodeId++;
        }
    }

    /**
     * Clears the high-level change marks.
     *
     * @param denseNodes {@code true} for clearing marked dense nodes, {@code false} for clearing marked sparse nodes.
     */
    private void clearChangedChunks(boolean denseNodes) {
        // Executed by a single thread, so no synchronized required
        byte chunkMask = chunkChangeMask(denseNodes);
        for (int i = 0; i < chunkChangedArray.length; i++) {
            chunkChangedArray[i] &= ~chunkMask;
        }
    }

    private boolean chunkHasChange(long nodeId, byte chunkMask) {
        int chunkId = chunkOf(nodeId);
        return (chunkChangedArray[chunkId] & chunkMask) != 0;
    }

    public long calculateMaxMemoryUsage(long numberOfRelationships) {
        return calculateMaxMemoryUsage(numberOfDenseNodes, numberOfRelationships);
    }

    public static long calculateMaxMemoryUsage(long numberOfDenseNodes, long numberOfRelationships) {
        long maxDenseNodesForThisType = min(numberOfDenseNodes, numberOfRelationships * 2 /*nodes/rel*/);
        return maxDenseNodesForThisType * NodeRelationshipCache.GROUP_ENTRY_SIZE;
    }

    public void countingCompleted() {
        numberOfDenseNodes = calculateNumberOfDenseNodes();
    }

    public long getNumberOfDenseNodes() {
        return numberOfDenseNodes;
    }

    public static MemoryStatsVisitor.Visitable memoryEstimation(long numberOfNodes) {
        return visitor -> visitor.offHeapUsage(ID_AND_COUNT_SIZE * numberOfNodes);
    }
}
