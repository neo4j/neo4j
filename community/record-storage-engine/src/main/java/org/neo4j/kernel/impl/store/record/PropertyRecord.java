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
package org.neo4j.kernel.impl.store.record;

import static java.lang.System.arraycopy;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_PREVIOUS_PROPERTY;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;
import static org.neo4j.memory.HeapEstimator.sizeOf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.string.Mask;

/**
 * PropertyRecord is a container for PropertyBlocks. PropertyRecords form
 * a double linked list and each one holds one or more PropertyBlocks that
 * are the actual property key/value pairs. Because PropertyBlocks are of
 * variable length, a full PropertyRecord can be holding just one
 * PropertyBlock.
 */
public class PropertyRecord extends AbstractBaseRecord {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(PropertyRecord.class);
    private static final int PAYLOAD_SIZE = PropertyType.getPayloadSizeLongs();
    public static final long INITIAL_SIZE = SHALLOW_SIZE
            + sizeOf(new long[PAYLOAD_SIZE])
            + shallowSizeOfObjectArray(PAYLOAD_SIZE)
            + PropertyBlock.HEAP_SIZE; // at least one block

    private static final byte TYPE_NODE = 1;
    private static final byte TYPE_REL = 2;
    private static final byte TYPE_SCHEMA_RULE = 3;

    private long nextProp;
    private long prevProp;
    // Holds the purely physical representation of the loaded properties in this record. This is so that
    // RecordPropertyCursor is able to use this raw data without the rather heavy and bloated data structures
    // of PropertyBlock and thereabouts. So when a property record is loaded only these blocks are read,
    // the construction of all PropertyBlock instances are loaded lazily when they are first needed, loaded
    // by ensureBlocksLoaded().
    // Modifications to a property record are still done on the PropertyBlock abstraction and so it's also
    // that data that gets written to the log and record when it's time to do so.
    private final long[] blocks = new long[PAYLOAD_SIZE];
    private int blocksCursor;

    // These MUST ONLY be populated if we're accessing PropertyBlocks. On just loading this record only the
    // next/prev and blocks should be filled.
    private final PropertyBlock[] blockRecords = new PropertyBlock[PAYLOAD_SIZE /*we can have at most these many*/];
    private int blockRecordsCursor;
    private boolean blocksLoaded;
    private long entityId;
    private byte entityType;
    private List<DynamicRecord> deletedRecords;

    public PropertyRecord(long id) {
        super(id);
    }

    public PropertyRecord(long id, PrimitiveRecord primitive) {
        super(id);
        primitive.setIdTo(this);
    }

    public PropertyRecord(PropertyRecord other) {
        this(other, false);
        this.blocksCursor = other.blocksCursor;
        this.blockRecordsCursor = other.blockRecordsCursor;
        this.blocksLoaded = other.blocksLoaded;
        arraycopy(other.blocks, 0, this.blocks, 0, other.blocks.length);

        for (int i = 0; i < blockRecordsCursor; i++) {
            this.blockRecords[i] = new PropertyBlock(other.blockRecords[i]);
        }
        if (other.deletedRecords != null) {
            this.deletedRecords = new ArrayList<>(other.deletedRecords.size());
            for (DynamicRecord deletedRecord : other.deletedRecords) {
                this.deletedRecords.add(new DynamicRecord(deletedRecord));
            }
        }
    }

    private PropertyRecord(PropertyRecord other, boolean ignored) {
        super(other);
        this.nextProp = other.nextProp;
        this.prevProp = other.prevProp;
        this.entityId = other.entityId;
        this.entityType = other.entityType;
    }

    public static PropertyRecord lightCopy(PropertyRecord other) {
        return new PropertyRecord(other, true);
    }

    public PropertyRecord initialize(boolean inUse, long prevProp, long nextProp) {
        super.initialize(inUse);
        this.prevProp = prevProp;
        this.nextProp = nextProp;
        this.deletedRecords = null;
        this.blockRecordsCursor = 0;
        this.blocksCursor = 0;
        this.blocksLoaded = false;
        return this;
    }

    @Override
    public void clear() {
        super.initialize(false);
        this.entityId = -1;
        this.entityType = 0;
        this.prevProp = NO_PREVIOUS_PROPERTY.intValue();
        this.nextProp = NO_NEXT_PROPERTY.intValue();
        this.deletedRecords = null;
        this.blockRecordsCursor = blocksCursor = 0;
        this.blocksLoaded = false;
    }

    public void setNodeId(long nodeId) {
        entityType = TYPE_NODE;
        entityId = nodeId;
    }

    public void setRelId(long relId) {
        entityType = TYPE_REL;
        entityId = relId;
    }

    public void setSchemaRuleId(long id) {
        entityType = TYPE_SCHEMA_RULE;
        entityId = id;
    }

    public void setEntity(PropertyRecord other) {
        entityType = other.entityType;
        entityId = other.entityId;
    }

    public boolean isNodeSet() {
        return entityType == TYPE_NODE;
    }

    public boolean isRelSet() {
        return entityType == TYPE_REL;
    }

    public boolean isSchemaSet() {
        return entityType == TYPE_SCHEMA_RULE;
    }

    public long getNodeId() {
        if (isNodeSet()) {
            return entityId;
        }
        return -1;
    }

    public long getRelId() {
        if (isRelSet()) {
            return entityId;
        }
        return -1;
    }

    public long getSchemaRuleId() {
        if (isSchemaSet()) {
            return entityId;
        }
        return -1;
    }

    public long getEntityId() {
        return entityId;
    }

    /**
     * Gets the sum of the sizes of the blocks in this record, in bytes.
     */
    public int size() {
        ensureBlocksLoaded();
        int result = 0;
        for (int i = 0; i < blockRecordsCursor; i++) {
            result += blockRecords[i].getSize();
        }
        return result;
    }

    public int numberOfProperties() {
        ensureBlocksLoaded();
        return blockRecordsCursor;
    }

    public PropertyBlock[] propertyBlocksArray() {
        return blockRecords;
    }

    public Iterable<PropertyBlock> propertyBlocks() {
        return this::iterator;
    }

    private Iterator<PropertyBlock> iterator() {
        ensureBlocksLoaded();
        return new Iterator<>() {
            // state for the Iterator aspect of this class.
            private int blockRecordsIteratorCursor;

            @Override
            public boolean hasNext() {
                return blockRecordsIteratorCursor < blockRecordsCursor;
            }

            @Override
            public PropertyBlock next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return blockRecords[blockRecordsIteratorCursor++];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("This iterator does not support removal");
            }
        };
    }

    public List<DynamicRecord> getDeletedRecords() {
        return deletedRecords != null ? deletedRecords : Collections.emptyList();
    }

    public void addDeletedRecord(DynamicRecord record) {
        assert !record.inUse();
        if (deletedRecords == null) {
            deletedRecords = new ArrayList<>(1);
        }
        deletedRecords.add(record);
    }

    public void addPropertyBlock(PropertyBlock block) {
        ensureBlocksLoaded();
        assert hasSpaceFor(block)
                : "Exceeded capacity of property record " + this
                        + ". My current size is reported as " + size() + "The added block was " + block
                        + " (note that size is "
                        + block.getSize() + ")";

        blockRecords[blockRecordsCursor++] = block;
    }

    public boolean hasSpaceFor(PropertyBlock block) {
        return size() + block.getSize() <= PropertyType.getPayloadSize();
    }

    /**
     * Reads blocks[] and constructs {@link PropertyBlock} instances from them, making that abstraction
     * available to the outside. Done the first time any PropertyBlock is needed or manipulated.
     */
    public void ensureBlocksLoaded() {
        if (!blocksLoaded) {
            assert blockRecordsCursor == 0;
            // We haven't loaded the blocks yet, please do so now
            int index = 0;
            while (index < blocksCursor) {
                PropertyType type = PropertyType.getPropertyTypeOrThrow(blocks[index]);
                PropertyBlock block = new PropertyBlock();
                int length = type.calculateNumberOfBlocksUsed(blocks[index]);
                block.setValueBlocks(Arrays.copyOfRange(blocks, index, index + length));
                blockRecords[blockRecordsCursor++] = block;
                index += length;
            }
            blocksLoaded = true;
        }
    }

    public PropertyBlock getPropertyBlock(int keyIndex) {
        ensureBlocksLoaded();
        for (int i = 0; i < blockRecordsCursor; i++) {
            PropertyBlock block = blockRecords[i];
            if (block.getKeyIndexId() == keyIndex) {
                return block;
            }
        }
        return null;
    }

    public PropertyBlock removePropertyBlock(int keyIndex) {
        ensureBlocksLoaded();
        for (int i = 0; i < blockRecordsCursor; i++) {
            if (blockRecords[i].getKeyIndexId() == keyIndex) {
                PropertyBlock block = blockRecords[i];
                if (--blockRecordsCursor > i) {
                    blockRecords[i] = blockRecords[blockRecordsCursor];
                }
                return block;
            }
        }
        return null;
    }

    public void clearPropertyBlocks() {
        blockRecordsCursor = 0;
    }

    public long getNextProp() {
        return nextProp;
    }

    public void setNextProp(long nextProp) {
        this.nextProp = nextProp;
    }

    @Override
    public String toString(Mask mask) {
        StringBuilder buf = new StringBuilder();
        buf.append("Property[")
                .append(getId())
                .append(",used=")
                .append(inUse())
                .append(",prev=")
                .append(prevProp)
                .append(",next=")
                .append(nextProp)
                .append(",created=")
                .append(isCreated())
                .append(",");

        switch (entityType) {
            case TYPE_NODE -> buf.append("node");
            case TYPE_REL -> buf.append("rel");
            case TYPE_SCHEMA_RULE -> buf.append("schema");
            default -> buf.append("unknownType(").append(entityType).append(")");
        }

        buf.append("=").append(entityId);

        if (blocksLoaded) {
            for (int i = 0; i < blockRecordsCursor; i++) {
                buf.append(',').append(blockRecords[i].toString(mask));
            }
        } else {
            buf.append(", (blocks not loaded)");
        }

        if (deletedRecords != null) {
            for (DynamicRecord dyn : deletedRecords) {
                buf.append(", del:").append(dyn);
            }
        }

        buf.append(']');
        return buf.toString();
    }

    public void setChanged(PrimitiveRecord primitive) {
        primitive.setIdTo(this);
    }

    public long getPrevProp() {
        return prevProp;
    }

    public void setPrevProp(long prev) {
        prevProp = prev;
    }

    public long[] getBlocks() {
        return blocks;
    }

    public void addLoadedBlock(long block) {
        assert blocksCursor + 1 <= blocks.length : "Capacity of " + blocks.length + " exceeded";
        blocks[blocksCursor++] = block;
    }

    public int getBlockCapacity() {
        return blocks.length;
    }

    public int getNumberOfBlocks() {
        return blocksCursor;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), nextProp, prevProp, Arrays.hashCode(blocks), entityId, entityType);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        PropertyRecord other = (PropertyRecord) obj;
        return nextProp == other.nextProp
                && prevProp == other.prevProp
                && Arrays.equals(blocks, 0, blocksCursor, other.blocks, 0, other.blocksCursor)
                && entityId == other.entityId
                && entityType == other.entityType;
    }
}
