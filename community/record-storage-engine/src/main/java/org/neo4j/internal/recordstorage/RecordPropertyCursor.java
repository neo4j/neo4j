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
package org.neo4j.internal.recordstorage;

import static org.neo4j.internal.recordstorage.InconsistentDataReadException.CYCLE_DETECTION_THRESHOLD;
import static org.neo4j.kernel.impl.store.record.RecordLoad.ALWAYS;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.common.EntityType;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.GeometryType;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.LongerShortString;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.ShortArray;
import org.neo4j.kernel.impl.store.TemporalType;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoadOverride;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.string.Mask;
import org.neo4j.util.BitBuffer;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.ShortValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

public class RecordPropertyCursor extends PropertyRecord implements StoragePropertyCursor {
    private static final int MAX_BYTES_IN_SHORT_STRING_OR_SHORT_ARRAY = 32;
    private static final int INITIAL_POSITION = -1;
    public static final int DEFAULT_PROPERTY_BUFFER_CAPACITY = 512;

    private final PropertyStore propertyStore;
    private final CursorContext cursorContext;
    private final StoreCursors storeCursors;
    private final MemoryTracker memoryTracker;
    private long next = NO_ID;
    private int block;
    private ScopedBuffer scopedBuffer;
    private ByteBuffer buffer;
    private PageCursor page;
    private PageCursor stringPage;
    private PageCursor arrayPage;
    private boolean open;
    private int numSeenPropertyRecords;
    private long first;
    private long ownerReference;
    // Only instantiated used when crossing a certain threshold of the traversed property chain
    private MutableLongSet cycleDetection;
    private EntityType ownerEntityType;
    private RecordLoadOverride loadMode;
    private PropertySelection selection;
    private int propertyKey;

    RecordPropertyCursor(
            PropertyStore propertyStore,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        super(NO_ID);
        this.propertyStore = propertyStore;
        this.cursorContext = cursorContext;
        this.storeCursors = storeCursors;
        this.memoryTracker = memoryTracker;
        loadMode = RecordLoadOverride.none();
    }

    @Override
    public void initNodeProperties(Reference reference, PropertySelection selection, long ownerReference) {
        init(reference, selection, ownerReference, EntityType.NODE);
    }

    @Override
    public void initRelationshipProperties(Reference reference, PropertySelection selection, long ownerReference) {
        init(reference, selection, ownerReference, EntityType.RELATIONSHIP);
    }

    /**
     * In this implementation property ids are unique among nodes AND relationships so they all init the same way
     * @param reference properties reference, actual property record id.
     * @param selection which properties to read.
     */
    private void init(
            Reference reference, PropertySelection selection, long ownerReference, EntityType ownerEntityType) {
        if (getId() != NO_ID) {
            clear();
        }

        // Set to high value to force a read
        long referenceId = ((LongReference) reference).id;
        this.block = Integer.MAX_VALUE;
        this.ownerReference = ownerReference;
        this.ownerEntityType = ownerEntityType;
        if (referenceId != NO_ID) {
            if (page == null) {
                page = storeCursors.readCursor(RecordCursorTypes.PROPERTY_CURSOR);
            }
        }

        // Store state
        this.next = referenceId;
        this.first = ((LongReference) reference).id;
        this.numSeenPropertyRecords = 0;
        this.cycleDetection = null;
        this.open = true;
        this.selection = selection;
    }

    @Override
    public boolean next() {
        while (true) {
            // Figure out number of blocks of record
            int numberOfBlocks = getNumberOfBlocks();
            while (block < numberOfBlocks) {
                // We have just read a record, so we are at the beginning
                if (block == INITIAL_POSITION) {
                    block = 0;
                } else {
                    // Figure out the type and how many blocks that are used
                    long current = currentBlock();
                    PropertyType type = PropertyType.getPropertyTypeOrNull(current);
                    if (type == null) {
                        break;
                    }
                    block += type.calculateNumberOfBlocksUsed(current);
                }
                // nothing left, need to read a new record
                if (block >= numberOfBlocks || type() == null) {
                    break;
                }

                propertyKey = PropertyBlock.keyIndexId(currentBlock());
                if (selection.test(propertyKey)) {
                    return true;
                }
            }

            if (next == NO_ID) {
                return false;
            }

            property(this, next, page);
            next = getNextProp();
            block = INITIAL_POSITION;

            if (++numSeenPropertyRecords >= CYCLE_DETECTION_THRESHOLD) {
                if (cycleDetection == null) {
                    cycleDetection = LongSets.mutable.empty();
                }
                if (!cycleDetection.add(next)) {
                    throw new InconsistentDataReadException(
                            "Aborting property reading due to detected chain cycle, starting at property record id:%d from owner %s:%d",
                            first, ownerEntityType, ownerReference);
                }
            }
        }
    }

    private long currentBlock() {
        return getBlocks()[block];
    }

    @Override
    public void reset() {
        if (open) {
            open = false;
            loadMode = RecordLoadOverride.none();
            clear();
            numSeenPropertyRecords = 0;
            block = Integer.MAX_VALUE;
            next = NO_ID;
            first = NO_ID;
            ownerReference = NO_ID;
            cycleDetection = null;
            selection = PropertySelection.NO_PROPERTIES;
        }
    }

    @Override
    public void setForceLoad() {
        this.loadMode = RecordLoadOverride.FORCE;
    }

    @Override
    public int propertyKey() {
        return propertyKey;
    }

    @Override
    public ValueGroup propertyType() {
        PropertyType type = type();
        if (type == null) {
            return ValueGroup.NO_VALUE;
        }
        return switch (type) {
            case BOOL -> ValueGroup.BOOLEAN;
            case BYTE, SHORT, INT, LONG, FLOAT, DOUBLE -> ValueGroup.NUMBER;
            case STRING, CHAR, SHORT_STRING -> ValueGroup.TEXT;
            case TEMPORAL, GEOMETRY, SHORT_ARRAY, ARRAY ->
            // value read is needed to get correct value group since type is not fine grained enough to match all
            // ValueGroups
            propertyValue().valueGroup();
        };
    }

    private PropertyType type() {
        return PropertyType.getPropertyTypeOrNull(currentBlock());
    }

    @Override
    public Value propertyValue() {
        try {
            return readValue();
        } catch (InvalidRecordException | InconsistentDataReadException e) {
            throw new InconsistentDataReadException(
                    e,
                    "Unable to read property value in record:%d, starting at property record id:%d from owner %s:%d",
                    getId(),
                    first,
                    ownerEntityType,
                    ownerReference);
        }
    }

    private Value readValue() {
        PropertyType type = type();
        if (type == null) {
            return NO_VALUE;
        }
        return switch (type) {
            case BOOL -> readBoolean();
            case BYTE -> readByte();
            case SHORT -> readShort();
            case INT -> readInt();
            case LONG -> readLong();
            case FLOAT -> readFloat();
            case DOUBLE -> readDouble();
            case CHAR -> readChar();
            case SHORT_STRING -> readShortString();
            case SHORT_ARRAY -> readShortArray();
            case STRING -> readLongString();
            case ARRAY -> readLongArray();
            case GEOMETRY -> geometryValue();
            case TEMPORAL -> temporalValue();
        };
    }

    private Value geometryValue() {
        return GeometryType.decode(getBlocks(), block);
    }

    private Value temporalValue() {
        return TemporalType.decode(getBlocks(), block);
    }

    private ArrayValue readLongArray() {
        long reference = PropertyBlock.fetchLong(currentBlock());
        if (arrayPage == null) {
            arrayPage = arrayPage(reference);
        }
        return array(this, reference, arrayPage);
    }

    private TextValue readLongString() {
        long reference = PropertyBlock.fetchLong(currentBlock());
        if (stringPage == null) {
            stringPage = stringPage(reference);
        }
        return string(this, reference, stringPage);
    }

    private Value readShortArray() {
        BitBuffer bits = BitBuffer.bits(MAX_BYTES_IN_SHORT_STRING_OR_SHORT_ARRAY);
        int blocksUsed = ShortArray.calculateNumberOfBlocksUsed(currentBlock());
        for (int i = 0; i < blocksUsed; i++) {
            bits.put(getBlocks()[block + i]);
        }
        return ShortArray.decode(bits);
    }

    private TextValue readShortString() {
        return LongerShortString.decode(getBlocks(), block);
    }

    private TextValue readChar() {
        return Values.charValue((char) PropertyBlock.fetchShort(currentBlock()));
    }

    private DoubleValue readDouble() {
        return Values.doubleValue(Double.longBitsToDouble(getBlocks()[block + 1]));
    }

    private FloatValue readFloat() {
        return Values.floatValue(Float.intBitsToFloat(PropertyBlock.fetchInt(currentBlock())));
    }

    private LongValue readLong() {
        if (PropertyBlock.valueIsInlined(currentBlock())) {
            return Values.longValue(PropertyBlock.fetchLong(currentBlock()) >>> 1);
        } else {
            return Values.longValue(getBlocks()[block + 1]);
        }
    }

    private IntValue readInt() {
        return Values.intValue(PropertyBlock.fetchInt(currentBlock()));
    }

    private ShortValue readShort() {
        return Values.shortValue(PropertyBlock.fetchShort(currentBlock()));
    }

    private ByteValue readByte() {
        return Values.byteValue(PropertyBlock.fetchByte(currentBlock()));
    }

    private BooleanValue readBoolean() {
        return Values.booleanValue(PropertyBlock.fetchByte(currentBlock()) == 1);
    }

    @Override
    public String toString(Mask mask) {
        if (!open) {
            return "RecordPropertyCursor[closed state]";
        } else {
            return "RecordPropertyCursor[id=" + getId() + ", open state with: block=" + block + ", next=" + next
                    + ", underlying record=" + super.toString(mask) + "]";
        }
    }

    @Override
    public void close() {
        page = null; // Cursor owned by StoreCursors cache so not closed here
        if (stringPage != null) {
            stringPage.close();
            stringPage = null;
        }
        if (arrayPage != null) {
            arrayPage.close();
            arrayPage = null;
        }
        if (scopedBuffer != null) {
            scopedBuffer.close();
            scopedBuffer = null;
            buffer = null;
        }
    }

    private PageCursor stringPage(long reference) {
        return propertyStore.openStringPageCursor(reference, cursorContext);
    }

    private PageCursor arrayPage(long reference) {
        return propertyStore.openArrayPageCursor(reference, cursorContext);
    }

    private void property(PropertyRecord record, long reference, PageCursor pageCursor) {
        // We need to load forcefully here since otherwise we can have inconsistent reads
        // for properties across blocks, see org.neo4j.graphdb.ConsistentPropertyReadsIT
        propertyStore.getRecordByCursor(reference, record, loadMode.orElse(ALWAYS), pageCursor);
    }

    private TextValue string(RecordPropertyCursor cursor, long reference, PageCursor page) {
        propertyStore.loadString(reference, cursor, page, loadMode.orElse(ALWAYS));
        buffer.flip();
        byte[] bytes = new byte[buffer.limit()];
        buffer.get(bytes);
        return Values.utf8Value(bytes);
    }

    private ArrayValue array(RecordPropertyCursor cursor, long reference, PageCursor page) {
        propertyStore.loadArray(reference, cursor, page, loadMode.orElse(ALWAYS));
        buffer.flip();
        return propertyStore.readArrayFromBuffer(buffer);
    }

    public void setScopedBuffer(ScopedBuffer scopedBuffer) {
        this.scopedBuffer = scopedBuffer;
        this.buffer = scopedBuffer.getBuffer();
    }

    public ByteBuffer getOrCreateClearBuffer() {
        if (buffer == null) {
            // byte order is important only for string arrays, always big-endian
            setScopedBuffer(
                    new HeapScopedBuffer(DEFAULT_PROPERTY_BUFFER_CAPACITY, ByteOrder.BIG_ENDIAN, memoryTracker));
        } else {
            buffer.clear();
        }
        return buffer;
    }

    public ByteBuffer growBuffer(int minAdditionalCapacity) {
        buffer.flip();
        int oldCapacity = buffer.capacity();
        int newCapacity = Math.max(oldCapacity, minAdditionalCapacity) + oldCapacity;

        var oldScopedBuffer = scopedBuffer;
        // byte order is important only for string arrays, always big-endian
        setScopedBuffer(new HeapScopedBuffer(newCapacity, ByteOrder.BIG_ENDIAN, memoryTracker));
        buffer.put(oldScopedBuffer.getBuffer());
        oldScopedBuffer.close();

        return buffer;
    }
}
