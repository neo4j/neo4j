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
package org.neo4j.kernel.impl.store;

import static org.neo4j.collection.trackable.HeapTrackingCollections.newArrayList;
import static org.neo4j.internal.recordstorage.InconsistentDataReadException.CYCLE_DETECTION_THRESHOLD;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_ARRAY_STORE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_STRING_STORE_CURSOR;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.kernel.impl.store.NoStoreHeaderFormat.NO_STORE_HEADER_FORMAT;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.InconsistentDataReadException;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.internal.recordstorage.RecordPropertyCursor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatSettings;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.string.UTF8;
import org.neo4j.util.BitBuffer;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.utils.TemporalValueWriterAdapter;

/**
 * Implementation of the property store. This implementation has two dynamic
 * stores. One used to store keys and another for string property values.
 * Primitives are directly stored in the PropertyStore using this format:
 * <pre>
 *  0: high bits  ( 1 byte)
 *  1: next       ( 4 bytes)    where new property records are added
 *  5: prev       ( 4 bytes)    points to more PropertyRecords in this chain
 *  9: payload    (32 bytes - 4 x 8 byte blocks)
 * </pre>
 * <h2>high bits</h2>
 * <pre>
 * [    ,xxxx] high(next)
 * [xxxx,    ] high(prev)
 * </pre>
 * <h2>block structure</h2>
 * <pre>
 * [][][][] [    ,xxxx] [    ,    ] [    ,    ] [    ,    ] type (0x0000_0000_0F00_0000)
 * [][][][] [    ,    ] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] key  (0x0000_0000_00FF_FFFF)
 * </pre>
 * <h2>property types</h2>
 * <pre>
 *  1: BOOL
 *  2: BYTE
 *  3: SHORT
 *  4: CHAR
 *  5: INT
 *  6: LONG
 *  7: FLOAT
 *  8: DOUBLE
 *  9: STRING REFERENCE
 * 10: ARRAY  REFERENCE
 * 11: SHORT STRING
 * 12: SHORT ARRAY
 * 13: GEOMETRY
 * 14: TEMPORAL
 * </pre>
 * <h2>value formats</h2>
 * <pre>
 * BOOL:      [    ,    ] [    ,    ] [    ,    ] [    ,    ] [   x,type][K][K][K]           (0x0000_0000_1000_0000)
 * BYTE:      [    ,    ] [    ,    ] [    ,    ] [    ,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0000_000F_F000_0000)
 * SHORT:     [    ,    ] [    ,    ] [    ,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0000_0FFF_F000_0000)
 * CHAR:      [    ,    ] [    ,    ] [    ,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0000_0FFF_F000_0000)
 * INT:       [    ,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0FFF_FFFF_F000_0000)
 * LONG:      [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxx1,type][K][K][K] inline>>29(0xFFFF_FFFF_E000_0000)
 * LONG:      [    ,    ] [    ,    ] [    ,    ] [    ,    ] [   0,type][K][K][K] value in next long block
 * FLOAT:     [    ,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0FFF_FFFF_F000_0000)
 * DOUBLE:    [    ,    ] [    ,    ] [    ,    ] [    ,    ] [    ,type][K][K][K] value in next long block
 * REFERENCE: [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0xFFFF_FFFF_F000_0000)
 * SHORT STR: [    ,    ] [    ,    ] [    ,    ] [    ,   x] [xxxx,type][K][K][K] encoding  (0x0000_0001_F000_0000)
 *            [    ,    ] [    ,    ] [    ,    ] [ xxx,xxx ] [    ,type][K][K][K] length    (0x0000_007E_0000_0000)
 *            [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [x   ,    ] payload(+ maybe in next block) (0xFFFF_FF80_0000_0000)
 *                                                            bits are densely packed, bytes torn across blocks
 * SHORT ARR: [    ,    ] [    ,    ] [    ,    ] [    ,    ] [xxxx,type][K][K][K] data type (0x0000_0000_F000_0000)
 *            [    ,    ] [    ,    ] [    ,    ] [  xx,xxxx] [    ,type][K][K][K] length    (0x0000_003F_0000_0000)
 *            [    ,    ] [    ,    ] [    ,xxxx] [xx  ,    ] [    ,type][K][K][K] bits/item (0x0000_003F_0000_0000)
 *                                                                                 0 means 64, other values "normal"
 *            [xxxx,xxxx] [xxxx,xxxx] [xxxx,    ] [    ,    ] payload(+ maybe in next block) (0xFFFF_FF00_0000_0000)
 *                                                            bits are densely packed, bytes torn across blocks
 * POINT:     [    ,    ] [    ,    ] [    ,    ] [    ,    ] [ 01 ,type][K][K][K] geometry subtype
 *            [    ,    ] [    ,    ] [    ,    ] [    ,xxxx] [    ,type][K][K][K] dimension
 *            [    ,    ] [    ,    ] [    ,    ] [xxxx,    ] [    ,type][K][K][K] CRSTable
 *            [    ,    ] [xxxx,xxxx] [xxxx,xxxx] [    ,    ] [    ,type][K][K][K] CRS code
 *            [    ,   x] [    ,    ] [    ,    ] [    ,    ] [    ,type][K][K][K] Precision flag: 0=double, 1=float
 *            values in next dimension long blocks
 * DATE:      [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxx1] [ 01 ,type][K][K][K] epochDay
 * DATE:      [    ,    ] [    ,    ] [    ,    ] [    ,   0] [ 01 ,type][K][K][K] epochDay in next long block
 * LOCALTIME: [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxx1] [ 02 ,type][K][K][K] nanoOfDay
 * LOCALTIME: [    ,    ] [    ,    ] [    ,    ] [    ,   0] [ 02 ,type][K][K][K] nanoOfDay in next long block
 * LOCALDTIME:[xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [ 03 ,type][K][K][K] nanoOfSecond
 *            epochSecond in next long block
 * TIME:      [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [ 04 ,type][K][K][K] secondOffset (=ZoneOffset)
 *            nanoOfDay in next long block
 * DATETIME:  [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxx1] [ 05 ,type][K][K][K] nanoOfSecond
 *            epochSecond in next long block
 *            secondOffset in next long block
 * DATETIME:  [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxx0] [ 05 ,type][K][K][K] nanoOfSecond
 *            epochSecond in next long block
 *            timeZone number in next long block
 * DURATION:  [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [ 06 ,type][K][K][K] nanoOfSecond
 *            months in next long block
 *            days in next long block
 *            seconds in next long block
 * </pre>
 */
public class PropertyStore extends CommonAbstractStore<PropertyRecord, NoStoreHeader> {
    public static final String TYPE_DESCRIPTOR = "PropertyStore";

    private final DynamicStringStore stringStore;
    private final PropertyKeyTokenStore propertyKeyTokenStore;
    private final DynamicArrayStore arrayStore;

    public PropertyStore(
            FileSystemAbstraction fileSystem,
            Path path,
            Path idFile,
            Config configuration,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            InternalLogProvider logProvider,
            DynamicStringStore stringPropertyStore,
            PropertyKeyTokenStore propertyKeyTokenStore,
            DynamicArrayStore arrayPropertyStore,
            RecordFormats recordFormats,
            boolean readOnly,
            String databaseName,
            ImmutableSet<OpenOption> openOptions) {
        super(
                fileSystem,
                path,
                idFile,
                configuration,
                RecordIdType.PROPERTY,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                logProvider,
                TYPE_DESCRIPTOR,
                recordFormats.property(),
                NO_STORE_HEADER_FORMAT,
                readOnly,
                databaseName,
                openOptions);
        this.stringStore = stringPropertyStore;
        this.propertyKeyTokenStore = propertyKeyTokenStore;
        this.arrayStore = arrayPropertyStore;
    }

    public DynamicStringStore getStringStore() {
        return stringStore;
    }

    public DynamicArrayStore getArrayStore() {
        return arrayStore;
    }

    public PropertyKeyTokenStore getPropertyKeyTokenStore() {
        return propertyKeyTokenStore;
    }

    @Override
    public void updateRecord(
            PropertyRecord record,
            IdUpdateListener idUpdateListener,
            PageCursor cursor,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        updatePropertyBlocks(record, idUpdateListener, cursorContext, storeCursors);
        super.updateRecord(record, idUpdateListener, cursor, cursorContext, storeCursors);
    }

    private void updatePropertyBlocks(
            PropertyRecord record,
            IdUpdateListener idUpdateListener,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        if (record.inUse()) {
            // Go through the blocks
            for (PropertyBlock block : record.propertyBlocks()) {
                /*
                 * For each block we need to update its dynamic record chain if
                 * it is just created. Deleted dynamic records are in the property
                 * record and dynamic records are never modified. Also, they are
                 * assigned as a whole, so just checking the first should be enough.
                 */
                if (!block.isLight() && block.getValueRecords().get(0).isCreated()) {
                    updateDynamicRecords(block.getValueRecords(), idUpdateListener, cursorContext, storeCursors);
                }
            }
        }
        updateDynamicRecords(record.getDeletedRecords(), idUpdateListener, cursorContext, storeCursors);
    }

    private void updateDynamicRecords(
            List<DynamicRecord> records,
            IdUpdateListener idUpdateListener,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        PageCursor stringCursor = null;
        PageCursor arrayCursor = null;
        try {
            for (DynamicRecord valueRecord : records) {
                PropertyType recordType = valueRecord.getType();
                if (recordType == PropertyType.STRING) {
                    if (stringCursor == null) {
                        stringCursor = storeCursors.writeCursor(DYNAMIC_STRING_STORE_CURSOR);
                    }
                    stringStore.updateRecord(valueRecord, idUpdateListener, stringCursor, cursorContext, storeCursors);
                } else if (recordType == PropertyType.ARRAY) {
                    if (arrayCursor == null) {
                        arrayCursor = storeCursors.writeCursor(DYNAMIC_ARRAY_STORE_CURSOR);
                    }
                    arrayStore.updateRecord(valueRecord, idUpdateListener, arrayCursor, cursorContext, storeCursors);
                } else {
                    throw new InvalidRecordException("Unknown dynamic record" + valueRecord);
                }
            }
        } finally {
            closeAllUnchecked(stringCursor, arrayCursor);
        }
    }

    @Override
    public void ensureHeavy(PropertyRecord record, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        long heapBefore = memoryTracker.estimatedHeapMemory();
        for (PropertyBlock block : record.propertyBlocks()) {
            ensureHeavy(block, storeCursors, memoryTracker);
        }
        record.heavyHeapSize = memoryTracker.estimatedHeapMemory() - heapBefore;
    }

    public void ensureHeavy(PropertyBlock block, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        if (!block.isLight()) {
            return;
        }

        PropertyType type = block.getType();
        var dynamicStore = dynamicStoreForValueType(type);
        if (dynamicStore != null) {
            var cursorForType = dynamicStoreCursorForType(storeCursors, type);
            List<DynamicRecord> dynamicRecords =
                    dynamicStore.getRecords(block.getSingleValueLong(), NORMAL, false, cursorForType, memoryTracker);
            for (DynamicRecord dynamicRecord : dynamicRecords) {
                dynamicRecord.setType(type.intValue());
            }
            block.setValueRecords(dynamicRecords);
        }
    }

    private PageCursor dynamicStoreCursorForType(StoreCursors storeCursors, PropertyType type) {
        return switch (type) {
            case ARRAY -> storeCursors.readCursor(DYNAMIC_ARRAY_STORE_CURSOR);
            case STRING -> storeCursors.readCursor(DYNAMIC_STRING_STORE_CURSOR);
            default -> throw new IllegalArgumentException("Unsupported type of dynamic property " + type);
        };
    }

    private AbstractDynamicStore dynamicStoreForValueType(PropertyType type) {
        return switch (type) {
            case ARRAY -> arrayStore;
            case STRING -> stringStore;
            default -> null;
        };
    }

    public Value getValue(PropertyBlock propertyBlock, StoreCursors cursors, MemoryTracker memoryTracker) {
        return propertyBlock.getType().value(propertyBlock, this, cursors, memoryTracker);
    }

    private static void allocateStringRecords(
            Collection<DynamicRecord> target,
            byte[] chars,
            DynamicRecordAllocator allocator,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        AbstractDynamicStore.allocateRecordsFromBytes(target, chars, allocator, cursorContext, memoryTracker);
    }

    private static void allocateArrayRecords(
            Collection<DynamicRecord> target,
            Object array,
            DynamicRecordAllocator allocator,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        DynamicArrayStore.allocateRecords(target, array, allocator, cursorContext, memoryTracker);
    }

    public static void encodeValue(
            PropertyBlock block,
            int keyId,
            Value value,
            DynamicRecordAllocator stringAllocator,
            DynamicRecordAllocator arrayAllocator,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        if (value instanceof ArrayValue) {
            Object asObject = value.asObject();

            // Try short array first, i.e. inlined in the property block
            if (ShortArray.encode(keyId, asObject, block, PropertyType.getPayloadSize())) {
                return;
            }

            // Fall back to dynamic array store
            List<DynamicRecord> arrayRecords = newArrayList(memoryTracker);
            allocateArrayRecords(arrayRecords, asObject, arrayAllocator, cursorContext, memoryTracker);
            setSingleBlockValue(
                    block,
                    keyId,
                    PropertyType.ARRAY,
                    Iterables.first(arrayRecords).getId());
            for (DynamicRecord valueRecord : arrayRecords) {
                valueRecord.setType(PropertyType.ARRAY.intValue());
            }
            block.setValueRecords(arrayRecords);
        } else {
            value.writeTo(new PropertyBlockValueWriter(block, keyId, stringAllocator, cursorContext, memoryTracker));
        }
    }

    public PageCursor openStringPageCursor(long reference, CursorContext cursorContext) {
        return stringStore.openPageCursorForReading(reference, cursorContext);
    }

    public PageCursor openArrayPageCursor(long reference, CursorContext cursorContext) {
        return arrayStore.openPageCursorForReading(reference, cursorContext);
    }

    public void loadString(
            long reference,
            RecordPropertyCursor propertyCursor,
            PageCursor page,
            RecordLoad loadMode,
            MemoryTracker memoryTracker) {
        readDynamic(stringStore, reference, propertyCursor, page, loadMode, memoryTracker);
    }

    public void loadArray(
            long reference,
            RecordPropertyCursor propertyCursor,
            PageCursor page,
            RecordLoad loadMode,
            MemoryTracker memoryTracker) {
        readDynamic(arrayStore, reference, propertyCursor, page, loadMode, memoryTracker);
    }

    private static void readDynamic(
            AbstractDynamicStore store,
            long reference,
            RecordPropertyCursor propertyCursor,
            PageCursor page,
            RecordLoad loadMode,
            MemoryTracker memoryTracker) {
        var buffer = propertyCursor.getOrCreateClearBuffer();
        DynamicRecord record = store.newRecord();
        // Only instantiated if number of dynamic records reaches a certain threshold, at which point it's instantiated
        // and the set of dynamic IDs is observed for cycles to allow aborting the read if cycle detected.
        MutableLongSet seenDynamicIds = null;
        long firstReference = reference;
        int count = 0;
        do {
            // We need to load forcefully here since otherwise we can have inconsistent reads
            // for properties across blocks, see org.neo4j.graphdb.ConsistentPropertyReadsIT
            store.getRecordByCursor(reference, record, loadMode, page, memoryTracker);
            reference = record.getNextBlock();
            byte[] data = record.getData();
            if (buffer.remaining() < data.length) {
                buffer = propertyCursor.growBuffer(data.length);
            }
            buffer.put(data, 0, data.length);
            // release memory used by dynamic record to keep its payload
            memoryTracker.releaseHeap(data.length);

            // An arbitrarily high threshold so that it's very likely only hit on actual chain cycle
            if (++count >= CYCLE_DETECTION_THRESHOLD) {
                if (seenDynamicIds == null) {
                    seenDynamicIds = LongSets.mutable.empty();
                }
                if (!seenDynamicIds.add(reference)) {
                    throw new InconsistentDataReadException(
                            "Chain cycle detected in dynamic property value store %s starting at id:%d",
                            store, firstReference);
                }
            }
        } while (reference != LongReference.NULL);
    }

    private static class PropertyBlockValueWriter extends TemporalValueWriterAdapter<IllegalArgumentException> {
        private final PropertyBlock block;
        private final int keyId;
        private final DynamicRecordAllocator stringAllocator;
        private final CursorContext cursorContext;
        private final MemoryTracker memoryTracker;

        PropertyBlockValueWriter(
                PropertyBlock block,
                int keyId,
                DynamicRecordAllocator stringAllocator,
                CursorContext cursorContext,
                MemoryTracker memoryTracker) {
            this.block = block;
            this.keyId = keyId;
            this.stringAllocator = stringAllocator;
            this.cursorContext = cursorContext;
            this.memoryTracker = memoryTracker;
        }

        @Override
        public void writeNull() throws IllegalArgumentException {
            throw new IllegalArgumentException("Cannot write null values to the property store");
        }

        @Override
        public void writeBoolean(boolean value) throws IllegalArgumentException {
            setSingleBlockValue(block, keyId, PropertyType.BOOL, value ? 1L : 0L);
        }

        @Override
        public void writeInteger(byte value) throws IllegalArgumentException {
            setSingleBlockValue(block, keyId, PropertyType.BYTE, value);
        }

        @Override
        public void writeInteger(short value) throws IllegalArgumentException {
            setSingleBlockValue(block, keyId, PropertyType.SHORT, value);
        }

        @Override
        public void writeInteger(int value) throws IllegalArgumentException {
            setSingleBlockValue(block, keyId, PropertyType.INT, value);
        }

        @Override
        public void writeInteger(long value) throws IllegalArgumentException {
            long keyAndType = keyId
                    | (((long) PropertyType.LONG.intValue()) << StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS);
            if (ShortArray.LONG.getRequiredBits(value)
                    <= 35) { // We only need one block for this value, special layout compared to, say, an integer
                block.setSingleBlock(keyAndType | (1L << 28) | (value << 29));
            } else { // We need two blocks for this value
                block.setValueBlocks(new long[] {keyAndType, value});
            }
        }

        @Override
        public void writeFloatingPoint(float value) throws IllegalArgumentException {
            setSingleBlockValue(block, keyId, PropertyType.FLOAT, Float.floatToRawIntBits(value));
        }

        @Override
        public void writeFloatingPoint(double value) throws IllegalArgumentException {
            block.setValueBlocks(new long[] {
                keyId
                        | (((long) PropertyType.DOUBLE.intValue())
                                << StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS),
                Double.doubleToRawLongBits(value)
            });
        }

        @Override
        public void writeString(String value) throws IllegalArgumentException {
            // Try short string first, i.e. inlined in the property block
            if (LongerShortString.encode(keyId, value, block, PropertyType.getPayloadSize())) {
                return;
            }

            // Fall back to dynamic string store
            byte[] encodedString = encodeString(value);
            List<DynamicRecord> valueRecords =
                    newArrayList(encodedString.length / stringAllocator.getRecordDataSize() + 1, memoryTracker);
            allocateStringRecords(valueRecords, encodedString, stringAllocator, cursorContext, memoryTracker);
            setSingleBlockValue(
                    block,
                    keyId,
                    PropertyType.STRING,
                    Iterables.first(valueRecords).getId());
            for (DynamicRecord valueRecord : valueRecords) {
                valueRecord.setType(PropertyType.STRING.intValue());
            }
            block.setValueRecords(valueRecords);
        }

        @Override
        public void writeString(char value) throws IllegalArgumentException {
            setSingleBlockValue(block, keyId, PropertyType.CHAR, value);
        }

        @Override
        public void beginArray(int size, ArrayType arrayType) throws IllegalArgumentException {
            throw new IllegalArgumentException("Cannot persist arrays to property store using ValueWriter");
        }

        @Override
        public void endArray() throws IllegalArgumentException {
            throw new IllegalArgumentException("Cannot persist arrays to property store using ValueWriter");
        }

        @Override
        public void writeByteArray(byte[] value) throws IllegalArgumentException {
            throw new IllegalArgumentException("Cannot persist arrays to property store using ValueWriter");
        }

        @Override
        public void writePoint(CoordinateReferenceSystem crs, double[] coordinate) throws IllegalArgumentException {
            block.setValueBlocks(GeometryType.encodePoint(keyId, crs, coordinate));
        }

        @Override
        public void writeDuration(long months, long days, long seconds, int nanos) throws IllegalArgumentException {
            block.setValueBlocks(TemporalType.encodeDuration(keyId, months, days, seconds, nanos));
        }

        @Override
        public void writeDate(long epochDay) throws IllegalArgumentException {
            block.setValueBlocks(TemporalType.encodeDate(keyId, epochDay));
        }

        @Override
        public void writeLocalTime(long nanoOfDay) throws IllegalArgumentException {
            block.setValueBlocks(TemporalType.encodeLocalTime(keyId, nanoOfDay));
        }

        @Override
        public void writeTime(long nanosOfDayUTC, int offsetSeconds) throws IllegalArgumentException {
            block.setValueBlocks(TemporalType.encodeTime(keyId, nanosOfDayUTC, offsetSeconds));
        }

        @Override
        public void writeLocalDateTime(long epochSecond, int nano) throws IllegalArgumentException {
            block.setValueBlocks(TemporalType.encodeLocalDateTime(keyId, epochSecond, nano));
        }

        @Override
        public void writeDateTime(long epochSecondUTC, int nano, int offsetSeconds) throws IllegalArgumentException {
            block.setValueBlocks(TemporalType.encodeDateTime(keyId, epochSecondUTC, nano, offsetSeconds));
        }

        @Override
        public void writeDateTime(long epochSecondUTC, int nano, String zoneId) throws IllegalArgumentException {
            block.setValueBlocks(TemporalType.encodeDateTime(keyId, epochSecondUTC, nano, zoneId));
        }
    }

    public static void setSingleBlockValue(PropertyBlock block, int keyId, PropertyType type, long longValue) {
        block.setSingleBlock(singleBlockLongValue(keyId, type, longValue));
    }

    public static long singleBlockLongValue(int keyId, PropertyType type, long longValue) {
        return keyId
                | (((long) type.intValue()) << StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS)
                | (longValue << 28);
    }

    public static byte[] encodeString(String string) {
        return UTF8.encode(string);
    }

    public static String decodeString(byte[] byteArray) {
        return UTF8.decode(byteArray);
    }

    TextValue getTextValueFor(PropertyBlock propertyBlock, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        ensureHeavy(propertyBlock, storeCursors, memoryTracker);
        return getTextValueFor(propertyBlock.getValueRecords(), storeCursors, memoryTracker);
    }

    public TextValue getTextValueFor(
            Collection<DynamicRecord> dynamicRecords, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        AbstractDynamicStore.HeavyRecordData source =
                stringStore.readFullByteArray(dynamicRecords, PropertyType.STRING, storeCursors, memoryTracker);
        // A string doesn't have a header in the data array
        return Values.utf8Value(source.data());
    }

    Value getArrayFor(PropertyBlock propertyBlock, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        ensureHeavy(propertyBlock, storeCursors, memoryTracker);
        return getArrayFor(propertyBlock.getValueRecords(), storeCursors, memoryTracker);
    }

    public Value getArrayFor(
            Collection<DynamicRecord> records, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return arrayStore.getArrayFor(records, storeCursors, memoryTracker);
    }

    @Override
    public String toString() {
        return super.toString() + "[blocksPerRecord:" + PropertyType.getPayloadSizeLongs() + "]";
    }

    @Override
    public PropertyRecord newRecord() {
        return new PropertyRecord(-1);
    }

    /**
     * @return a calculator of property value sizes. The returned instance is designed to be used multiple times by a single thread only.
     */
    public PropertyValueRecordSizeCalculator newValueEncodedSizeCalculator() {
        return new PropertyValueRecordSizeCalculator(this);
    }

    public ArrayValue readArrayFromBuffer(ByteBuffer buffer) {
        if (buffer.limit() <= 0) {
            throw new IllegalStateException("Given buffer is empty");
        }

        byte typeId = buffer.get();
        if (typeId == PropertyType.STRING.intValue()) {
            int arrayLength = buffer.getInt();
            String[] result = new String[arrayLength];

            for (int i = 0; i < arrayLength; i++) {
                int byteLength = buffer.getInt();
                result[i] = UTF8.decode(buffer.array(), buffer.position(), byteLength);
                buffer.position(buffer.position() + byteLength);
            }
            return Values.stringArray(result);
        } else if (typeId == PropertyType.GEOMETRY.intValue()) {
            GeometryType.GeometryHeader header = GeometryType.GeometryHeader.fromArrayHeaderByteBuffer(buffer);
            byte[] byteArray = new byte[buffer.limit() - buffer.position()];
            buffer.get(byteArray);
            return GeometryType.decodeGeometryArray(header, byteArray);
        } else if (typeId == PropertyType.TEMPORAL.intValue()) {
            TemporalType.TemporalHeader header = TemporalType.TemporalHeader.fromArrayHeaderByteBuffer(buffer);
            byte[] byteArray = new byte[buffer.limit() - buffer.position()];
            buffer.get(byteArray);
            return TemporalType.decodeTemporalArray(header, byteArray);
        } else {
            ShortArray type = ShortArray.typeOf(typeId);
            int bitsUsedInLastByte = buffer.get();
            int requiredBits = buffer.get();
            if (requiredBits == 0) {
                return type.createEmptyArray();
            }
            if (type == ShortArray.BYTE
                    && requiredBits == Byte.SIZE) { // Optimization for byte arrays (probably large ones)
                byte[] byteArray = new byte[buffer.limit() - buffer.position()];
                buffer.get(byteArray);
                return Values.byteArray(byteArray);
            } else { // Fallback to the generic approach, which is a slower
                BitBuffer bits = BitBuffer.bitsFromBytes(buffer.array(), buffer.position());
                int length = ((buffer.limit() - buffer.position()) * 8 - (8 - bitsUsedInLastByte)) / requiredBits;
                return type.createArray(length, bits, requiredBits);
            }
        }
    }
}
