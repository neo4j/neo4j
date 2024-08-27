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

import static java.lang.System.arraycopy;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Collection;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.util.BitBuffer;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.CRSTable;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Dynamic store that stores arrays.
 *
 * Arrays are uniform collections of the same type. They can contain primitives, strings or Geometries.
 * <ul>
 *     <li>
 *         Primitive arrays are stored using a 3 byte header followed by a byte[] of bit-compacted data. The header defines the format of the byte[]:
 *         <ul>
 *             <li>Byte 0: The type of the primitive being stored. See {@link PropertyType}</li>
 *             <li>Byte 1: The number of bits used in the last byte</li>
 *             <li>Byte 2: The number of bits required for each element of the data array (after compaction)</li>
 *         </ul>
 *         The total number of elements can be calculated by combining the information about the individual element size
 *         (bits required - 3rd byte) with the length of the data specified in the DynamicRecordFormat.
 *     </li>
 *     <li>
 *         Arrays of strings are stored using a 5 byte header:
 *         <ul>
 *             <li>Byte 0: PropertyType.STRING</li>
 *             <li>Bytes 1 to 4: 32bit Int length of string array</li>
 *         </ul>
 *         This is followed by a byte[] composed of a 4 byte header containing the length of the byte[] representation of the string, and then those bytes.
 *     </li>
 *     <li>
 *         Arrays of Geometries starting with a 6 byte header:
 *         <ul>
 *             <li>Byte 0: PropertyType.GEOMETRY</li>
 *             <li>Byte 1: GeometryType, currently only POINT is supported</li>
 *             <li>Byte 2: The dimension of the geometry (currently only 2 or 3 dimensions are supported)</li>
 *             <li>Byte 3: Coordinate Reference System Table id: {@link CRSTable}</li>
 *             <li>Bytes 4-5: 16bit short Coordinate Reference System code: {@link CoordinateReferenceSystem}</li>
 *         </ul>
 *         The format of the body is specific to the type of Geometry being stored:
 *         <ul>
 *             <li>Points: Stored as double[] using the same format as primitive arrays above, starting with the 3 byte header (see above)</li>
 *         </ul>
 *     </li>
 * </ul>
 */
public class DynamicArrayStore extends AbstractDynamicStore {
    public static final int NUMBER_HEADER_SIZE = 3;
    public static final int STRING_HEADER_SIZE = 5;
    public static final int GEOMETRY_HEADER_SIZE = 6; // This should match contents of GeometryType.GeometryHeader
    public static final int TEMPORAL_HEADER_SIZE = 2;

    // store version, each store ends with this string (byte encoded)
    public static final String TYPE_DESCRIPTOR = "ArrayPropertyStore";

    public DynamicArrayStore(
            FileSystemAbstraction fileSystem,
            Path path,
            Path idFile,
            Config configuration,
            RecordIdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            InternalLogProvider logProvider,
            int dataSizeFromConfiguration,
            RecordFormats recordFormats,
            boolean readOnly,
            String databaseName,
            ImmutableSet<OpenOption> openOptions) {
        super(
                fileSystem,
                path,
                idFile,
                configuration,
                idType,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                logProvider,
                TYPE_DESCRIPTOR,
                dataSizeFromConfiguration,
                recordFormats.dynamic(),
                readOnly,
                databaseName,
                openOptions);
    }

    public static byte[] encodeFromNumbers(Object array, int offsetBytes) {
        ShortArray type = ShortArray.typeOf(array);
        if (type == null) {
            throw new IllegalArgumentException(array + " not a valid array type.");
        }

        if (type == ShortArray.DOUBLE || type == ShortArray.FLOAT) {
            // Skip array compaction for floating point numbers where compaction makes very little difference
            return createUncompactedArray(type, array, offsetBytes);
        } else {
            return createBitCompactedArray(type, array, offsetBytes);
        }
    }

    private static byte[] createBitCompactedArray(ShortArray type, Object array, int offsetBytes) {
        Class<?> componentType = array.getClass().getComponentType();
        boolean isPrimitiveByteArray = componentType.equals(Byte.TYPE);
        boolean isByteArray = componentType.equals(Byte.class) || isPrimitiveByteArray;
        int arrayLength = Array.getLength(array);
        int requiredBits = isByteArray ? Byte.SIZE : type.calculateRequiredBitsForArray(array, arrayLength);
        int totalBits = requiredBits * arrayLength;
        int bitsUsedInLastByte = totalBits % 8;
        bitsUsedInLastByte = bitsUsedInLastByte == 0 ? 8 : bitsUsedInLastByte;
        if (isByteArray) {
            return createBitCompactedByteArray(
                    type, isPrimitiveByteArray, array, bitsUsedInLastByte, requiredBits, offsetBytes);
        } else {
            int numberOfBytes = (totalBits - 1) / 8 + 1;
            numberOfBytes += NUMBER_HEADER_SIZE; // type + rest + requiredBits header. TODO no need to use full bytes
            BitBuffer bits = BitBuffer.bits(numberOfBytes);
            bits.put((byte) type.intValue());
            bits.put((byte) bitsUsedInLastByte);
            bits.put((byte) requiredBits);
            type.writeAll(array, arrayLength, requiredBits, bits);
            return bits.asBytes(offsetBytes);
        }
    }

    private static byte[] createBitCompactedByteArray(
            ShortArray type,
            boolean isPrimitiveByteArray,
            Object array,
            int bitsUsedInLastByte,
            int requiredBits,
            int offsetBytes) {
        int arrayLength = Array.getLength(array);
        byte[] bytes = new byte[NUMBER_HEADER_SIZE + arrayLength + offsetBytes];
        bytes[offsetBytes] = (byte) type.intValue();
        bytes[offsetBytes + 1] = (byte) bitsUsedInLastByte;
        bytes[offsetBytes + 2] = (byte) requiredBits;
        if (isPrimitiveByteArray) {
            arraycopy(array, 0, bytes, NUMBER_HEADER_SIZE + offsetBytes, arrayLength);
        } else {
            Byte[] source = (Byte[]) array;
            for (int i = 0; i < source.length; i++) {
                bytes[NUMBER_HEADER_SIZE + offsetBytes + i] = source[i];
            }
        }
        return bytes;
    }

    private static byte[] createUncompactedArray(ShortArray type, Object array, int offsetBytes) {
        int arrayLength = Array.getLength(array);
        int bytesPerElement = type.maxBits / 8;
        byte[] bytes = new byte[NUMBER_HEADER_SIZE + bytesPerElement * arrayLength + offsetBytes];
        bytes[offsetBytes] = (byte) type.intValue();
        bytes[offsetBytes + 1] = (byte) 8;
        bytes[offsetBytes + 2] = (byte) type.maxBits;
        type.writeAll(array, bytes, NUMBER_HEADER_SIZE + offsetBytes);
        return bytes;
    }

    public static void allocateFromNumbers(
            Collection<DynamicRecord> target,
            Object array,
            DynamicRecordAllocator recordAllocator,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        byte[] bytes = encodeFromNumbers(array, 0);
        allocateRecordsFromBytes(target, bytes, recordAllocator, cursorContext, memoryTracker);
    }

    private static void allocateFromCompositeType(
            Collection<DynamicRecord> target,
            byte[] bytes,
            DynamicRecordAllocator recordAllocator,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        allocateRecordsFromBytes(target, bytes, recordAllocator, cursorContext, memoryTracker);
    }

    private static void allocateFromString(
            Collection<DynamicRecord> target,
            String[] array,
            DynamicRecordAllocator recordAllocator,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        byte[][] stringsAsBytes = new byte[array.length][];
        int totalBytesRequired = STRING_HEADER_SIZE; // 1b type + 4b array length
        for (int i = 0; i < array.length; i++) {
            String string = array[i];
            byte[] bytes = PropertyStore.encodeString(string);
            stringsAsBytes[i] = bytes;
            totalBytesRequired += 4 /*byte[].length*/ + bytes.length;
        }

        // byte order of string array metadata (lengths) always big-endian
        try (var scopedBuffer = new HeapScopedBuffer(totalBytesRequired, ByteOrder.BIG_ENDIAN, memoryTracker)) {
            var buffer = scopedBuffer.getBuffer();
            buffer.put(PropertyType.STRING.byteValue());
            buffer.putInt(array.length);
            for (byte[] stringAsBytes : stringsAsBytes) {
                buffer.putInt(stringAsBytes.length);
                buffer.put(stringAsBytes);
            }
            allocateRecordsFromBytes(target, buffer.array(), recordAllocator, cursorContext, memoryTracker);
        }
    }

    public static void allocateRecords(
            Collection<DynamicRecord> target,
            Object array,
            DynamicRecordAllocator recordAllocator,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        if (!array.getClass().isArray()) {
            throw new IllegalArgumentException(array + " not an array");
        }

        Class<?> type = array.getClass().getComponentType();
        if (type.equals(String.class)) {
            allocateFromString(target, (String[]) array, recordAllocator, cursorContext, memoryTracker);
        } else if (type.equals(PointValue.class)) {
            allocateFromCompositeType(
                    target,
                    GeometryType.encodePointArray((PointValue[]) array),
                    recordAllocator,
                    cursorContext,
                    memoryTracker);
        } else if (type.equals(LocalDate.class)) {
            allocateFromCompositeType(
                    target,
                    TemporalType.encodeDateArray((LocalDate[]) array),
                    recordAllocator,
                    cursorContext,
                    memoryTracker);
        } else if (type.equals(LocalTime.class)) {
            allocateFromCompositeType(
                    target,
                    TemporalType.encodeLocalTimeArray((LocalTime[]) array),
                    recordAllocator,
                    cursorContext,
                    memoryTracker);
        } else if (type.equals(LocalDateTime.class)) {
            allocateFromCompositeType(
                    target,
                    TemporalType.encodeLocalDateTimeArray((LocalDateTime[]) array),
                    recordAllocator,
                    cursorContext,
                    memoryTracker);
        } else if (type.equals(OffsetTime.class)) {
            allocateFromCompositeType(
                    target,
                    TemporalType.encodeTimeArray((OffsetTime[]) array),
                    recordAllocator,
                    cursorContext,
                    memoryTracker);
        } else if (type.equals(ZonedDateTime.class)) {
            allocateFromCompositeType(
                    target,
                    TemporalType.encodeDateTimeArray((ZonedDateTime[]) array),
                    recordAllocator,
                    cursorContext,
                    memoryTracker);
        } else if (type.equals(DurationValue.class)) {
            allocateFromCompositeType(
                    target,
                    TemporalType.encodeDurationArray((DurationValue[]) array),
                    recordAllocator,
                    cursorContext,
                    memoryTracker);
        } else {
            allocateFromNumbers(target, array, recordAllocator, cursorContext, memoryTracker);
        }
    }

    public static ArrayValue getRightArray(byte[] header, byte[] bArray) {
        byte typeId = header[0];
        if (typeId == PropertyType.STRING.intValue()) {
            ByteBuffer headerBuffer = ByteBuffer.wrap(header, 1 /*skip the type*/, header.length - 1);
            int arrayLength = headerBuffer.getInt();
            String[] result = new String[arrayLength];

            ByteBuffer dataBuffer = ByteBuffer.wrap(bArray);
            for (int i = 0; i < arrayLength; i++) {
                int byteLength = dataBuffer.getInt();
                byte[] stringByteArray = new byte[byteLength];
                dataBuffer.get(stringByteArray);
                result[i] = PropertyStore.decodeString(stringByteArray);
            }
            return Values.stringArray(result);
        } else if (typeId == PropertyType.GEOMETRY.intValue()) {
            GeometryType.GeometryHeader geometryHeader = GeometryType.GeometryHeader.fromArrayHeaderBytes(header);
            return GeometryType.decodeGeometryArray(geometryHeader, bArray);
        } else if (typeId == PropertyType.TEMPORAL.intValue()) {
            TemporalType.TemporalHeader temporalHeader = TemporalType.TemporalHeader.fromArrayHeaderBytes(header);
            return TemporalType.decodeTemporalArray(temporalHeader, bArray);
        } else {
            return getNumbersArray(header, bArray);
        }
    }

    public static ArrayValue getNumbersArray(byte[] header, byte[] bArray) {
        byte typeId = header[0];
        ShortArray type = ShortArray.typeOf(typeId);
        int bitsUsedInLastByte = header[1];
        int requiredBits = header[2];
        if (requiredBits == 0) {
            return type.createEmptyArray();
        }
        if (type == ShortArray.BYTE
                && requiredBits == Byte.SIZE) { // Optimization for byte arrays (probably large ones)
            return Values.byteArray(bArray);
        } else { // Fallback to the generic approach, which is a slower
            BitBuffer bits = BitBuffer.bitsFromBytes(bArray);
            int length = (bArray.length * 8 - (8 - bitsUsedInLastByte)) / requiredBits;
            return type.createArray(length, bits, requiredBits);
        }
    }

    public Value getArrayFor(Iterable<DynamicRecord> records, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        HeavyRecordData data = readFullByteArray(records, PropertyType.ARRAY, storeCursors, memoryTracker);
        byte[] header = data.header();
        byte[] bArray = data.data();
        return getRightArray(header, bArray);
    }
}
