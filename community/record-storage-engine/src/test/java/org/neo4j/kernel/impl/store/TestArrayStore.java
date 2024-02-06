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

import static java.lang.Double.longBitsToDouble;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.AbstractDynamicStore.HeavyRecordData;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.util.BitBuffer;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

@PageCacheExtension
@Neo4jLayoutExtension
public class TestArrayStore {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private DatabaseLayout databaseLayout;

    private DynamicArrayStore arrayStore;
    private NeoStores neoStores;
    private DynamicAllocatorProvider allocatorProvider;

    public static Stream<Arguments> recordFormats() {
        return Stream.of(Arguments.of(PageAligned.LATEST_RECORD_FORMATS));
    }

    void setup(RecordFormats recordFormats) {
        var pageCacheTracer = PageCacheTracer.NULL;
        var factory = new StoreFactory(
                databaseLayout,
                Config.defaults(),
                new DefaultIdGeneratorFactory(
                        fileSystem, immediate(), pageCacheTracer, databaseLayout.getDatabaseName()),
                pageCache,
                pageCacheTracer,
                fileSystem,
                recordFormats,
                NullLogProvider.getInstance(),
                CursorContextFactory.NULL_CONTEXT_FACTORY,
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                Sets.immutable.empty());

        neoStores = factory.openAllNeoStores();
        allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);
        arrayStore = neoStores.getPropertyStore().getArrayStore();
    }

    @AfterEach
    void after() {
        if (neoStores != null) {
            neoStores.close();
        }
    }

    @ParameterizedTest
    @MethodSource("recordFormats")
    void intArrayPropertiesShouldBeBitPacked(RecordFormats recordFormats) {
        setup(recordFormats);
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized(
                new int[] {1, 2, 3, 4, 5, 6, 7}, PropertyType.INT, 3);
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized(
                new int[] {1, 2, 3, 4, 5, 6, 7, 8}, PropertyType.INT, 4);
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized(
                new int[] {1000, 10000, 13000}, PropertyType.INT, 14);
    }

    @ParameterizedTest
    @MethodSource("recordFormats")
    void longArrayPropertiesShouldBeBitPacked(RecordFormats recordFormats) {
        setup(recordFormats);
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized(
                new long[] {1, 2, 3, 4, 5, 6, 7}, PropertyType.LONG, 3);
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized(
                new long[] {1, 2, 3, 4, 5, 6, 7, 8}, PropertyType.LONG, 4);
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized(
                new long[] {1000, 10000, 13000, 15000000000L}, PropertyType.LONG, 34);
    }

    @ParameterizedTest
    @MethodSource("recordFormats")
    void doubleArrayPropertiesShouldNotBeBitPacked(RecordFormats recordFormats) {
        setup(recordFormats);
        // TODO Enabling right-trim would allow doubles that are integers, like 42.0, to pack well
        // While enabling the default left-trim would only allow some extreme doubles to pack, like
        // Double.longBitsToDouble( 0x1L )

        // Test doubles that pack well with right-trim
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized(
                new double[] {0.0, -100.0, 100.0, 0.5}, PropertyType.DOUBLE, 64);
        // Test doubles that pack well with left-trim
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized(
                new double[] {longBitsToDouble(0x1L), longBitsToDouble(0x8L)}, PropertyType.DOUBLE, 64);
    }

    @ParameterizedTest
    @MethodSource("recordFormats")
    void byteArrayPropertiesShouldNotBeBitPacked(RecordFormats recordFormats) {
        setup(recordFormats);
        /* Byte arrays are always stored unpacked. For two reasons:
         * - They are very unlikely to gain anything from bit packing
         * - byte[] are often used for storing big arrays and the bigger the long
         *   any bit analysis would take. For both writing and reading */
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized(
                new byte[] {1, 2, 3, 4, 5}, PropertyType.BYTE, Byte.SIZE);
    }

    @ParameterizedTest
    @MethodSource("recordFormats")
    void stringArrayGetsStoredAsUtf8(RecordFormats recordFormats) {
        setup(recordFormats);
        String[] array = new String[] {"first", "second"};
        Collection<DynamicRecord> records = new ArrayList<>();
        arrayStore.allocateRecords(
                records,
                array,
                allocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                CursorContext.NULL_CONTEXT,
                INSTANCE);
        var loaded = loadArray(records);
        assertStringHeader(loaded.header(), array.length);
        ByteBuffer buffer = ByteBuffer.wrap(loaded.data());
        for (String item : array) {
            byte[] expectedData = UTF8.encode(item);
            assertEquals(expectedData.length, buffer.getInt());
            byte[] loadedItem = new byte[expectedData.length];
            buffer.get(loadedItem);
            assertArrayEquals(expectedData, loadedItem);
        }
    }

    @ParameterizedTest
    @MethodSource("recordFormats")
    void pointArraysOfWgs84(RecordFormats recordFormats) {
        setup(recordFormats);
        PointValue[] array = new PointValue[] {
            Values.pointValue(CoordinateReferenceSystem.WGS_84, -45.0, -45.0),
            Values.pointValue(CoordinateReferenceSystem.WGS_84, 12.8, 56.3)
        };
        int numberOfBitsUsedForDoubles = 64;

        assertPointArrayHasCorrectFormat(array, numberOfBitsUsedForDoubles);
    }

    @ParameterizedTest
    @MethodSource("recordFormats")
    void pointArraysOfCartesian(RecordFormats recordFormats) {
        setup(recordFormats);
        PointValue[] array = new PointValue[] {
            Values.pointValue(CoordinateReferenceSystem.CARTESIAN, -100.0, -100.0),
            Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 25.0, 50.5)
        };
        int numberOfBitsUsedForDoubles = 64;

        assertPointArrayHasCorrectFormat(array, numberOfBitsUsedForDoubles);
    }

    @ParameterizedTest
    @MethodSource("recordFormats")
    void pointArraysOfMixedCRS(RecordFormats recordFormats) {
        setup(recordFormats);
        assertThrows(IllegalArgumentException.class, () -> {
            PointValue[] array = new PointValue[] {
                Values.pointValue(CoordinateReferenceSystem.CARTESIAN, longBitsToDouble(0x1L), longBitsToDouble(0x7L)),
                Values.pointValue(CoordinateReferenceSystem.WGS_84, longBitsToDouble(0x1L), longBitsToDouble(0x1L))
            };

            Collection<DynamicRecord> records = new ArrayList<>();
            arrayStore.allocateRecords(
                    records,
                    array,
                    allocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                    CursorContext.NULL_CONTEXT,
                    INSTANCE);
        });
    }

    @ParameterizedTest
    @MethodSource("recordFormats")
    void pointArraysOfMixedDimension(RecordFormats recordFormats) {
        setup(recordFormats);
        assertThrows(InvalidArgumentException.class, () -> {
            PointValue[] array = new PointValue[] {
                Values.pointValue(CoordinateReferenceSystem.CARTESIAN, longBitsToDouble(0x1L), longBitsToDouble(0x7L)),
                Values.pointValue(
                        CoordinateReferenceSystem.CARTESIAN,
                        longBitsToDouble(0x1L),
                        longBitsToDouble(0x1L),
                        longBitsToDouble(0x4L))
            };

            Collection<DynamicRecord> records = new ArrayList<>();
            arrayStore.allocateRecords(
                    records,
                    array,
                    allocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                    CursorContext.NULL_CONTEXT,
                    INSTANCE);
        });
    }

    private void assertPointArrayHasCorrectFormat(PointValue[] array, int numberOfBitsUsedForDoubles) {
        Collection<DynamicRecord> records = new ArrayList<>();
        arrayStore.allocateRecords(
                records,
                array,
                allocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                CursorContext.NULL_CONTEXT,
                INSTANCE);
        var recordData = loadArray(records);
        assertGeometryHeader(
                recordData.header(),
                GeometryType.GEOMETRY_POINT.getGtype(),
                2,
                array[0].getCoordinateReferenceSystem().getTable().getTableId(),
                array[0].getCoordinateReferenceSystem().getCode());

        final int dimension = array[0].coordinate().length;
        double[] pointDoubles = new double[array.length * dimension];
        for (int i = 0; i < pointDoubles.length; i++) {
            pointDoubles[i] = array[i / dimension].coordinate()[i % dimension];
        }

        byte[] doubleHeader = Arrays.copyOf(recordData.data(), DynamicArrayStore.NUMBER_HEADER_SIZE);
        byte[] doubleBody =
                Arrays.copyOfRange(recordData.data(), DynamicArrayStore.NUMBER_HEADER_SIZE, recordData.data().length);
        assertNumericArrayHeaderAndContent(
                pointDoubles,
                PropertyType.DOUBLE,
                numberOfBitsUsedForDoubles,
                new HeavyRecordData(doubleHeader, doubleBody));
    }

    private static void assertStringHeader(byte[] header, int itemCount) {
        assertEquals(PropertyType.STRING.byteValue(), header[0]);
        assertEquals(itemCount, ByteBuffer.wrap(header, 1, 4).getInt());
    }

    private static void assertGeometryHeader(
            byte[] header, int geometryTpe, int dimension, int crsTableId, int crsCode) {
        assertEquals(PropertyType.GEOMETRY.byteValue(), header[0]);
        assertEquals(geometryTpe, header[1]);
        assertEquals(dimension, header[2]);
        assertEquals(crsTableId, header[3]);
        assertEquals(crsCode, ByteBuffer.wrap(header, 4, 2).getShort());
    }

    private void assertBitPackedArrayGetsCorrectlySerializedAndDeserialized(
            Object array, PropertyType type, int expectedBitsUsedPerItem) {
        Collection<DynamicRecord> records = storeArray(array);
        var recordData = loadArray(records);
        assertNumericArrayHeaderAndContent(array, type, expectedBitsUsedPerItem, recordData);
    }

    private static void assertNumericArrayHeaderAndContent(
            Object array, PropertyType type, int expectedBitsUsedPerItem, HeavyRecordData recordData) {
        assertArrayHeader(recordData.header(), type, expectedBitsUsedPerItem);
        BitBuffer bits = BitBuffer.bitsFromBytes(recordData.data());
        int length = Array.getLength(array);
        for (int i = 0; i < length; i++) {
            if (array instanceof double[]) {
                assertEquals(Double.doubleToLongBits(Array.getDouble(array, i)), bits.getLong(expectedBitsUsedPerItem));
            } else {
                assertEquals(Array.getLong(array, i), bits.getLong(expectedBitsUsedPerItem));
            }
        }
    }

    private static void assertArrayHeader(byte[] header, PropertyType type, int bitsPerItem) {
        assertEquals(type.byteValue(), header[0]);
        assertEquals(bitsPerItem, header[2]);
    }

    private Collection<DynamicRecord> storeArray(Object array) {
        Collection<DynamicRecord> records = new ArrayList<>();
        arrayStore.allocateRecords(
                records,
                array,
                allocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                CursorContext.NULL_CONTEXT,
                INSTANCE);
        try (var storeCursor = arrayStore.openPageCursorForWriting(0, NULL_CONTEXT)) {
            for (DynamicRecord record : records) {
                arrayStore.updateRecord(record, storeCursor, NULL_CONTEXT, StoreCursors.NULL);
            }
        }
        return records;
    }

    private HeavyRecordData loadArray(Collection<DynamicRecord> records) {
        return arrayStore.readFullByteArray(records, PropertyType.ARRAY, StoreCursors.NULL);
    }
}
