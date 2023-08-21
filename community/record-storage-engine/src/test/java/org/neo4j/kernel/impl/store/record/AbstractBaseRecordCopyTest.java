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

import static java.lang.String.format;
import static org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.store.PropertyStore.encodeValue;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.ReflectionUtil.callCopyConstructor;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StandaloneDynamicRecordAllocator;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class AbstractBaseRecordCopyTest {
    private static final int MAX_RANDOM_ARRAY = 128;
    private static final int MAX_RANDOM_LIST_SIZE = 16;

    @Inject
    private RandomSupport random;

    private final Map<Class<?>, Callable<Object>> dataProviders = new HashMap<>();

    @BeforeEach
    void setUp() {
        dataProviders.put(int.class, () -> random.nextInt());
        dataProviders.put(long.class, () -> random.nextLong());
        dataProviders.put(boolean.class, () -> random.nextBoolean());
        dataProviders.put(byte[].class, () -> {
            byte[] bytes = new byte[random.nextInt(MAX_RANDOM_ARRAY)];
            random.nextBytes(bytes);
            return bytes;
        });
        dataProviders.put(DynamicRecord.class, () -> randomPopulatedRecord(new DynamicRecord(-1)));
    }

    private static Stream<Arguments> records() {
        return Stream.of(
                Arguments.of(new NodeRecord(-1), NodeRecord.class.getSimpleName()),
                Arguments.of(new SchemaRecord(-1), SchemaRecord.class.getSimpleName()),
                Arguments.of(new NeoStoreRecord(), NeoStoreRecord.class.getSimpleName()),
                Arguments.of(new RelationshipRecord(-1), RelationshipRecord.class.getSimpleName()),
                Arguments.of(new RelationshipGroupRecord(-1), RelationshipGroupRecord.class.getSimpleName()),
                Arguments.of(new LabelTokenRecord(-1), LabelTokenRecord.class.getSimpleName()),
                Arguments.of(new RelationshipTypeTokenRecord(-1), RelationshipTypeTokenRecord.class.getSimpleName()),
                Arguments.of(new PropertyKeyTokenRecord(-1), PropertyKeyTokenRecord.class.getSimpleName()),
                Arguments.of(new DynamicRecord(-1), DynamicRecord.class.getSimpleName()));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("records")
    void copyAllFields(AbstractBaseRecord record, String classname) throws Exception {
        randomPopulatedRecord(record);

        AbstractBaseRecord copy = callCopyConstructor(record);
        assertTrue(reflectionEquals(record, copy));
        assertEquals(record, copy);
    }

    @Test
    void copyAllFieldsPropertyRecord() throws Exception {
        // Due to fields relying on each other we can not randomly populate all fields here.
        PropertyRecord record = getRandomPropertyRecord();

        PropertyRecord copy = new PropertyRecord(record);
        assertTrue(reflectionEquals(record, copy));
        assertEquals(record, copy);
    }

    private PropertyRecord getRandomPropertyRecord() throws Exception {
        PropertyRecord record = new PropertyRecord(random.nextLong());
        record.initialize(random.nextBoolean(), random.nextLong(), random.nextLong());
        record.setNodeId(random.nextLong());
        record.setPrevProp(random.nextLong());
        record.setNextProp(random.nextLong());
        int numberOfDeletedRecords = random.intBetween(0, PropertyType.getPayloadSizeLongs());
        for (int i = 0; i < numberOfDeletedRecords; i++) {
            DynamicRecord randomDynamicRecord = getRandomDynamicRecord();
            randomDynamicRecord.setInUse(false);
            record.addDeletedRecord(randomDynamicRecord);
        }

        StandaloneDynamicRecordAllocator stringAllocator = new StandaloneDynamicRecordAllocator();
        StandaloneDynamicRecordAllocator arrayAllocator = new StandaloneDynamicRecordAllocator();
        int blocksOccupied = 0;
        while (blocksOccupied < 4) {
            PropertyBlock block = new PropertyBlock();
            // Dynamic records will not be written and read by the property record format,
            // that happens in the store where it delegates to a "sub" store.
            encodeValue(
                    block,
                    random.nextInt(16),
                    random.nextValue(),
                    stringAllocator,
                    arrayAllocator,
                    NULL_CONTEXT,
                    INSTANCE);
            int tentativeBlocksWithThisOne = blocksOccupied + block.getValueBlocks().length;
            if (tentativeBlocksWithThisOne <= 4) {
                record.addPropertyBlock(block);
                blocksOccupied = tentativeBlocksWithThisOne;
            }
        }

        return record;
    }

    private DynamicRecord getRandomDynamicRecord() throws Exception {
        return (DynamicRecord) dataProviders.get(DynamicRecord.class).call();
    }

    private <T> T randomPopulatedRecord(T record) throws Exception {
        Class<?> clazz = record.getClass();
        while (clazz != null) {
            for (Field field : clazz.getDeclaredFields()) {
                field.setAccessible(true);
                if (Modifier.isStatic(field.getModifiers())) {
                    continue; // Don't touch static
                }

                field.set(record, newValue(field));
            }
            clazz = clazz.getSuperclass();
        }
        return record;
    }

    private Object newValue(Field field) throws Exception {
        Class<?> type = field.getType();

        if (type.isAssignableFrom(List.class)) {
            ParameterizedType listType = (ParameterizedType) field.getGenericType();
            Class<?> listClass = (Class<?>) listType.getActualTypeArguments()[0];
            Callable<Object> randomDataProviderForType = getRandomDataProviderForType(listClass);
            int size = random.nextInt(MAX_RANDOM_LIST_SIZE);
            List<Object> value = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                value.add(randomDataProviderForType.call());
            }
            return value;
        }

        return getRandomDataProviderForType(type).call();
    }

    private Callable<Object> getRandomDataProviderForType(Class<?> type) {
        Callable<Object> objectSupplier = dataProviders.get(type);
        if (objectSupplier == null) {
            throw new AssertionError(format("Missing data provider for type %s", type.getSimpleName()));
        }
        return objectSupplier;
    }
}
