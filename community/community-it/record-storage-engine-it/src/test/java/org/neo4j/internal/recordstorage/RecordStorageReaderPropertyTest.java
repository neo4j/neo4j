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

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;

import java.lang.reflect.Array;
import org.junit.jupiter.api.Test;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Test read access to committed properties.
 */
class RecordStorageReaderPropertyTest extends RecordStorageReaderTestBase {
    @Test
    void shouldGetAllNodeProperties() throws Exception {
        // GIVEN
        String longString =
                "AlalalalalongAlalalalalongAlalalalalongAlalalalalongAlalalalalongAlalalalalongAlalalalalongAlalalalalong";
        Object[] properties = {
            longString,
            createNew(String.class),
            createNew(long.class),
            createNew(int.class),
            createNew(byte.class),
            createNew(short.class),
            createNew(boolean.class),
            createNew(char.class),
            createNew(float.class),
            createNew(double.class),
            array(0, String.class),
            array(0, long.class),
            array(0, int.class),
            array(0, byte.class),
            array(0, short.class),
            array(0, boolean.class),
            array(0, char.class),
            array(0, float.class),
            array(0, double.class),
            array(1, String.class),
            array(1, long.class),
            array(1, int.class),
            array(1, byte.class),
            array(1, short.class),
            array(1, boolean.class),
            array(1, char.class),
            array(1, float.class),
            array(1, double.class),
            array(256, String.class),
            array(256, long.class),
            array(256, int.class),
            array(256, byte.class),
            array(256, short.class),
            array(256, boolean.class),
            array(256, char.class),
            array(256, float.class),
            array(256, double.class),
        };

        for (Object value : properties) {
            // given
            long nodeId = createNode(singletonMap("prop", value), label1);

            // when
            try (StorageNodeCursor node =
                    storageReader.allocateNodeCursor(NULL_CONTEXT, storageCursors, EmptyMemoryTracker.INSTANCE)) {
                node.single(nodeId);
                assertTrue(node.next());

                try (StoragePropertyCursor props = storageReader.allocatePropertyCursor(
                        NULL_CONTEXT, storageCursors, EmptyMemoryTracker.INSTANCE)) {
                    node.properties(props, ALL_PROPERTIES);
                    if (props.next()) {
                        Value propVal = props.propertyValue();

                        // then
                        assertTrue(propVal.equals(Values.of(value)), propVal + ".equals(" + value + ")");
                    } else {
                        fail();
                    }
                }
            }
        }
    }

    private static Object array(int length, Class<?> componentType) {
        Object array = Array.newInstance(componentType, length);
        for (int i = 0; i < length; i++) {
            Array.set(array, i, createNew(componentType));
        }
        return array;
    }

    private static Object createNew(Class<?> type) {
        if (type == int.class) {
            return 666;
        }
        if (type == long.class) {
            return 17L;
        }
        if (type == double.class) {
            return 6.28318530717958647692d;
        }
        if (type == float.class) {
            return 3.14f;
        }
        if (type == short.class) {
            return (short) 8733;
        }
        if (type == byte.class) {
            return (byte) 123;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return 'Z';
        }
        if (type == String.class) {
            return "hello world";
        }
        throw new IllegalArgumentException(type.getName());
    }
}
