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
package org.neo4j.index.internal.gbptree;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.CursorException;

class OffloadStoreTest {
    private static final int PAGE_SIZE = 256;
    private static final int STABLE_GENERATION = 4;
    private static final int UNSTABLE_GENERATION = 5;

    private static final SimpleByteArrayLayout layout = new SimpleByteArrayLayout(false);
    private PageAwareByteArrayCursor cursor;
    private SimpleIdProvider idProvider;
    private OffloadPageCursorFactory pcFactory;
    private OffloadIdValidator idValidator;

    @BeforeEach
    void setup() {
        cursor = new PageAwareByteArrayCursor(PAGE_SIZE);
        idProvider = new SimpleIdProvider(cursor::duplicate);
        pcFactory = (id, flags, cursorContext) -> cursor.duplicate(id);
        idValidator = OffloadIdValidator.ALWAYS_TRUE;
    }

    @Test
    void mustReadKeyAndValue() throws IOException {
        OffloadStore<RawBytes, RawBytes> offloadStore = getOffloadStore();

        RawBytes key = layout.newKey();
        RawBytes value = layout.newValue();
        key.bytes = new byte[200];
        value.bytes = new byte[offloadStore.maxEntrySize() - layout.keySize(key)];
        long offloadId = offloadStore.writeKeyValue(key, value, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);

        {
            RawBytes into = layout.newKey();
            offloadStore.readKey(offloadId, into, NULL_CONTEXT);
            assertEquals(0, layout.compare(key, into));
        }

        {
            RawBytes into = layout.newKey();
            offloadStore.readValue(offloadId, into, NULL_CONTEXT);
            assertEquals(0, layout.compare(value, into));
        }

        {
            RawBytes intoKey = layout.newKey();
            RawBytes intoValue = layout.newValue();
            offloadStore.readKeyValue(offloadId, intoKey, intoValue, NULL_CONTEXT);
            assertEquals(0, layout.compare(key, intoKey));
            assertEquals(0, layout.compare(value, intoValue));
        }
    }

    @Test
    void mustThrowIfReadNegativeKeySize() throws IOException {
        OffloadStore<RawBytes, RawBytes> offloadStore = getOffloadStore();

        RawBytes key = layout.newKey();
        RawBytes value = layout.newValue();
        key.bytes = new byte[200];
        value.bytes = new byte[offloadStore.maxEntrySize() - layout.keySize(key)];
        long offloadId = offloadStore.writeKeyValue(key, value, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        cursor.next(offloadId);
        cursor.setOffset(OffloadStoreImpl.SIZE_HEADER);
        OffloadStoreImpl.putKeyValueSize(cursor, -1, layout.valueSize(value));

        // readKeyValue
        {
            RawBytes intoKey = layout.newKey();
            RawBytes intoValue = layout.newValue();
            CursorException e = assertThrows(
                    CursorException.class,
                    () -> offloadStore.readKeyValue(offloadId, intoKey, intoValue, NULL_CONTEXT));
            assertThat(e).hasMessageContaining("Read unreliable key");
        }

        // readKey
        {
            RawBytes into = layout.newKey();
            CursorException e =
                    assertThrows(CursorException.class, () -> offloadStore.readKey(offloadId, into, NULL_CONTEXT));
            assertThat(e).hasMessageContaining("Read unreliable key");
        }

        // readValue
        {
            RawBytes into = layout.newKey();
            CursorException e =
                    assertThrows(CursorException.class, () -> offloadStore.readValue(offloadId, into, NULL_CONTEXT));
            assertThat(e).hasMessageContaining("Read unreliable key");
        }
    }

    @Test
    void mustThrowIfReadNegativeValueSize() throws IOException {
        OffloadStore<RawBytes, RawBytes> offloadStore = getOffloadStore();

        RawBytes key = layout.newKey();
        RawBytes value = layout.newValue();
        key.bytes = new byte[200];
        value.bytes = new byte[offloadStore.maxEntrySize() - layout.keySize(key)];
        long offloadId = offloadStore.writeKeyValue(key, value, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        cursor.next(offloadId);
        cursor.setOffset(OffloadStoreImpl.SIZE_HEADER);
        OffloadStoreImpl.putKeyValueSize(cursor, layout.keySize(key), -1);

        // readKeyValue
        {
            RawBytes intoKey = layout.newKey();
            RawBytes intoValue = layout.newValue();
            CursorException e = assertThrows(
                    CursorException.class,
                    () -> offloadStore.readKeyValue(offloadId, intoKey, intoValue, NULL_CONTEXT));
            assertThat(e).hasMessageContaining("Read unreliable key");
        }

        // readKey
        {
            RawBytes into = layout.newKey();
            CursorException e =
                    assertThrows(CursorException.class, () -> offloadStore.readKey(offloadId, into, NULL_CONTEXT));
            assertThat(e).hasMessageContaining("Read unreliable key");
        }

        // readValue
        {
            RawBytes into = layout.newKey();
            CursorException e =
                    assertThrows(CursorException.class, () -> offloadStore.readValue(offloadId, into, NULL_CONTEXT));
            assertThat(e).hasMessageContaining("Read unreliable key");
        }
    }

    @Test
    void mustInitializeOffloadPage() throws IOException {
        OffloadStoreImpl<RawBytes, RawBytes> offloadStore = getOffloadStore();

        RawBytes key = layout.newKey();
        key.bytes = new byte[200];
        long offloadId = offloadStore.writeKey(key, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);

        cursor.next(offloadId);
        assertEquals(TreeNodeUtil.NODE_TYPE_OFFLOAD, TreeNodeUtil.nodeType(cursor));
    }

    @Test
    void mustAssertOnOffloadPageDuringRead() {
        OffloadStoreImpl<RawBytes, RawBytes> offloadStore = getOffloadStore();
        String expectedMessage = "Tried to read from offload store but page is not an offload page";

        {
            RawBytes key = layout.newKey();
            IOException exception = assertThrows(IOException.class, () -> offloadStore.readKey(0, key, NULL_CONTEXT));
            assertTrue(exception.getMessage().contains(expectedMessage));
        }

        {
            RawBytes value = layout.newValue();
            IOException exception =
                    assertThrows(IOException.class, () -> offloadStore.readValue(0, value, NULL_CONTEXT));
            assertTrue(exception.getMessage().contains(expectedMessage));
        }

        {
            RawBytes key = layout.newKey();
            RawBytes value = layout.newValue();
            IOException exception =
                    assertThrows(IOException.class, () -> offloadStore.readKeyValue(0, key, value, NULL_CONTEXT));
            assertTrue(exception.getMessage().contains(expectedMessage));
        }
    }

    private OffloadStoreImpl<RawBytes, RawBytes> getOffloadStore() {
        return new OffloadStoreImpl<>(layout, idProvider, pcFactory, idValidator, PAGE_SIZE);
    }
}
