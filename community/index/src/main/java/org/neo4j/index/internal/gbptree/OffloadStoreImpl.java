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

import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.CursorCreator.bind;
import static org.neo4j.index.internal.gbptree.PointerChecking.checkOutOfBounds;

import java.io.IOException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageCursorUtil;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.util.VisibleForTesting;

/**
 * Offload page layout: [HEADER 9B| KEYSIZE 4B| VALUESIZE 4B | KEY | VALUE]
 * [HEADER]: [TYPE 1B | RESERVED SPACE 8B]
 * Key and value size are simple integers
 * Key and value layout is decided by layout.
 */
public class OffloadStoreImpl<KEY, VALUE> implements OffloadStore<KEY, VALUE> {
    @VisibleForTesting
    static final int SIZE_HEADER = Byte.BYTES + Long.BYTES;

    private static final int SIZE_KEY_SIZE = Integer.BYTES;
    private static final int SIZE_VALUE_SIZE = Integer.BYTES;
    private final Layout<KEY, VALUE> layout;
    private final IdProvider idProvider;
    private final OffloadPageCursorFactory pcFactory;
    private final OffloadIdValidator offloadIdValidator;
    private final int maxEntrySize;

    OffloadStoreImpl(
            Layout<KEY, VALUE> layout,
            IdProvider idProvider,
            OffloadPageCursorFactory pcFactory,
            OffloadIdValidator offloadIdValidator,
            int payloadSize) {
        this.layout = layout;
        this.idProvider = idProvider;
        this.pcFactory = pcFactory;
        this.offloadIdValidator = offloadIdValidator;
        this.maxEntrySize = keyValueSizeCapFromPageSize(payloadSize);
    }

    @Override
    public int maxEntrySize() {
        return maxEntrySize;
    }

    @Override
    public void readKey(long offloadId, KEY into, CursorContext cursorContext) throws IOException {
        validateOffloadId(offloadId);

        try (PageCursor cursor = pcFactory.create(offloadId, PagedFile.PF_SHARED_READ_LOCK, cursorContext)) {
            do {
                placeCursorAtOffloadId(cursor, offloadId);

                if (!readHeader(cursor)) {
                    continue;
                }
                cursor.setOffset(SIZE_HEADER);
                int keySize = cursor.getInt();
                int valueSize = cursor.getInt();
                if (keyValueSizeTooLarge(keySize, valueSize) || keySize < 0 || valueSize < 0) {
                    readUnreliableKeyValueSize(cursor, keySize, valueSize);
                    continue;
                }
                layout.readKey(cursor, into, keySize);
            } while (cursor.shouldRetry());
            checkOutOfBounds(cursor);
            cursor.checkAndClearCursorException();
        }
    }

    @Override
    public void readKeyValue(long offloadId, KEY key, VALUE value, CursorContext cursorContext) throws IOException {
        validateOffloadId(offloadId);

        try (PageCursor cursor = pcFactory.create(offloadId, PagedFile.PF_SHARED_READ_LOCK, cursorContext)) {
            do {
                placeCursorAtOffloadId(cursor, offloadId);

                if (!readHeader(cursor)) {
                    continue;
                }
                cursor.setOffset(SIZE_HEADER);
                int keySize = cursor.getInt();
                int valueSize = cursor.getInt();
                if (keyValueSizeTooLarge(keySize, valueSize) || keySize < 0 || valueSize < 0) {
                    readUnreliableKeyValueSize(cursor, keySize, valueSize);
                    continue;
                }
                layout.readKey(cursor, key, keySize);
                layout.readValue(cursor, value, valueSize);
            } while (cursor.shouldRetry());
            checkOutOfBounds(cursor);
            cursor.checkAndClearCursorException();
        }
    }

    @Override
    public void readValue(long offloadId, VALUE into, CursorContext cursorContext) throws IOException {
        validateOffloadId(offloadId);

        try (PageCursor cursor = pcFactory.create(offloadId, PagedFile.PF_SHARED_READ_LOCK, cursorContext)) {
            do {
                placeCursorAtOffloadId(cursor, offloadId);

                if (!readHeader(cursor)) {
                    continue;
                }
                cursor.setOffset(SIZE_HEADER);
                int keySize = cursor.getInt();
                int valueSize = cursor.getInt();
                if (keyValueSizeTooLarge(keySize, valueSize) || keySize < 0 || valueSize < 0) {
                    readUnreliableKeyValueSize(cursor, keySize, valueSize);
                    continue;
                }
                cursor.setOffset(cursor.getOffset() + keySize);
                layout.readValue(cursor, into, valueSize);
            } while (cursor.shouldRetry());
            checkOutOfBounds(cursor);
            cursor.checkAndClearCursorException();
        }
    }

    @Override
    public long writeKey(KEY key, long stableGeneration, long unstableGeneration, CursorContext cursorContext)
            throws IOException {
        int keySize = layout.keySize(key);
        long newId = acquireNewId(stableGeneration, unstableGeneration, cursorContext);
        try (PageCursor cursor = pcFactory.create(newId, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext)) {
            placeCursorAtOffloadId(cursor, newId);

            writeHeader(cursor);
            cursor.setOffset(SIZE_HEADER);
            putKeyValueSize(cursor, keySize, 0);
            layout.writeKey(cursor, key);
            return newId;
        }
    }

    @Override
    public long writeKeyValue(
            KEY key, VALUE value, long stableGeneration, long unstableGeneration, CursorContext cursorContext)
            throws IOException {
        int keySize = layout.keySize(key);
        int valueSize = layout.valueSize(value);
        long newId = acquireNewId(stableGeneration, unstableGeneration, cursorContext);
        try (PageCursor cursor = pcFactory.create(newId, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext)) {
            placeCursorAtOffloadId(cursor, newId);

            writeHeader(cursor);
            cursor.setOffset(SIZE_HEADER);
            putKeyValueSize(cursor, keySize, valueSize);
            layout.writeKey(cursor, key);
            layout.writeValue(cursor, value);
            return newId;
        }
    }

    @Override
    public void free(long offloadId, long stableGeneration, long unstableGeneration, CursorContext cursorContext)
            throws IOException {
        idProvider.releaseId(
                stableGeneration,
                unstableGeneration,
                offloadId,
                bind(pcFactory, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext));
    }

    @VisibleForTesting
    static int keyValueSizeCapFromPageSize(int payloadSize) {
        return payloadSize - SIZE_HEADER - SIZE_KEY_SIZE - SIZE_VALUE_SIZE;
    }

    static void writeHeader(PageCursor cursor) {
        cursor.putByte(TreeNodeUtil.BYTE_POS_NODE_TYPE, TreeNodeUtil.NODE_TYPE_OFFLOAD);
    }

    private static boolean readHeader(PageCursor cursor) {
        byte type = TreeNodeUtil.nodeType(cursor);
        if (type != TreeNodeUtil.NODE_TYPE_OFFLOAD) {
            cursor.setCursorException(format(
                    "Tried to read from offload store but page is not an offload page. Expected %d but was %d",
                    TreeNodeUtil.NODE_TYPE_OFFLOAD, type));
            return false;
        }
        return true;
    }

    @VisibleForTesting
    static void putKeyValueSize(PageCursor cursor, int keySize, int valueSize) {
        cursor.putInt(keySize);
        cursor.putInt(valueSize);
    }

    private long acquireNewId(long stableGeneration, long unstableGeneration, CursorContext cursorContext)
            throws IOException {
        return idProvider.acquireNewId(
                stableGeneration, unstableGeneration, bind(pcFactory, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext));
    }

    private static void placeCursorAtOffloadId(PageCursor cursor, long offloadId) throws IOException {
        PageCursorUtil.goTo(cursor, "offload page", offloadId);
    }

    private boolean keyValueSizeTooLarge(int keySize, int valueSize) {
        return keySize > maxEntrySize || valueSize > maxEntrySize || (keySize + valueSize) > maxEntrySize;
    }

    private static void readUnreliableKeyValueSize(PageCursor cursor, int keySize, int valueSize) {
        cursor.setCursorException(format(
                "Read unreliable key, id=%d, keySize=%d, valueSize=%d", cursor.getCurrentPageId(), keySize, valueSize));
    }

    private void validateOffloadId(long offloadId) throws IOException {
        if (!offloadIdValidator.valid(offloadId)) {
            throw new IOException(String.format("Offload id %d is outside of valid range, ", offloadId));
        }
    }
}
