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

import static org.neo4j.kernel.impl.store.AbstractDynamicStore.allocateRecordsFromBytes;
import static org.neo4j.kernel.impl.store.NoStoreHeaderFormat.NO_STORE_HEADER_FORMAT;
import static org.neo4j.kernel.impl.store.PropertyStore.decodeString;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.CursorType;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.token.api.NamedToken;

public abstract class TokenStore<RECORD extends TokenRecord> extends CommonAbstractStore<RECORD, NoStoreHeader> {
    public static final int NAME_STORE_BLOCK_SIZE = 30;

    private final DynamicStringStore nameStore;
    private final CursorType cursorType;
    private final CursorType dynamicCursorType;

    public TokenStore(
            FileSystemAbstraction fileSystem,
            Path path,
            Path idFile,
            Config configuration,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            InternalLogProvider logProvider,
            DynamicStringStore nameStore,
            String typeDescriptor,
            RecordFormat<RECORD> recordFormat,
            boolean readOnly,
            String databaseName,
            CursorType cursorType,
            CursorType dynamicCursorType,
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
                typeDescriptor,
                recordFormat,
                NO_STORE_HEADER_FORMAT,
                readOnly,
                databaseName,
                openOptions);
        this.nameStore = nameStore;
        this.cursorType = cursorType;
        this.dynamicCursorType = dynamicCursorType;
    }

    public DynamicStringStore getNameStore() {
        return nameStore;
    }

    public List<NamedToken> getTokens(StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return readAllTokens(false, storeCursors, memoryTracker);
    }

    /**
     * Same as {@link #getTokens(StoreCursors, MemoryTracker)}, except tokens that cannot be read due to inconsistencies will just be ignored,
     * while {@link #getTokens(StoreCursors, MemoryTracker)} would throw an exception in such cases.
     * @return All tokens that could be read without any apparent problems.
     */
    public List<NamedToken> getAllReadableTokens(StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return readAllTokens(true, storeCursors, memoryTracker);
    }

    private List<NamedToken> readAllTokens(
            boolean ignoreInconsistentTokens, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        long highId = getIdGenerator().getHighId();
        ArrayList<NamedToken> records = new ArrayList<>(Math.toIntExact(highId));
        RECORD record = newRecord();
        var cursor = getTokenStoreCursor(storeCursors);
        for (int i = 0; i < highId; i++) {
            if (!getRecordByCursor(i, record, RecordLoad.LENIENT_CHECK, cursor, memoryTracker)
                    .inUse()) {
                continue;
            }

            if (record.getNameId() != Record.RESERVED.intValue()) {
                try {
                    String name = getStringFor(record, storeCursors, memoryTracker);
                    records.add(new NamedToken(name, i, record.isInternal()));
                } catch (Exception e) {
                    if (!ignoreInconsistentTokens) {
                        throw e;
                    }
                }
            }
        }
        return records;
    }

    public NamedToken getToken(int id, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        RECORD record = getRecordByCursor(id, newRecord(), NORMAL, getTokenStoreCursor(storeCursors), memoryTracker);
        return new NamedToken(
                getStringFor(record, storeCursors, memoryTracker), record.getIntId(), record.isInternal());
    }

    public Collection<DynamicRecord> allocateNameRecords(
            byte[] chars,
            DynamicRecordAllocator nameStoreRecordAllocator,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        Collection<DynamicRecord> records = HeapTrackingCollections.newArrayList(memoryTracker);
        allocateRecordsFromBytes(records, chars, nameStoreRecordAllocator, cursorContext, memoryTracker);
        return records;
    }

    @Override
    public void updateRecord(
            RECORD record,
            IdUpdateListener idUpdateListener,
            PageCursor cursor,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        super.updateRecord(record, idUpdateListener, cursor, cursorContext, storeCursors);
        if (!record.isLight()) {
            try (var nameCursor = getWriteDynamicTokenCursor(storeCursors)) {
                for (DynamicRecord keyRecord : record.getNameRecords()) {
                    nameStore.updateRecord(keyRecord, idUpdateListener, nameCursor, cursorContext, storeCursors);
                }
            }
        }
    }

    @Override
    public void ensureHeavy(RECORD record, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        if (!record.isLight()) {
            return;
        }

        // Guard for cycles in the name chain, since this might be called by the consistency checker on an inconsistent
        // store.
        // This will throw an exception if there's a cycle, and we'll just ignore those tokens at this point.
        record.addNameRecords(nameStore.getRecords(
                record.getNameId(), NORMAL, true, getDynamicTokenCursor(storeCursors), memoryTracker));
    }

    public PageCursor getTokenStoreCursor(StoreCursors storeCursors) {
        return storeCursors.readCursor(cursorType);
    }

    public PageCursor getDynamicTokenCursor(StoreCursors storeCursors) {
        return storeCursors.readCursor(dynamicCursorType);
    }

    public PageCursor getWriteDynamicTokenCursor(StoreCursors storeCursors) {
        return storeCursors.writeCursor(dynamicCursorType);
    }

    public String getStringFor(RECORD nameRecord, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        ensureHeavy(nameRecord, storeCursors, memoryTracker);
        int recordToFind = nameRecord.getNameId();
        Iterator<DynamicRecord> records = nameRecord.getNameRecords().iterator();
        Collection<DynamicRecord> relevantRecords = new ArrayList<>();
        while (recordToFind != Record.NO_NEXT_BLOCK.intValue() && records.hasNext()) {
            DynamicRecord record = records.next();
            if (record.inUse() && record.getId() == recordToFind) {
                recordToFind = (int) record.getNextBlock();
                relevantRecords.add(record);
                records = nameRecord.getNameRecords().iterator();
            }
        }
        return decodeString(nameStore
                .readFullByteArray(relevantRecords, PropertyType.STRING, storeCursors, memoryTracker)
                .data());
    }
}
