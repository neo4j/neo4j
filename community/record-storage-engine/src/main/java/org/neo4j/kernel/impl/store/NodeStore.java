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

import static java.lang.String.format;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_LABEL_STORE_CURSOR;
import static org.neo4j.kernel.impl.store.NoStoreHeaderFormat.NO_STORE_HEADER_FORMAT;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.util.BitBuffer;

/**
 * Implementation of the node store.
 */
public class NodeStore extends CommonAbstractStore<NodeRecord, NoStoreHeader> {
    public static final String TYPE_DESCRIPTOR = "NodeStore";
    private final DynamicArrayStore dynamicLabelStore;

    public static Long readOwnerFromDynamicLabelsRecord(DynamicRecord record) {
        byte[] data = record.getData();
        byte[] header = PropertyType.ARRAY.readDynamicRecordHeader(data);
        byte[] array = Arrays.copyOfRange(data, header.length, data.length);

        int requiredBits = header[2];
        if (requiredBits == 0) {
            return null;
        }
        BitBuffer bits = BitBuffer.bitsFromBytes(array);
        return bits.getLong(requiredBits);
    }

    public NodeStore(
            FileSystemAbstraction fileSystem,
            Path path,
            Path idFile,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            InternalLogProvider logProvider,
            DynamicArrayStore dynamicLabelStore,
            RecordFormats recordFormats,
            boolean readOnly,
            String databaseName,
            ImmutableSet<OpenOption> openOptions) {
        super(
                fileSystem,
                path,
                idFile,
                config,
                RecordIdType.NODE,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                logProvider,
                TYPE_DESCRIPTOR,
                recordFormats.node(),
                NO_STORE_HEADER_FORMAT,
                readOnly,
                databaseName,
                openOptions);
        this.dynamicLabelStore = dynamicLabelStore;
    }

    @Override
    public void ensureHeavy(NodeRecord node, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        if (NodeLabelsField.fieldPointsToDynamicRecordOfLabels(node.getLabelField())) {
            ensureHeavy(
                    node, NodeLabelsField.firstDynamicLabelRecordId(node.getLabelField()), storeCursors, memoryTracker);
        }
    }

    public void ensureHeavy(
            NodeRecord node, long firstDynamicLabelRecord, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        if (!node.isLight()) {
            return;
        }

        // Load any dynamic labels and populate the node record
        try {
            node.setLabelField(
                    node.getLabelField(),
                    dynamicLabelStore.getRecords(
                            firstDynamicLabelRecord,
                            RecordLoad
                                    .ALWAYS /* We might load labels that are no longer in use. In situations where this matters, getUsedDynamicLabels should be used. */,
                            false,
                            storeCursors.readCursor(DYNAMIC_LABEL_STORE_CURSOR),
                            memoryTracker));
        } catch (InvalidRecordException e) {
            throw new InvalidRecordException(
                    format("Error loading dynamic label records for %s | %s", node, e.getMessage()), e);
        }
    }

    @Override
    public void updateRecord(
            NodeRecord record,
            IdUpdateListener idUpdateListener,
            PageCursor cursor,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        super.updateRecord(record, idUpdateListener, cursor, cursorContext, storeCursors);
        updateDynamicLabelRecords(record.getDynamicLabelRecords(), idUpdateListener, cursorContext, storeCursors);
    }

    public DynamicArrayStore getDynamicLabelStore() {
        return dynamicLabelStore;
    }

    public void updateDynamicLabelRecords(
            Iterable<DynamicRecord> dynamicLabelRecords,
            IdUpdateListener idUpdateListener,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        try (var cursor = storeCursors.writeCursor(DYNAMIC_LABEL_STORE_CURSOR)) {
            for (DynamicRecord record : dynamicLabelRecords) {
                dynamicLabelStore.updateRecord(record, idUpdateListener, cursor, cursorContext, storeCursors);
            }
        }
    }
}
