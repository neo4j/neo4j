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
package org.neo4j.internal.batchimport;

import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

import java.util.concurrent.atomic.LongAdder;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.storageengine.util.IdGeneratorUpdatesWorkSync;

public class DeleteDuplicateNodesStep extends ProcessorStep<long[]> {
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;
    private final DataImporter.Monitor storeMonitor;
    private final IdGeneratorUpdatesWorkSync idUpdatesWorkSync;
    private final NeoStores neoStores;
    private final LongAdder nodesRemoved = new LongAdder();
    private final LongAdder propertiesRemoved = new LongAdder();

    public DeleteDuplicateNodesStep(
            StageControl control,
            Configuration config,
            NeoStores neoStores,
            DataImporter.Monitor storeMonitor,
            CursorContextFactory contextFactory,
            IdGeneratorUpdatesWorkSync idUpdatesWorkSync) {
        super(control, "DEDUP", config, 0, contextFactory);
        this.neoStores = neoStores;
        this.nodeStore = neoStores.getNodeStore();
        this.propertyStore = neoStores.getPropertyStore();
        this.storeMonitor = storeMonitor;
        this.idUpdatesWorkSync = idUpdatesWorkSync;
    }

    @Override
    protected void process(long[] batch, BatchSender sender, CursorContext cursorContext) throws Throwable {
        NodeRecord nodeRecord = nodeStore.newRecord();
        PropertyRecord propertyRecord = propertyStore.newRecord();
        try (var storeCursors = new CachedStoreCursors(neoStores, cursorContext);
                var idUpdates = idUpdatesWorkSync.newBatch(cursorContext)) {
            long batchPropertiesRemoved = 0;
            for (long duplicateNodeId : batch) {
                nodeStore.getRecordByCursor(duplicateNodeId, nodeRecord, NORMAL, storeCursors.readCursor(NODE_CURSOR));
                assert nodeRecord.inUse() : nodeRecord;
                // Ensure heavy so that the dynamic label records gets loaded (and then deleted) too
                nodeStore.ensureHeavy(nodeRecord, storeCursors);

                // Delete property records
                long nextProp = nodeRecord.getNextProp();
                while (!Record.NULL_REFERENCE.is(nextProp)) {
                    propertyStore.getRecordByCursor(
                            nextProp, propertyRecord, NORMAL, storeCursors.readCursor(PROPERTY_CURSOR));
                    assert propertyRecord.inUse() : propertyRecord + " for " + nodeRecord;
                    propertyStore.ensureHeavy(propertyRecord, storeCursors);
                    batchPropertiesRemoved += propertyRecord.numberOfProperties();
                    nextProp = propertyRecord.getNextProp();
                    deletePropertyRecordIncludingValueRecords(propertyRecord);
                    try (var propertyWriteCursor = storeCursors.writeCursor(PROPERTY_CURSOR)) {
                        propertyStore.updateRecord(
                                propertyRecord, idUpdates, propertyWriteCursor, cursorContext, storeCursors);
                    }
                }

                // Delete node (and dynamic label records, if any)
                nodeRecord.setInUse(false);
                for (DynamicRecord labelRecord : nodeRecord.getDynamicLabelRecords()) {
                    labelRecord.setInUse(false);
                }
                try (var nodeWriteCursor = storeCursors.writeCursor(NODE_CURSOR)) {
                    nodeStore.updateRecord(nodeRecord, idUpdates, nodeWriteCursor, cursorContext, storeCursors);
                }
            }
            propertiesRemoved.add(batchPropertiesRemoved);
            nodesRemoved.add(batch.length);
        }
    }

    private static void deletePropertyRecordIncludingValueRecords(PropertyRecord record) {
        for (PropertyBlock block : record) {
            for (DynamicRecord valueRecord : block.getValueRecords()) {
                assert valueRecord.inUse();
                valueRecord.setInUse(false);
                record.addDeletedRecord(valueRecord);
            }
        }
        record.clearPropertyBlocks();
        record.setInUse(false);
    }

    @Override
    public void close() throws Exception {
        super.close();
        storeMonitor.nodesRemoved(nodesRemoved.sum());
        storeMonitor.propertiesRemoved(propertiesRemoved.sum());
    }
}
