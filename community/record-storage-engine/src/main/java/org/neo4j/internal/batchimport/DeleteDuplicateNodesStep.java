/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport;

import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.internal.batchimport.staging.LonelyProcessingStep;
import org.neo4j.internal.batchimport.staging.StageControl;
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

public class DeleteDuplicateNodesStep extends LonelyProcessingStep {
    private static final String DELETE_DUPLICATE_IMPORT_STEP_TAG = "deleteDuplicateImportStep";
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;
    private final LongIterator nodeIds;
    private final DataImporter.Monitor storeMonitor;
    private final CursorContextFactory contextFactory;
    private final NeoStores neoStores;

    private long nodesRemoved;
    private long propertiesRemoved;

    public DeleteDuplicateNodesStep(
            StageControl control,
            Configuration config,
            LongIterator nodeIds,
            NeoStores neoStores,
            DataImporter.Monitor storeMonitor,
            CursorContextFactory contextFactory) {
        super(control, "DEDUP", config);
        this.neoStores = neoStores;
        this.nodeStore = neoStores.getNodeStore();
        this.propertyStore = neoStores.getPropertyStore();
        this.nodeIds = nodeIds;
        this.storeMonitor = storeMonitor;
        this.contextFactory = contextFactory;
    }

    @Override
    protected void process() {
        NodeRecord nodeRecord = nodeStore.newRecord();
        PropertyRecord propertyRecord = propertyStore.newRecord();
        try (var cursorContext = contextFactory.create(DELETE_DUPLICATE_IMPORT_STEP_TAG);
                var storeCursors = new CachedStoreCursors(neoStores, cursorContext)) {
            while (nodeIds.hasNext()) {
                long duplicateNodeId = nodeIds.next();
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
                    propertiesRemoved += propertyRecord.numberOfProperties();
                    nextProp = propertyRecord.getNextProp();
                    deletePropertyRecordIncludingValueRecords(propertyRecord);
                    try (var propertyWriteCursor = storeCursors.writeCursor(PROPERTY_CURSOR)) {
                        propertyStore.updateRecord(propertyRecord, propertyWriteCursor, cursorContext, storeCursors);
                    }
                }

                // Delete node (and dynamic label records, if any)
                nodeRecord.setInUse(false);
                for (DynamicRecord labelRecord : nodeRecord.getDynamicLabelRecords()) {
                    labelRecord.setInUse(false);
                }
                try (var nodeWriteCursor = storeCursors.writeCursor(NODE_CURSOR)) {
                    nodeStore.updateRecord(nodeRecord, nodeWriteCursor, cursorContext, storeCursors);
                }
                nodesRemoved++;
            }
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
        storeMonitor.nodesRemoved(nodesRemoved);
        storeMonitor.propertiesRemoved(propertiesRemoved);
    }
}
