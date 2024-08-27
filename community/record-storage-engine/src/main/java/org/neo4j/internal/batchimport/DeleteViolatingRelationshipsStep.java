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

import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.staging.LonelyProcessingStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;

public class DeleteViolatingRelationshipsStep extends LonelyProcessingStep {
    private static final String DELETE_VIOLATING_IMPORT_STEP_TAG = "deleteViolatingRelationshipsImportStep";
    private final RelationshipStore relStore;
    private final PropertyStore propertyStore;
    private final LongIterator relIds;
    private final DataImporter.Monitor storeMonitor;
    private final DataStatistics.Client client;
    private final CursorContextFactory contextFactory;
    private final NeoStores neoStores;

    private long relsRemoved;
    private long propertiesRemoved;

    public DeleteViolatingRelationshipsStep(
            StageControl control,
            Configuration config,
            LongIterator relIds,
            NeoStores neoStores,
            DataImporter.Monitor storeMonitor,
            DataStatistics.Client client,
            CursorContextFactory contextFactory) {
        super(control, "DELETE", config);
        this.neoStores = neoStores;
        this.relStore = neoStores.getRelationshipStore();
        this.propertyStore = neoStores.getPropertyStore();
        this.relIds = relIds;
        this.storeMonitor = storeMonitor;
        this.client = client;
        this.contextFactory = contextFactory;
    }

    @Override
    protected void process() {
        RelationshipRecord relRecord = relStore.newRecord();
        PropertyRecord propertyRecord = propertyStore.newRecord();
        try (var cursorContext = contextFactory.create(DELETE_VIOLATING_IMPORT_STEP_TAG);
                var storeCursors = new CachedStoreCursors(neoStores, cursorContext);
                MemoryTracker memoryTracker = EmptyMemoryTracker.INSTANCE) {
            while (relIds.hasNext()) {
                long violatingRelId = relIds.next();

                relStore.getRecordByCursor(
                        violatingRelId, relRecord, NORMAL, storeCursors.readCursor(RELATIONSHIP_CURSOR), memoryTracker);
                assert relRecord.inUse() : relRecord;

                // Delete property records
                long nextProp = relRecord.getNextProp();
                while (!Record.NULL_REFERENCE.is(nextProp)) {
                    propertyStore.getRecordByCursor(
                            nextProp, propertyRecord, NORMAL, storeCursors.readCursor(PROPERTY_CURSOR), memoryTracker);
                    assert propertyRecord.inUse() : propertyRecord + " for " + relRecord;
                    propertyStore.ensureHeavy(propertyRecord, storeCursors, memoryTracker);
                    propertiesRemoved += propertyRecord.numberOfProperties();
                    nextProp = propertyRecord.getNextProp();
                    deletePropertyRecordIncludingValueRecords(propertyRecord);
                    try (var propertyWriteCursor = storeCursors.writeCursor(PROPERTY_CURSOR)) {
                        propertyStore.updateRecord(propertyRecord, propertyWriteCursor, cursorContext, storeCursors);
                    }
                }

                // Delete relationship (the relationship should not have been linked to anything yet)
                relRecord.setInUse(false);
                // Update the typeDistribution so that is correct in the linking stage later
                client.increment(relRecord.getType(), -1);

                try (var relWriteCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
                    relStore.updateRecord(relRecord, relWriteCursor, cursorContext, storeCursors);
                }
                relsRemoved++;
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
        storeMonitor.relationshipsRemoved(relsRemoved);
        storeMonitor.propertiesRemoved(propertiesRemoved);
    }
}
