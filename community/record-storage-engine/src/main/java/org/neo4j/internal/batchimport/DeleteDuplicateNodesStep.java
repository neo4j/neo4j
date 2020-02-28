/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.eclipse.collections.api.iterator.LongIterator;

import org.neo4j.internal.batchimport.staging.LonelyProcessingStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;

import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class DeleteDuplicateNodesStep extends LonelyProcessingStep
{
    private static final String DELETE_DUPLICATE_IMPORT_STEP_TAG = "deleteDuplicateImportStep";
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;
    private final LongIterator nodeIds;
    private final DataImporter.Monitor storeMonitor;
    private final PageCacheTracer pageCacheTracer;

    private long nodesRemoved;
    private long propertiesRemoved;

    public DeleteDuplicateNodesStep( StageControl control, Configuration config, LongIterator nodeIds, NodeStore nodeStore,
            PropertyStore propertyStore, DataImporter.Monitor storeMonitor, PageCacheTracer pageCacheTracer )
    {
        super( control, "DEDUP", config );
        this.nodeStore = nodeStore;
        this.propertyStore = propertyStore;
        this.nodeIds = nodeIds;
        this.storeMonitor = storeMonitor;
        this.pageCacheTracer = pageCacheTracer;
    }

    @Override
    protected void process()
    {
        NodeRecord nodeRecord = nodeStore.newRecord();
        PropertyRecord propertyRecord = propertyStore.newRecord();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( DELETE_DUPLICATE_IMPORT_STEP_TAG );
              PageCursor cursor = nodeStore.openPageCursorForReading( 0, cursorTracer );
              PageCursor propertyCursor = propertyStore.openPageCursorForReading( 0, cursorTracer ) )
        {
            while ( nodeIds.hasNext() )
            {
                long duplicateNodeId = nodeIds.next();
                nodeStore.getRecordByCursor( duplicateNodeId, nodeRecord, NORMAL, cursor );
                assert nodeRecord.inUse() : nodeRecord;
                // Ensure heavy so that the dynamic label records gets loaded (and then deleted) too
                nodeStore.ensureHeavy( nodeRecord, cursorTracer );

                // Delete property records
                long nextProp = nodeRecord.getNextProp();
                while ( !Record.NULL_REFERENCE.is( nextProp ) )
                {
                    propertyStore.getRecordByCursor( nextProp, propertyRecord, NORMAL, propertyCursor );
                    assert propertyRecord.inUse() : propertyRecord + " for " + nodeRecord;
                    propertyStore.ensureHeavy( propertyRecord, cursorTracer );
                    propertiesRemoved += propertyRecord.numberOfProperties();
                    nextProp = propertyRecord.getNextProp();
                    deletePropertyRecordIncludingValueRecords( propertyRecord );
                    propertyStore.updateRecord( propertyRecord, cursorTracer );
                }

                // Delete node (and dynamic label records, if any)
                nodeRecord.setInUse( false );
                for ( DynamicRecord labelRecord : nodeRecord.getDynamicLabelRecords() )
                {
                    labelRecord.setInUse( false );
                }
                nodeStore.updateRecord( nodeRecord, cursorTracer );
                nodesRemoved++;
            }
        }
    }

    private static void deletePropertyRecordIncludingValueRecords( PropertyRecord record )
    {
        for ( PropertyBlock block : record )
        {
            for ( DynamicRecord valueRecord : block.getValueRecords() )
            {
                assert valueRecord.inUse();
                valueRecord.setInUse( false );
                record.addDeletedRecord( valueRecord );
            }
        }
        record.clearPropertyBlocks();
        record.setInUse( false );
    }

    @Override
    public void close() throws Exception
    {
        super.close();
        storeMonitor.nodesRemoved( nodesRemoved );
        storeMonitor.propertiesRemoved( propertiesRemoved );
    }
}
