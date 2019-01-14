/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.eclipse.collections.api.iterator.LongIterator;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.unsafe.impl.batchimport.staging.LonelyProcessingStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.PropertyDeleter.deletePropertyRecordIncludingValueRecords;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class DeleteDuplicateNodesStep extends LonelyProcessingStep
{
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;
    private final LongIterator nodeIds;
    private final DataImporter.Monitor storeMonitor;

    private long nodesRemoved;
    private long propertiesRemoved;

    public DeleteDuplicateNodesStep( StageControl control, Configuration config, LongIterator nodeIds, NodeStore nodeStore,
            PropertyStore propertyStore, DataImporter.Monitor storeMonitor )
    {
        super( control, "DEDUP", config );
        this.nodeStore = nodeStore;
        this.propertyStore = propertyStore;
        this.nodeIds = nodeIds;
        this.storeMonitor = storeMonitor;
    }

    @Override
    protected void process()
    {
        NodeRecord nodeRecord = nodeStore.newRecord();
        PropertyRecord propertyRecord = propertyStore.newRecord();
        try ( PageCursor cursor = nodeStore.openPageCursorForReading( 0 );
              PageCursor propertyCursor = propertyStore.openPageCursorForReading( 0 ) )
        {
            while ( nodeIds.hasNext() )
            {
                long duplicateNodeId = nodeIds.next();
                nodeStore.getRecordByCursor( duplicateNodeId, nodeRecord, NORMAL, cursor );
                assert nodeRecord.inUse() : nodeRecord;
                // Ensure heavy so that the dynamic label records gets loaded (and then deleted) too
                nodeStore.ensureHeavy( nodeRecord );

                // Delete property records
                long nextProp = nodeRecord.getNextProp();
                while ( !Record.NULL_REFERENCE.is( nextProp ) )
                {
                    propertyStore.getRecordByCursor( nextProp, propertyRecord, NORMAL, propertyCursor );
                    assert propertyRecord.inUse() : propertyRecord + " for " + nodeRecord;
                    propertyStore.ensureHeavy( propertyRecord );
                    propertiesRemoved += propertyRecord.numberOfProperties();
                    nextProp = propertyRecord.getNextProp();
                    deletePropertyRecordIncludingValueRecords( propertyRecord );
                    propertyStore.updateRecord( propertyRecord );
                }

                // Delete node (and dynamic label records, if any)
                nodeRecord.setInUse( false );
                for ( DynamicRecord labelRecord : nodeRecord.getDynamicLabelRecords() )
                {
                    labelRecord.setInUse( false );
                }
                nodeStore.updateRecord( nodeRecord );
                nodesRemoved++;
            }
        }
    }

    @Override
    public void close() throws Exception
    {
        super.close();
        storeMonitor.nodesRemoved( nodesRemoved );
        storeMonitor.propertiesRemoved( propertiesRemoved );
    }
}
