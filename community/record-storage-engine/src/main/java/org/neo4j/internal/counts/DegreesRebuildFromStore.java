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
package org.neo4j.internal.counts;

import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;

/**
 * Scans the store and rebuilds the {@link GBPTreeRelationshipGroupDegreesStore} contents if the file is missing.
 */
class DegreesRebuildFromStore implements GBPTreeRelationshipGroupDegreesStore.DegreesRebuilder
{
    private final NeoStores neoStores;

    DegreesRebuildFromStore( NeoStores neoStores )
    {
        this.neoStores = neoStores;
    }

    @Override
    public long lastCommittedTxId()
    {
        return neoStores.getMetaDataStore().getLastCommittedTransactionId();
    }

    @Override
    public void rebuild( RelationshipGroupDegreesStore.Updater updater, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        // === sketch of a more performant version
        // - Read all groups and for every group make a mark in a memory-structure like so:
        //    (node) -> (type,active directions,out,in,loop)
        // - Read all relationships and for every matching node go into the memory-structure
        //    if it's there then look up the type
        //      if it's there then see if the direction matches any active direction
        //        if it does then increment the counter in the correct slot
        // - Go though the memory-structure and write to the updater
        //
        // If not all data can fit in memory then do multiple passes (node id range)

        RelationshipGroupStore groupStore = neoStores.getRelationshipGroupStore();
        try ( RecordStorageReader storageReader = new RecordStorageReader( neoStores );
                StorageRelationshipTraversalCursor traversalCursor = storageReader.allocateRelationshipTraversalCursor( cursorTracer );
                PageCursor groupCursor = groupStore.openPageCursorForReadingWithPrefetching( 0, cursorTracer ) )
        {
            RelationshipGroupRecord groupRecord = groupStore.newRecord();
            long highGroupId = groupStore.getHighId();
            for ( long id = groupStore.getNumberOfReservedLowIds(); id < highGroupId; id++ )
            {
                groupStore.getRecordByCursor( id, groupRecord, RecordLoad.LENIENT_NORMAL, groupCursor );
                if ( groupRecord.inUse() &&
                        (groupRecord.hasExternalDegreesOut() || groupRecord.hasExternalDegreesIn() || groupRecord.hasExternalDegreesLoop()) )
                {
                    updateDegree( groupRecord.hasExternalDegreesOut(), groupRecord.getFirstOut(), groupRecord, OUTGOING, traversalCursor, updater );
                    updateDegree( groupRecord.hasExternalDegreesIn(), groupRecord.getFirstIn(), groupRecord, INCOMING, traversalCursor, updater );
                    updateDegree( groupRecord.hasExternalDegreesLoop(), groupRecord.getFirstLoop(), groupRecord, LOOP, traversalCursor, updater );
                }
            }
        }
    }

    private void updateDegree( boolean hasExternalDegrees, long firstRel, RelationshipGroupRecord groupRecord, RelationshipDirection direction,
            StorageRelationshipTraversalCursor traversalCursor, RelationshipGroupDegreesStore.Updater updater )
    {
        if ( !hasExternalDegrees )
        {
            return;
        }

        traversalCursor.init( groupRecord.getOwningNode(), firstRel, RelationshipSelection.ALL_RELATIONSHIPS );
        long degree = 0;
        while ( traversalCursor.next() )
        {
            degree++;
        }
        updater.increment( groupRecord.getId(), direction, degree );
    }
}
