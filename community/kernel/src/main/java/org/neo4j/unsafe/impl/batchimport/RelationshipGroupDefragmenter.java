/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.cache.ByteArray;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;

import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors.superviseExecution;

/**
 * Defragments {@link RelationshipGroupRecord} so that they end up sequential per node in the group store.
 * There's one constraint which is assumed to be true here: any relationship group that we see in the store
 * for any given {@link RelationshipGroupRecord#getOwningNode() owner} must have a lower
 * {@link RelationshipGroupRecord#getType() type} than any previous group encountered for that node,
 * i.e. all {@link RelationshipGroupRecord#getNext() next pointers} must be either
 * {@link Record#NO_NEXT_RELATIONSHIP NULL} or lower than the group id at hand. When this is true,
 * and the defragmenter verifies this constraint, the groups will be reversed so that types are instead
 * ascending and groups are always co-located.
 */
public class RelationshipGroupDefragmenter
{
    private final Configuration config;
    private final ExecutionMonitor executionMonitor;

    public RelationshipGroupDefragmenter( Configuration config, ExecutionMonitor executionMonitor )
    {
        this.config = config;
        this.executionMonitor = executionMonitor;
    }

    public void run( long memoryWeCanHoldForCertain, BatchingNeoStores neoStore, long highNodeId )
    {
        try ( RelationshipGroupCache groupCache =
                new RelationshipGroupCache( AUTO, memoryWeCanHoldForCertain, highNodeId ) )
        {
            try
            {
                // Read from the temporary relationship group store...
                RecordStore<RelationshipGroupRecord> fromStore = neoStore.getTemporaryRelationshipGroupStore();
                // and write into the main relationship group store
                RecordStore<RelationshipGroupRecord> toStore = neoStore.getRelationshipGroupStore();

                // Count all nodes, how many groups each node has each
                executeStage( new CountGroupsStage( config, fromStore, groupCache ) );
                long fromNodeId = 0;
                long toNodeId = 0;
                while ( fromNodeId < highNodeId )
                {
                    // See how many nodes' groups we can fit into the cache this iteration of the loop.
                    // Groups that doesn't fit in this round will be included in consecutive rounds.
                    toNodeId = groupCache.prepare( fromNodeId );
                    // Cache those groups
                    executeStage( new ScanAndCacheGroupsStage( config, fromStore, groupCache ) );
                    // And write them in sequential order in the store
                    executeStage( new WriteGroupsStage( config, groupCache, toStore ) );

                    // Make adjustments for the next iteration
                    fromNodeId = toNodeId;
                }

                // Now update nodes to point to the new groups
                ByteArray groupCountCache = groupCache.getGroupCountCache();
                groupCountCache.clear();
                executeStage( new NodeFirstGroupStage( config, toStore, neoStore.getNodeStore(), groupCountCache ) );
            }
            catch ( Throwable t )
            {
                t.printStackTrace();
                throw t;
            }
        }
    }

    private void executeStage( Stage stage )
    {
        superviseExecution( executionMonitor, config, stage );
    }
}
