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

import java.util.function.ToIntFunction;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.state.PropertyCreator;
import org.neo4j.kernel.impl.transaction.state.PropertyTraverser;
import org.neo4j.kernel.impl.transaction.state.RelationshipCreator;
import org.neo4j.kernel.impl.transaction.state.RelationshipGroupGetter;
import org.neo4j.kernel.impl.util.ReusableIteratorCostume;
import org.neo4j.unsafe.batchinsert.DirectRecordAccessSet;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingIdSequence;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;

import static org.neo4j.unsafe.impl.batchimport.EntityStoreUpdaterStep.reassignDynamicRecordIds;

public class BatchInsertRelationshipsStep extends ProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    private final ToIntFunction<Object> typeToId;
    private final RelationshipCreator relationshipCreator;
    private final Locks.Client noopLockClient = new NoOpClient();
    private final PropertyCreator propertyCreator;
    private final DirectRecordAccessSet recordAccess;
    private final PropertyStore propertyStore;
    private int pendingRelationshipChanges;

    // Reusable instances for less GC
    private final ReusableIteratorCostume<PropertyBlock> blockIterator = new ReusableIteratorCostume<>();
    private final IdSequence relationshipIdGenerator;

    public BatchInsertRelationshipsStep( StageControl control, Configuration config, BatchingNeoStores store,
            ToIntFunction<Object> typeToId, long nextRelationshipId )
    {
        super( control, "INSERT", config, 1 );
        this.typeToId = typeToId;
        RecordStore<RelationshipGroupRecord> relationshipGroupStore = store.getTemporaryRelationshipGroupStore();
        RelationshipGroupGetter groupGetter = new RelationshipGroupGetter( relationshipGroupStore );
        this.relationshipCreator = new RelationshipCreator( groupGetter, config.denseNodeThreshold() );
        PropertyTraverser propertyTraverser = new PropertyTraverser();
        this.propertyCreator = new PropertyCreator( store.getPropertyStore(), propertyTraverser );
        this.propertyStore = store.getPropertyStore();
        this.recordAccess = new DirectRecordAccessSet( store.getNodeStore(), propertyStore,
                store.getRelationshipStore(), relationshipGroupStore,
                store.getNeoStores().getPropertyKeyTokenStore(),
                store.getNeoStores().getRelationshipTypeTokenStore(),
                store.getNeoStores().getLabelTokenStore(), store.getNeoStores().getSchemaStore() );
        this.relationshipIdGenerator = new BatchingIdSequence( nextRelationshipId );
    }

    @Override
    protected void process( Batch<InputRelationship,RelationshipRecord> batch, BatchSender sender ) throws Throwable
    {
        for ( int i = 0, propertyBlockCursor = 0; i < batch.input.length; i++ )
        {
            InputRelationship input = batch.input[i];
            int propertyBlockCount = batch.propertyBlocksLengths[i];

            // Create relationship
            long startNodeId = batch.ids[i*2];
            long endNodeId = batch.ids[i*2+1];
            if ( startNodeId != -1 && endNodeId != -1 )
            {
                long id = relationshipIdGenerator.nextId();
                int typeId = typeToId.applyAsInt( input.typeAsObject() );
                relationshipCreator.relationshipCreate( id, typeId, startNodeId, endNodeId, recordAccess, noopLockClient );

                // Set properties
                RelationshipRecord record = recordAccess.getRelRecords().getOrLoad( id, null ).forChangingData();
                if ( input.hasFirstPropertyId() )
                {
                    record.setNextProp( input.firstPropertyId() );
                }
                else
                {
                    if ( propertyBlockCount > 0 )
                    {
                        reassignDynamicRecordIds( propertyStore, batch.propertyBlocks,
                                propertyBlockCursor, propertyBlockCount );
                        long firstProp = propertyCreator.createPropertyChain( record,
                                blockIterator.dressArray( batch.propertyBlocks, propertyBlockCursor, propertyBlockCount ),
                                recordAccess.getPropertyRecords() );
                        record.setNextProp( firstProp );
                    }
                }
            }
            // else --> This is commonly known as input relationship referring to missing node IDs
            propertyBlockCursor += propertyBlockCount;
        }

        pendingRelationshipChanges += batch.input.length;
        if ( pendingRelationshipChanges >= 50_000 )
        {
            recordAccess.close(); // <-- happens to be called close even though this impl just flushes
            pendingRelationshipChanges = 0;
        }
    }

    @Override
    protected void done()
    {
        recordAccess.close();
        super.done();
    }
}
