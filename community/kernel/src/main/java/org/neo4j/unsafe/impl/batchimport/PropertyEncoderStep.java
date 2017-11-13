/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StandardDynamicRecordAllocator;
import org.neo4j.kernel.impl.store.id.RenewableBatchIdSequence;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.state.PropertyCreator;
import org.neo4j.kernel.impl.util.ReusableIteratorCostume;
import org.neo4j.kernel.impl.util.collection.ArrayCollection;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPropertyRecordAccess;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;

/**
 * Encodes property data into {@link PropertyRecord property records}, attaching them to each
 * {@link Batch}. This step is designed to handle multiple threads doing the property encoding,
 * since property encoding is potentially the most costly step in this {@link Stage}.
 */
public class PropertyEncoderStep<RECORD extends PrimitiveRecord,INPUT extends InputEntity>
        extends ProcessorStep<Batch<INPUT,RECORD>>
{
    private final BatchingPropertyKeyTokenRepository propertyKeyHolder;
    private final ThreadLocal<IdBatches> ids;
    private final PropertyStore propertyStore;

    protected PropertyEncoderStep( StageControl control, Configuration config,
            BatchingPropertyKeyTokenRepository propertyKeyHolder, PropertyStore propertyStore )
    {
        super( control, "PROPERTIES", config, 0 );
        this.propertyKeyHolder = propertyKeyHolder;
        this.propertyStore = propertyStore;
        this.ids = new ThreadLocal<IdBatches>()
        {
            @Override
            protected IdBatches initialValue()
            {
                return new IdBatches( propertyStore );
            }
        };
    }

    @Override
    protected void process( Batch<INPUT,RECORD> batch, BatchSender sender )
    {
        IdBatches threadIds = ids.get();
        PropertyCreator propertyCreator = new PropertyCreator( threadIds.stringIds, threadIds.arrayIds, threadIds.propertyIds, null,
                propertyStore.allowStorePoints() );
        ArrayCollection<PropertyRecord> propertyRecordCollection = new ArrayCollection<>( 4 );
        BatchingPropertyRecordAccess propertyRecords = new BatchingPropertyRecordAccess();
        ReusableIteratorCostume<PropertyBlock> blockIterator = new ReusableIteratorCostume<>();

        batch.propertyRecords = new PropertyRecord[batch.input.length][];
        int totalNumberOfProperties = 0;
        for ( int i = 0; i < batch.input.length; i++ )
        {
            INPUT input = batch.input[i];
            if ( !input.hasFirstPropertyId() )
            {   // Encode the properties and attach the blocks to the Batch instance.
                // Dynamic records for each entity will start from 0, they will be reassigned later anyway
                int count = input.propertyCount();
                if ( count > 0 )
                {
                    PropertyBlock[] propertyBlocks = new PropertyBlock[count];
                    propertyKeyHolder.propertyKeysAndValues( propertyBlocks, 0, input.properties(), propertyCreator );
                    propertyCreator.createPropertyChain( null, // owner assigned in a later step
                            blockIterator.dressArray( propertyBlocks, 0, count ),
                            propertyRecords, propertyRecordCollection::add );
                    batch.propertyRecords[i] = propertyRecordCollection.toArray(
                            new PropertyRecord[propertyRecordCollection.size()] );
                    batch.records[i].setNextProp( batch.propertyRecords[i][0].getId() );
                    batch.records[i].setIdTo( batch.propertyRecords[i][0] );
                    totalNumberOfProperties += count;
                    propertyRecordCollection.clear();
                }
            }
        }

        batch.numberOfProperties = totalNumberOfProperties;
        sender.send( batch );
    }

    private static class IdBatches
    {
        final RenewableBatchIdSequence propertyIds;
        final DynamicRecordAllocator stringIds;
        final DynamicRecordAllocator arrayIds;

        IdBatches( PropertyStore propertyStore )
        {
            this.propertyIds = new RenewableBatchIdSequence( propertyStore, propertyStore.getRecordsPerPage(), id -> {} );
            this.stringIds = new StandardDynamicRecordAllocator(
                    new RenewableBatchIdSequence( propertyStore.getStringStore(),
                            propertyStore.getStringStore().getRecordsPerPage(), id -> {} ),
                    propertyStore.getStringStore().getRecordDataSize() );
            this.arrayIds = new StandardDynamicRecordAllocator(
                    new RenewableBatchIdSequence( propertyStore.getArrayStore(),
                            propertyStore.getArrayStore().getRecordsPerPage(), id -> {} ),
                    propertyStore.getArrayStore().getRecordDataSize() );
        }
    }
}
