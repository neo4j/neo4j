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

import java.util.Iterator;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.id.IdRangeIterator;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.state.PropertyCreator;
import org.neo4j.kernel.impl.util.ReusableIteratorCostume;
import org.neo4j.kernel.impl.util.collection.ArrayCollection;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingIdSequence;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPropertyRecordAccess;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;

import static java.lang.Math.toIntExact;

/**
 * Encodes property data into {@link PropertyRecord property records}, attaching them to each
 * {@link Batch}. This step is designed to handle multiple threads doing the property encoding,
 * since property encoding is potentially the most costly step in this {@link Stage}.
 */
public class PropertyEncoderStep<RECORD extends PrimitiveRecord,INPUT extends InputEntity>
        extends ProcessorStep<Batch<INPUT,RECORD>>
{
    private final BatchingPropertyKeyTokenRepository propertyKeyHolder;
    private final int arrayDataSize;
    private final int stringDataSize;
    private final PropertyStore propertyStore;

    protected PropertyEncoderStep( StageControl control, Configuration config,
            BatchingPropertyKeyTokenRepository propertyKeyHolder, PropertyStore propertyStore )
    {
        super( control, "PROPERTIES", config, 0 );
        this.propertyKeyHolder = propertyKeyHolder;
        this.propertyStore = propertyStore;
        this.arrayDataSize = propertyStore.getArrayStore().getRecordDataSize();
        this.stringDataSize = propertyStore.getStringStore().getRecordDataSize();
    }

    @Override
    protected void process( Batch<INPUT,RECORD> batch, BatchSender sender )
    {
        RelativeIdRecordAllocator stringAllocator = new RelativeIdRecordAllocator( stringDataSize );
        RelativeIdRecordAllocator arrayAllocator = new RelativeIdRecordAllocator( arrayDataSize );
        IdSequence relativePropertyRecordIds = new BatchingIdSequence();
        PropertyCreator propertyCreator = new PropertyCreator( stringAllocator, arrayAllocator,
                relativePropertyRecordIds, null );
        ArrayCollection<PropertyRecord> propertyRecordCollection = new ArrayCollection<>( 4 );
        BatchingPropertyRecordAccess propertyRecords = new BatchingPropertyRecordAccess();
        ReusableIteratorCostume<PropertyBlock> blockIterator = new ReusableIteratorCostume<>();

        batch.propertyRecords = new PropertyRecord[batch.input.length][];
        int totalNumberOfProperties = 0;
        int totalNumberOfPropertyRecords = 0;
        for ( int i = 0; i < batch.input.length; i++ )
        {
            INPUT input = batch.input[i];
            if ( !input.hasFirstPropertyId() )
            {   // Encode the properties and attach the blocks to the Batch instance.
                // Dynamic records for each entity will start from 0, they will be reassigned later anyway
                int count = input.properties().length >> 1;
                if ( count > 0 )
                {
                    PropertyBlock[] propertyBlocks = new PropertyBlock[count];
                    propertyKeyHolder.propertyKeysAndValues( propertyBlocks, 0, input.properties(), propertyCreator );

                    // Create the property records with local ids, they will have to be reassigned to real ids later
                    propertyCreator.createPropertyChain( null, // owner assigned in a later step
                            blockIterator.dressArray( propertyBlocks, 0, count ),
                            propertyRecords, propertyRecordCollection::add );
                    batch.propertyRecords[i] = propertyRecordCollection.toArray(
                            new PropertyRecord[propertyRecordCollection.size()] );
                    totalNumberOfPropertyRecords += propertyRecordCollection.size();
                    totalNumberOfProperties += count;
                    propertyRecordCollection.clear();
                }
            }
        }

        // Enter a synchronized block which assigns id ranges
        IdRangeIterator propertyRecordsIdRange;
        IdRangeIterator dynamicStringRecordsIdRange;
        IdRangeIterator dynamicArrayRecordsIdRange;
        synchronized ( propertyStore )
        {
            propertyRecordsIdRange = idRange( totalNumberOfPropertyRecords, propertyStore );
            dynamicStringRecordsIdRange = idRange( toIntExact( stringAllocator.peek() ),
                    propertyStore.getStringStore() );
            dynamicArrayRecordsIdRange = idRange( toIntExact( arrayAllocator.peek() ),
                    propertyStore.getArrayStore() );
        }

        // Do reassignment of ids here
        for ( int i = 0; i < batch.input.length; i++ )
        {
            INPUT input = batch.input[i];
            RECORD record = batch.records[i];
            if ( record != null )
            {
                reassignPropertyIds( input, record, batch.propertyRecords[i],
                        propertyRecordsIdRange, dynamicStringRecordsIdRange, dynamicArrayRecordsIdRange );
            }
        }

        // Assigned so that next single-threaded step can assign id ranges quickly
        batch.numberOfProperties = totalNumberOfProperties;
        sender.send( batch );
    }

    private static IdRangeIterator idRange( int size, IdSequence idSource )
    {
        return size > 0 ? idSource.nextIdBatch( size ).iterator() : IdRangeIterator.EMPTY_ID_RANGE_ITERATOR;
    }

    private static void reassignPropertyIds( InputEntity input, PrimitiveRecord record, PropertyRecord[] propertyRecords,
            IdRangeIterator propertyRecordsIdRange,
            IdRangeIterator dynamicStringRecordsIdRange,
            IdRangeIterator dynamicArrayRecordsIdRange )
    {
        if ( input.hasFirstPropertyId() )
        {
            record.setNextProp( input.firstPropertyId() );
        }
        else
        {
            if ( propertyRecords != null )
            {
                reassignDynamicRecordIds( dynamicStringRecordsIdRange, dynamicArrayRecordsIdRange, propertyRecords );
                long firstProp = reassignPropertyRecordIds( record, propertyRecordsIdRange, propertyRecords );
                record.setNextProp( firstProp );
            }
        }
    }

    private static long reassignPropertyRecordIds( PrimitiveRecord record, IdRangeIterator ids,
            PropertyRecord[] propertyRecords )
    {
        long newId = ids.nextId();
        long firstId = newId;
        PropertyRecord prev = null;
        for ( PropertyRecord propertyRecord : propertyRecords )
        {
            record.setIdTo( propertyRecord );
            propertyRecord.setId( newId );
            if ( !Record.NO_NEXT_PROPERTY.is( propertyRecord.getNextProp() ) )
            {
                propertyRecord.setNextProp( newId = ids.nextId() );
            }
            if ( prev != null )
            {
                propertyRecord.setPrevProp( prev.getId() );
            }
            prev = propertyRecord;
        }
        return firstId;
    }

    private static void reassignDynamicRecordIds( IdRangeIterator stringRecordsIds, IdRangeIterator arrayRecordsIds,
            PropertyRecord[] propertyRecords )
    {
        // OK, so here we have property blocks, potentially referring to DynamicRecords. The DynamicRecords
        // have ids that we need to re-assign in here, because the ids are generated by multiple property encoders,
        // and so we let each one of the encoders generate their own bogus ids and we re-assign those ids here,
        // where we know we have a single thread doing this.
        for ( PropertyRecord propertyRecord : propertyRecords )
        {
            for ( PropertyBlock block : propertyRecord )
            {
                PropertyType type = block.getType();
                switch ( type )
                {
                case STRING:
                    reassignDynamicRecordIds( block, type, stringRecordsIds );
                    break;
                case ARRAY:
                    reassignDynamicRecordIds( block, type, arrayRecordsIds );
                    break;
                default: // No need to do anything be default, we only need to relink for dynamic records
                }
            }
        }
    }

    private static void reassignDynamicRecordIds( PropertyBlock block, PropertyType type, IdRangeIterator ids )
    {
        Iterator<DynamicRecord> dynamicRecords = block.getValueRecords().iterator();
        long newId = ids.nextId();
        block.getValueBlocks()[0] = PropertyStore.singleBlockLongValue( block.getKeyIndexId(), type, newId );
        while ( dynamicRecords.hasNext() )
        {
            DynamicRecord dynamicRecord = dynamicRecords.next();
            dynamicRecord.setId( newId );
            if ( dynamicRecords.hasNext() )
            {
                dynamicRecord.setNextBlock( newId = ids.nextId() );
            }
        }
    }
}
