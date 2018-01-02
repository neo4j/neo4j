/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.AbstractRecordStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.state.PropertyCreator;
import org.neo4j.kernel.impl.util.ReusableIteratorCostume;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPropertyRecordAccess;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

import static java.lang.Math.max;

/**
 * Writes {@link RECORD entity batches} to the underlying stores. Also makes final composition of the
 * {@link Batch entities} before writing, such as clumping up {@link PropertyBlock properties} into
 * {@link PropertyRecord property records}.
 *
 * @param <RECORD> type of entities.
 * @param <INPUT> type of input.
 */
public class EntityStoreUpdaterStep<RECORD extends PrimitiveRecord,INPUT extends InputEntity>
        extends ProcessorStep<Batch<INPUT,RECORD>>
{
    public interface Monitor
    {
        void entitiesWritten( Class<? extends PrimitiveRecord> type, long count );

        void propertiesWritten( long count );
    }

    private final AbstractRecordStore<RECORD> entityStore;
    private final PropertyStore propertyStore;
    private final IoMonitor ioMonitor;
    private final PropertyCreator propertyCreator;
    private final Monitor monitor;

    // Reusable instances for less GC
    private final BatchingPropertyRecordAccess propertyRecords = new BatchingPropertyRecordAccess();
    private final ReusableIteratorCostume<PropertyBlock> blockIterator = new ReusableIteratorCostume<>();

    EntityStoreUpdaterStep( StageControl control, Configuration config,
            AbstractRecordStore<RECORD> entityStore,
            PropertyStore propertyStore, IoMonitor ioMonitor,
            Monitor monitor )
    {
        super( control, "v", config, 1, ioMonitor );
        this.entityStore = entityStore;
        this.propertyStore = propertyStore;
        this.monitor = monitor;
        this.propertyCreator = new PropertyCreator( propertyStore, null );
        this.ioMonitor = ioMonitor;
        this.ioMonitor.reset();
    }

    @Override
    protected void process( Batch<INPUT,RECORD> batch, BatchSender sender )
    {
        // Clear reused data structures
        propertyRecords.close();

        // Write the entity records, and at the same time allocate property records for its property blocks.
        long highestId = 0;
        RECORD[] records = batch.records;
        if ( records.length == 0 )
        {
            return;
        }

        int propertyBlockCursor = 0, skipped = 0;
        for ( int i = 0; i < records.length; i++ )
        {
            RECORD record = records[i];

            int propertyBlockCount = batch.propertyBlocksLengths[i];
            if ( record != null )
            {
                INPUT input = batch.input[i];
                if ( input.hasFirstPropertyId() )
                {
                    record.setNextProp( input.firstPropertyId() );
                }
                else
                {
                    if ( propertyBlockCount > 0 )
                    {
                        reassignDynamicRecordIds( batch.propertyBlocks, propertyBlockCursor, propertyBlockCount );
                        long firstProp = propertyCreator.createPropertyChain( record,
                                blockIterator.dressArray( batch.propertyBlocks, propertyBlockCursor, propertyBlockCount ),
                                propertyRecords );
                        record.setNextProp( firstProp );
                    }
                }
                highestId = max( highestId, record.getId() );
                entityStore.updateRecord( record );
            }
            else
            {   // Here we have a relationship that refers to missing nodes. It's within the tolerance levels
                // of number of bad relationships. Just don't import this relationship.
                skipped++;
            }
            propertyBlockCursor += propertyBlockCount;
        }
        entityStore.setHighestPossibleIdInUse( highestId );

        // Write all the created property records.
        for ( PropertyRecord propertyRecord : propertyRecords.records() )
        {
            propertyStore.updateRecord( propertyRecord );
        }

        monitor.entitiesWritten( records[0].getClass(), records.length-skipped );
        monitor.propertiesWritten( propertyBlockCursor );
    }

    private void reassignDynamicRecordIds( PropertyBlock[] blocks, int offset, int length )
    {
        // OK, so here we have property blocks, potentially referring to DynamicRecords. The DynamicRecords
        // have ids that we need to re-assign in here, because the ids are generated by multiple property encoders,
        // and so we let each one of the encoders generate their own bogus ids and we re-assign those ids here,
        // where we know we have a single thread doing this.
        for ( int i = 0; i < length; i++ )
        {
            PropertyBlock block = blocks[offset+i];
            PropertyType type = block.getType();
            switch ( type )
            {
            case STRING:
                reassignDynamicRecordIds( block, type, propertyStore.getStringStore() );
                break;
            case ARRAY:
                reassignDynamicRecordIds( block, type, propertyStore.getArrayStore() );
                break;
            default: // No need to do anything be default, we only need to relink for dynamic records
            }
        }
    }

    private void reassignDynamicRecordIds( PropertyBlock block, PropertyType type, AbstractDynamicStore store )
    {
        Iterator<DynamicRecord> dynamicRecords = block.getValueRecords().iterator();
        long newId = store.nextId();
        block.getValueBlocks()[0] = PropertyStore.singleBlockLongValue( block.getKeyIndexId(), type, newId );
        while ( dynamicRecords.hasNext() )
        {
            DynamicRecord dynamicRecord = dynamicRecords.next();
            dynamicRecord.setId( newId );
            if ( dynamicRecords.hasNext() )
            {
                dynamicRecord.setNextBlock( newId = store.nextId() );
            }
        }
    }

    @Override
    protected void done()
    {
        super.done();
        // Stop the I/O monitor, since the stats in there is based on time passed since the start
        // and bytes written. NodeStage and CalculateDenseNodesStage can be run in parallel so if
        // NodeStage completes before CalculateDenseNodesStage then we want to stop the time in the I/O monitor.
        ioMonitor.stop();
    }
}
