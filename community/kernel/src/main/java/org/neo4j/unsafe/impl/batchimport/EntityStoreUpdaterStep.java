/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

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
import org.neo4j.unsafe.impl.batchimport.staging.ExecutorServiceStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.WriterFactory;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPropertyRecordAccess;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

import static java.lang.Math.max;

/**
 * Writes {@link RecordBatch entity batches} to the underlying stores. Also makes final composition of the
 * {@link BatchEntity entities} before writing, such as clumping up {@link PropertyBlock properties} into
 * {@link PropertyRecord property records}.
 *
 * @param <RECORD> type of entities.
 * @param <INPUT> type of input.
 */
public class EntityStoreUpdaterStep<RECORD extends PrimitiveRecord,INPUT extends InputEntity>
        extends ExecutorServiceStep<List<BatchEntity<RECORD,INPUT>>>
{
    private final AbstractRecordStore<RECORD> entityStore;
    private final PropertyStore propertyStore;
    private final IoMonitor monitor;
    private final WriterFactory writerFactory;
    private final PropertyCreator propertyCreator;

    // Reusable instances for less GC
    private final BatchingPropertyRecordAccess propertyRecords = new BatchingPropertyRecordAccess();
    private final ReusableIteratorCostume<PropertyBlock> blockIterator = new ReusableIteratorCostume<>();

    EntityStoreUpdaterStep( StageControl control, String name, Configuration config,
            AbstractRecordStore<RECORD> entityStore,
            PropertyStore propertyStore, IoMonitor monitor, WriterFactory writerFactory )
    {
        super( control, name, 1, config.movingAverageSize(), 1 ); // work-ahead doesn't matter, we're the last one
        this.entityStore = entityStore;
        this.propertyStore = propertyStore;
        this.writerFactory = writerFactory;
        this.propertyCreator = new PropertyCreator( propertyStore, null );
        this.monitor = monitor;
        this.monitor.reset();
    }

    @Override
    protected Object process( long ticket, List<BatchEntity<RECORD,INPUT>> batch )
    {
        // Clear reused data structures
        propertyRecords.close();

        // Write the entity records, and at the same time allocate property records for its property blocks.
        long highestId = 0;
        for ( BatchEntity<RECORD,INPUT> entity : batch )
        {
            RECORD record = entity.record();
            PropertyBlock[] propertyBlocks = entity.getPropertyBlocks();
            if ( propertyBlocks.length > 0 )
            {
                reassignDynamicRecordIds( propertyBlocks );
                long firstProp = propertyCreator.createPropertyChain( record,
                        blockIterator.dressArray( propertyBlocks ), propertyRecords );
                record.setNextProp( firstProp );
            }
            highestId = max( highestId, record.getId() );
            entityStore.updateRecord( record );
        }
        entityStore.setHighestPossibleIdInUse( highestId );

        // Write all the created property records.
        for ( PropertyRecord propertyRecord : propertyRecords.records() )
        {
            propertyStore.updateRecord( propertyRecord );
        }
        return null; // end of the line
    }

    private void reassignDynamicRecordIds( PropertyBlock[] blocks )
    {
        // OK, so here we have property blocks, potentially referring to DynamicRecords. The DynamicRecords
        // have ids that we need to re-assign in here, because the ids are generated by multiple property encoders,
        // and so we let each one of the encoders generate their own bogus ids and we re-assign those ids here,
        // where we know we have a single thread doing this.
        for ( PropertyBlock block : blocks )
        {
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
    protected void addStatsProviders( Collection<StatsProvider> providers )
    {
        super.addStatsProviders( providers );
        providers.add( monitor );
    }

    @Override
    protected void done()
    {
        // Stop the I/O monitor, since the stats in there is based on time passed since the start
        // and bytes written. NodeStage and CalculateDenseNodesStage can be run in parallel so if
        // NodeStage completes before CalculateDenseNodesStage then we want to stop the time in the I/O monitor.
        monitor.stop();
    }

    // Below we override the "parallizable" methods to go directly towards the I/O writer, since
    // this step is very cheap and not parallelizable, except for the I/O part which is all handled by the writer.
    @Override
    public int numberOfProcessors()
    {
        return writerFactory.numberOfProcessors();
    }

    @Override
    public boolean incrementNumberOfProcessors()
    {
        return writerFactory.incrementNumberOfProcessors();
    }

    @Override
    public boolean decrementNumberOfProcessors()
    {
        return writerFactory.decrementNumberOfProcessors();
    }
}
