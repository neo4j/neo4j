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

import java.util.function.LongFunction;

import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.PrepareIdSequence;
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

    private final CommonAbstractStore<RECORD,? extends StoreHeader> entityStore;
    private final PropertyStore propertyStore;
    private final IoMonitor ioMonitor;
    private final Monitor monitor;
    private final PrepareIdSequence prepareIdSequence;

    EntityStoreUpdaterStep( StageControl control, Configuration config,
            CommonAbstractStore<RECORD,? extends StoreHeader> entityStore,
            PropertyStore propertyStore, IoMonitor ioMonitor,
            Monitor monitor, PrepareIdSequence prepareIdSequence )
    {
        super( control, "v", config, config.parallelRecordWrites() ? 0 : 1, ioMonitor );
        this.entityStore = entityStore;
        this.propertyStore = propertyStore;
        this.monitor = monitor;
        this.ioMonitor = ioMonitor;
        this.prepareIdSequence = prepareIdSequence;
        this.ioMonitor.reset();
    }

    @Override
    protected void process( Batch<INPUT,RECORD> batch, BatchSender sender )
    {
        // Write the entity records, and at the same time allocate property records for its property blocks.
        LongFunction<IdSequence> idSequence = prepareIdSequence.apply( entityStore );
        long highestId = 0;
        RECORD[] records = batch.records;
        if ( records.length == 0 )
        {
            return;
        }

        int skipped = 0;
        for ( RECORD record : records )
        {
            if ( record != null && record.inUse() )
            {
                highestId = max( highestId, record.getId() );
                entityStore.prepareForCommit( record, idSequence.apply( record.getId() ) );
                entityStore.updateRecord( record );
            }
            else
            {   // Here we have a relationship that refers to missing nodes. It's within the tolerance levels
                // of number of bad relationships. Just don't import this relationship.
                skipped++;
            }
        }

        writePropertyRecords( batch.propertyRecords, propertyStore );

        monitor.entitiesWritten( records[0].getClass(), records.length - skipped );
        monitor.propertiesWritten( batch.numberOfProperties );
    }

    static void writePropertyRecords( PropertyRecord[][] batch, PropertyStore propertyStore )
    {
        // Write all the created property records.
        for ( PropertyRecord[] propertyRecords : batch )
        {
            if ( propertyRecords != null )
            {
                for ( PropertyRecord propertyRecord : propertyRecords )
                {
                    propertyStore.prepareForCommit( propertyRecord );
                    propertyStore.updateRecord( propertyRecord );
                }
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
