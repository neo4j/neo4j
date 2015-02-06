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

import java.util.Arrays;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.transaction.state.PropertyCreator;
import org.neo4j.kernel.impl.util.MovingAverage;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutorServiceStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;

/**
 * Encodes property data into {@link PropertyBlock property blocks}, attaching them to each
 * {@link BatchEntity}. This step is designed to handle multiple threads doing the property encoding,
 * since property encoding is potentially the most costly step in this {@link Stage}.
 */
public class PropertyEncoderStep<RECORD extends PrimitiveRecord,INPUT extends InputEntity>
        extends ExecutorServiceStep<Batch<INPUT,RECORD>>
{
    private final BatchingPropertyKeyTokenRepository propertyKeyHolder;
    private final int arrayDataSize;
    private final int stringDataSize;
    private final MovingAverage averageBlocksPerBatch;

    protected PropertyEncoderStep( StageControl control, Configuration config, int numberOfExecutors,
            BatchingPropertyKeyTokenRepository propertyKeyHolder,
            PropertyStore propertyStore )
    {
        super( control, "PROPERTIES", config.workAheadSize(), config.movingAverageSize(), numberOfExecutors, true );
        this.propertyKeyHolder = propertyKeyHolder;
        this.arrayDataSize = propertyStore.getArrayStore().dataSize();
        this.stringDataSize = propertyStore.getStringStore().dataSize();
        this.averageBlocksPerBatch = new MovingAverage( config.movingAverageSize() );
    }

    @Override
    protected Object process( long ticket, Batch<INPUT,RECORD> batch )
    {
        RelativeIdRecordAllocator stringAllocator = new RelativeIdRecordAllocator( stringDataSize );
        RelativeIdRecordAllocator arrayAllocator = new RelativeIdRecordAllocator( arrayDataSize );
        PropertyCreator propertyCreator = new PropertyCreator( stringAllocator, arrayAllocator, null, null );

        int blockCountGuess = (int) averageBlocksPerBatch.average();
        PropertyBlock[] propertyBlocks = new PropertyBlock[blockCountGuess == 0
                ? batch.input.length
                : blockCountGuess + batch.input.length / 20 /*some upper margin*/];
        int blockCursor = 0;

        for ( int i = 0; i < batch.input.length; i++ )
        {
            stringAllocator.initialize();
            arrayAllocator.initialize();

            INPUT input = batch.input[i];
            if ( !input.hasFirstPropertyId() )
            {   // Encode the properties and attach the blocks to the BatchEntity.
                // Dynamic records for each entity will start from 0, they will be reassigned later anyway
                int count = input.properties().length >> 1;
                if ( blockCursor+count > propertyBlocks.length )
                {
                    propertyBlocks = Arrays.copyOf( propertyBlocks, propertyBlocks.length << 1 );
                }
                propertyKeyHolder.propertyKeysAndValues( propertyBlocks, blockCursor,
                        input.properties(), propertyCreator );
                batch.propertyBlocksLengths[i] = count;
                blockCursor += count;
            }
        }
        batch.propertyBlocks = propertyBlocks;
        averageBlocksPerBatch.add( blockCursor );
        return batch;
    }
}
