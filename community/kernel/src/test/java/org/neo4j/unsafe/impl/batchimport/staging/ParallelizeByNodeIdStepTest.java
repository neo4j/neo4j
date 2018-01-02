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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.Test;

import java.util.Arrays;

import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.Batch;
import org.neo4j.unsafe.impl.batchimport.ParallelizeByNodeIdStep;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.Configuration;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ParallelizeByNodeIdStepTest
{
    private final StageControl control = mock( StageControl.class );
    private final BatchSender sender = mock( BatchSender.class );

    @Test
    public void shouldDetectABA() throws Throwable
    {
        // GIVEN
        ProcessorStep<Batch<InputRelationship,RelationshipRecord>> step = new ParallelizeByNodeIdStep(
                control, Configuration.DEFAULT );
        int batchSize = Configuration.DEFAULT.batchSize();
        Batch<InputRelationship,RelationshipRecord> a = new Batch<>( new InputRelationship[batchSize] );
        setIds( a, 0, 2, batchSize*2 );
        Batch<InputRelationship,RelationshipRecord> b = new Batch<>( new InputRelationship[batchSize] );
        setIds( b, 1, 2, batchSize*2 );
        Batch<InputRelationship,RelationshipRecord> aa = new Batch<>( new InputRelationship[batchSize] );
        setIds( aa, 0, 2, batchSize*2 );
        Batch<InputRelationship,RelationshipRecord> bb = new Batch<>( new InputRelationship[batchSize] );
        setIds( bb, 1, 2, batchSize*2 );

        // WHEN
        step.process( a, sender );
        step.process( b, sender );
        step.process( aa, sender );
        step.process( bb, sender );

        // THEN
        assertTrue( a.parallelizableWithPrevious ); // because it's the first batch
        assertTrue( b.parallelizableWithPrevious ); // because no id here collides with the previous batch
        assertFalse( aa.parallelizableWithPrevious ); // because there's one or more ids in this batch that collides
                                                      // with the first batch
        assertTrue( bb.parallelizableWithPrevious ); // because no id here collides with aa
    }

    @Test
    public void shouldSkipReservervedId() throws Throwable
    {
        // GIVEN
        ProcessorStep<Batch<InputRelationship,RelationshipRecord>> step = new ParallelizeByNodeIdStep( control,
                Configuration.DEFAULT, IdGeneratorImpl.INTEGER_MINUS_ONE - 123_456 );
        int batchSize = Configuration.DEFAULT.batchSize();

        // WHEN
        Batch<InputRelationship,RelationshipRecord> batch = new Batch<>( new InputRelationship[batchSize] );
        batch.ids = new long[] {1L};
        batch.sortedIds = batch.ids.clone();
        while ( batch.firstRecordId < IdGeneratorImpl.INTEGER_MINUS_ONE )
        {
            step.process( batch, sender );
            assertFalse( "Batch got first id " + batch.firstRecordId + " which contains the reserved id",
                    idWithin( IdGeneratorImpl.INTEGER_MINUS_ONE,
                            batch.firstRecordId, batch.firstRecordId + batchSize ) );
        }

        assertTrue( batch.firstRecordId > IdGeneratorImpl.INTEGER_MINUS_ONE );
    }

    private boolean idWithin( long id, long low, long high )
    {
        return id >= low && id <= high;
    }

    private void setIds( Batch<?,?> batch, long first, long stride, int count )
    {
        batch.ids = new long[count];
        long value = first;
        for ( int i = 0; i < count; i++ )
        {
            batch.ids[i] = value;
            value += stride;
        }
        batch.sortedIds = batch.ids.clone();
        Arrays.sort( batch.sortedIds );
    }
}
