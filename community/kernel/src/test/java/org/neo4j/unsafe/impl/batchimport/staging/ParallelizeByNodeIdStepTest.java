/*
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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.Test;

import java.util.Arrays;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.Batch;
import org.neo4j.unsafe.impl.batchimport.ParallelizeByNodeIdStep;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ParallelizeByNodeIdStepTest
{
    @Test
    public void shouldDetectABA() throws Throwable
    {
        // GIVEN
        StageControl control = mock( StageControl.class );
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
        BatchSender sender = mock( BatchSender.class );

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
