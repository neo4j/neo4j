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

import org.junit.Test;

import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.staging.Step;

import static org.junit.Assert.assertEquals;

public class CalculateDenseNodesStepTest
{
    /**
     * Batches are provided to {@link CalculateDenseNodesStep} in batches where each id all is of the same radix.
     * This test asserts that, regardless of how many processors are assigned to the step there cannot be
     * two processors processing multiple batches with ids of the same radix concurrently.
     */
    @Test
    public void shouldPreventMultipleConcurrentProcessorsForAnyGivenRadixBatchSparse() throws Exception
    {
        // GIVEN
        StageControl control = new StageControl()
        {
            @Override
            public void panic( Throwable cause )
            {
                cause.printStackTrace();
            }
        };
        Configuration config = Configuration.DEFAULT;
        NodeRelationshipCache cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, -1 );
        Step<long[]> step = new CalculateDenseNodesStep( control, config, cache );
        step.start( 0 );
        maxOutNumberOfProcessors( step );

        // WHEN sending many batches, all which "happens" to have ids of the same radix, in fact
        // this test "happens" to send the same batch of ids over and over, which actually may happen in read life,
        // although it's an extreme case.
        long[] ids = batchOfIdsWithRadix( 3 );
        int numberOfBatches = 100;
        for ( int i = 0; i < numberOfBatches; i++ )
        {
            step.receive( i, ids );
        }
        step.endOfUpstream();
        waitUntilCompleted( step );

        // THEN
        for ( long id : ids )
        {
            assertEquals( numberOfBatches, cache.getCount( id, 0, null /*shouldn't be used here anyway*/ ) );
        }
    }

    private void waitUntilCompleted( Step<?> step ) throws InterruptedException
    {
        while ( !step.isCompleted() )
        {
            Thread.sleep( 1 );
        }
    }

    private long[] batchOfIdsWithRadix( int radixOutOfTen )
    {
        long[] ids = new long[1_000];
        for ( int i = 0; i < ids.length; i++ )
        {
            ids[i] = i*10 + radixOutOfTen;
        }
        return ids;
    }

    private void maxOutNumberOfProcessors( Step<?> step )
    {
        for ( int i = 0; i < 100 && step.incrementNumberOfProcessors(); i++ )
        {
            // Then increment number of processors
        }
    }
}
