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

import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.Configuration;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static org.neo4j.unsafe.impl.batchimport.Utils.anyIdCollides;
import static org.neo4j.unsafe.impl.batchimport.Utils.mergeSortedInto;

/**
 * Enables {@link RelationshipEncoderStep} to safely process batches in parallel.
 *
 * Using actual node ids from {@link Batch#ids}, this step compares ids from consecutive batches and
 * detects whether or not there are any overlap of ids between the batches. Batches that have no id overlap
 * can be run in parallel. Batches that do have id overlap will not be run in parallel since that would
 * introduce a chance for multiple threads updating the same cache entries of the {@link NodeRelationshipCache}
 * concurrently.
 *
 * "Why not solve this with CAS in the {@link NodeRelationshipCache}, you say"... sure, that
 * would solve the issue of updating the same cache entries concurrently, but may break a contract of
 * the batch importer that relationship ids in any chain are ordered by id. Id ordering inside chains are
 * fundamental for implementing the {@link RelationshipLinkbackStage "prev pointer" linking} in the
 * efficient way it's done currently, upholding sequential I/O access properties correctly.
 *
 * This step will have an array, {@link #concurrentNodeIds}, containing all node ids that are potentially
 * processed (by the downstream step) at any given point in time. In order to allow one more batch to be
 * executed in parallel with any other concurrently executing batches no node id in the tentative batch
 * can already exist in {@link #concurrentNodeIds}. This makes this array grow with consecutive parallelizable
 * batches until a non-parallelizable batch is encountered, at which point the array can be reset and
 * the comparison (and potential growth of it) can begin again. Implementation of this is garbage-free,
 * but fact still remains that multiple consecutive parallelizable batches will keep increasing the
 * time it takes to detect this property. Therefore there's an upper limit of 10 for the number of
 * consecutive parallelizable batches, such that even if there would theoretically be 11 consecutive
 * parallelizable batches, the 11:th will not be marked as parallelizable with the other previous 10.
 */
public class ParallelizeByNodeIdStep extends ProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    private static final int MAX_PARALLELIZABLE_BATCHES = 10;

    private final int batchSize;
    private final int idBatchSize;

    // Since this step is single-threaded and currently RelationshipEncoderStep isn't then this is a perfect
    // place for assigning actual relationship ids to batches, set in each Batch and used in RelationshipEncoderStep.
    private long firstRecordId;

    // Collection of all ids from batches (theoretically) concurrently processing. Ids in here are sorted.
    private final long[] concurrentNodeIds;
    private int concurrentBatches;

    public ParallelizeByNodeIdStep( StageControl control, Configuration config )
    {
        this( control, config, 0 );
    }

    public ParallelizeByNodeIdStep( StageControl control, Configuration config, long firstRecordId )
    {
        super( control, "PARALLELIZE", config, 1 );
        // x2 since ids array cover both start and end nodes
        this.batchSize = config.batchSize();
        this.idBatchSize = batchSize*2;
        this.concurrentNodeIds = new long[idBatchSize * MAX_PARALLELIZABLE_BATCHES];
        this.firstRecordId = firstRecordId;
    }

    @Override
    protected void process( Batch<InputRelationship,RelationshipRecord> batch, BatchSender sender ) throws Throwable
    {
        // Compare ids with concurrent ids
        int concurrentNodeIdsRange = concurrentBatches*idBatchSize;
        batch.parallelizableWithPrevious = concurrentBatches < MAX_PARALLELIZABLE_BATCHES && !anyIdCollides(
                concurrentNodeIds, concurrentNodeIdsRange, batch.sortedIds, batch.ids.length );

        // Assign first record id and send
        batch.firstRecordId = firstRecordId;
        sender.send( batch );

        // Set state for the next batch
        firstRecordId += batch.input.length;
        if ( firstRecordId <= IdGeneratorImpl.INTEGER_MINUS_ONE &&
                firstRecordId + batchSize >= IdGeneratorImpl.INTEGER_MINUS_ONE )
        {
            // There's this pesky INTEGER_MINUS_ONE ID again. Easiest is to simply skip this batch of ids
            // or at least the part up to that id and just continue after it.
            firstRecordId = IdGeneratorImpl.INTEGER_MINUS_ONE + 1;
        }

        if ( batch.parallelizableWithPrevious )
        {
            mergeSortedInto( batch.sortedIds, concurrentNodeIds, concurrentNodeIdsRange );
            concurrentBatches++;
        }
        else
        {
            System.arraycopy( batch.sortedIds, 0, concurrentNodeIds, 0, batch.sortedIds.length );
            concurrentBatches = 1;
        }
    }
}
