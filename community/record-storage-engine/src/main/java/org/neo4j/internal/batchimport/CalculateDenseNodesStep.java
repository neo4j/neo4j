/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.batchimport.stats.StatsProvider;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.MemoryTracker;

/**
 * Increments counts for each visited relationship, once for start node and once for end node
 * (unless for loops). This to be able to determine which nodes are dense before starting to import relationships.
 */
public class CalculateDenseNodesStep extends ProcessorStep<RelationshipRecord[]> {
    private static final int NUM_LATCHES = 1024;
    private static final int LATCH_STRIPE_MASK = Integer.highestOneBit(NUM_LATCHES) - 1;

    private final Lock[] latches = new Lock[NUM_LATCHES];
    private final NodeRelationshipCache cache;

    public CalculateDenseNodesStep(
            StageControl control,
            Configuration config,
            NodeRelationshipCache cache,
            CursorContextFactory contextFactory,
            StatsProvider... statsProviders) {
        super(control, "CALCULATE", config, config.maxNumberOfWorkerThreads(), contextFactory, statsProviders);
        this.cache = cache;
        for (int i = 0; i < latches.length; i++) {
            latches[i] = new ReentrantLock();
        }
    }

    @Override
    protected void process(
            RelationshipRecord[] batch, BatchSender sender, CursorContext cursorContext, MemoryTracker memoryTracker)
            throws Throwable {
        for (RelationshipRecord record : batch) {
            if (record.inUse()) {
                long startNodeId = record.getFirstNode();
                long endNodeId = record.getSecondNode();
                processNodeId(startNodeId);
                if (startNodeId != endNodeId) // avoid counting loops twice
                {
                    // Loops only counts as one
                    processNodeId(endNodeId);
                }
            }
        }
    }

    private void processNodeId(long nodeId) {
        int hash = (int) (nodeId & LATCH_STRIPE_MASK);
        Lock latch = latches[hash];
        latch.lock();
        try {
            cache.incrementCount(nodeId);
        } finally {
            latch.unlock();
        }
    }
}
