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
package org.neo4j.internal.batchimport.staging;

import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.RecordIdIterator;

/**
 * Releases batches of record ids to be read, potentially in parallel, by downstream batches.
 */
public class BatchFeedStep extends PullingProducerStep<ProcessContext> {
    private final RecordIdIterator ids;
    private final int recordSize;
    private final AtomicLong count = new AtomicLong();

    public BatchFeedStep(StageControl control, Configuration config, RecordIdIterator ids, int recordSize) {
        super(control, config);
        this.ids = ids;
        this.recordSize = recordSize;
    }

    @Override
    protected Object nextBatchOrNull(long ticket, int batchSize, ProcessContext processContext) {
        count.getAndAdd(batchSize);
        return ids.nextBatch();
    }

    @Override
    protected long position() {
        return count.get() * recordSize;
    }
}
