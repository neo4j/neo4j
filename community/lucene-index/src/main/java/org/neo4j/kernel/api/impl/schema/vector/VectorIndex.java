/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.api.impl.schema.vector;

import java.io.IOException;
import java.util.List;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndex;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.impl.schema.reader.PartitionedValueIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracker;

class VectorIndex extends AbstractLuceneIndex<ValueIndexReader> {
    private final IndexSamplingConfig samplingConfig;
    private final TaskCoordinator taskCoordinator = new TaskCoordinator();

    VectorIndex(
            PartitionedIndexStorage indexStorage,
            IndexPartitionFactory partitionFactory,
            IndexDescriptor descriptor,
            Config config,
            IndexSamplingConfig samplingConfig) {
        super(indexStorage, partitionFactory, descriptor, config);
        this.samplingConfig = samplingConfig;
    }

    @Override
    public void drop() {
        taskCoordinator.cancel();
        try {
            taskCoordinator.awaitCompletion();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for concurrent tasks to complete.", e);
        }
        super.drop();
    }

    @Override
    protected ValueIndexReader createSimpleReader(
            List<AbstractIndexPartition> partitions, IndexUsageTracker usageTracker) throws IOException {
        final var searcher = getFirstPartition(partitions);
        return new VectorIndexReader(
                descriptor, searcher.acquireSearcher(), samplingConfig, taskCoordinator, usageTracker);
    }

    @Override
    protected PartitionedValueIndexReader createPartitionedReader(
            List<AbstractIndexPartition> partitions, IndexUsageTracker usageTracker) throws IOException {
        final var readers = acquireSearchers(partitions).stream()
                .map(partitionSearcher -> (ValueIndexReader) new VectorIndexReader(
                        descriptor, partitionSearcher, samplingConfig, taskCoordinator, usageTracker))
                .toList();
        return new PartitionedValueIndexReader(descriptor, readers, usageTracker);
    }
}
