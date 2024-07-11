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
package org.neo4j.kernel.api.impl.schema.trigram;

import java.io.IOException;
import java.util.List;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndex;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.reader.PartitionedValueIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;

class TrigramIndex extends AbstractLuceneIndex<ValueIndexReader> {

    TrigramIndex(
            PartitionedIndexStorage indexStorage,
            IndexDescriptor descriptor,
            IndexPartitionFactory partitionFactory,
            Config config) {
        super(indexStorage, partitionFactory, descriptor, config);
    }

    @Override
    protected TrigramIndexReader createSimpleReader(
            List<AbstractIndexPartition> partitions, IndexUsageTracking usageTracker) throws IOException {
        AbstractIndexPartition searcher = getFirstPartition(partitions);
        return new TrigramIndexReader(searcher.acquireSearcher(), descriptor, usageTracker);
    }

    @Override
    protected PartitionedValueIndexReader createPartitionedReader(
            List<AbstractIndexPartition> partitions, IndexUsageTracking usageTracker) throws IOException {
        List<ValueIndexReader> readers = acquireSearchers(partitions).stream()
                .map(partitionSearcher ->
                        (ValueIndexReader) new TrigramIndexReader(partitionSearcher, descriptor, usageTracker))
                .toList();
        return new PartitionedValueIndexReader(descriptor, readers, usageTracker);
    }
}
