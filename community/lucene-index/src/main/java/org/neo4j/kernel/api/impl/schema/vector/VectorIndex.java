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
package org.neo4j.kernel.api.impl.schema.vector;

import java.io.IOException;
import java.util.List;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndex;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.logging.LogProvider;

class VectorIndex extends AbstractLuceneIndex<VectorIndexReader> {
    private final VectorIndexConfig vectorIndexConfig;
    private final VectorDocumentStructure documentStructure;
    private final int maxEfSearch;

    VectorIndex(
            PartitionedIndexStorage indexStorage,
            IndexPartitionFactory partitionFactory,
            VectorDocumentStructure documentStructure,
            IndexDescriptor descriptor,
            VectorIndexConfig vectorIndexConfig,
            Config config,
            LogProvider logProvider) {
        super(indexStorage, partitionFactory, descriptor, config, logProvider);
        this.vectorIndexConfig = vectorIndexConfig;
        this.documentStructure = documentStructure;
        this.maxEfSearch = config.get(LuceneSettings.vector_hnsw_max_ef_search);
    }

    @Override
    protected VectorIndexReader createSimpleReader(
            List<AbstractIndexPartition> partitions, IndexUsageTracking usageTracker) throws IOException {
        return createPartitionedReader(partitions, usageTracker);
    }

    @Override
    protected VectorIndexReader createPartitionedReader(
            List<AbstractIndexPartition> partitions, IndexUsageTracking usageTracker) throws IOException {
        List<SearcherReference> searchers = acquireSearchers(partitions);
        return new VectorIndexReader(
                descriptor, vectorIndexConfig, documentStructure, maxEfSearch, searchers, usageTracker, logProvider);
    }
}
