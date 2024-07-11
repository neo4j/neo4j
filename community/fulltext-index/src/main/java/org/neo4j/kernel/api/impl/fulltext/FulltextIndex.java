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
package org.neo4j.kernel.api.impl.fulltext;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndex;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.token.api.TokenHolder;

public class FulltextIndex extends AbstractLuceneIndex<FulltextIndexReader> implements Closeable {
    private final Config config;
    private final Analyzer analyzer;
    private final TokenHolder propertyKeyTokenHolder;
    private final String[] propertyNames;
    private final Path transactionsFolder;

    FulltextIndex(
            PartitionedIndexStorage storage,
            IndexPartitionFactory partitionFactory,
            IndexDescriptor descriptor,
            TokenHolder propertyKeyTokenHolder,
            Config config,
            Analyzer analyzer,
            String[] propertyNames) {
        super(storage, partitionFactory, descriptor, config);
        this.config = config;
        this.analyzer = analyzer;
        this.propertyNames = propertyNames;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        Path indexFolder = storage.getIndexFolder();
        transactionsFolder = indexFolder.resolve(indexFolder.getFileName() + ".tx");
    }

    @Override
    public void open() throws IOException {
        super.open();
        indexStorage.prepareFolder(transactionsFolder);
    }

    @Override
    public void close() throws IOException {
        super.close();
        indexStorage.cleanupFolder(transactionsFolder);
    }

    @Override
    protected FulltextIndexReader createSimpleReader(
            List<AbstractIndexPartition> partitions, IndexUsageTracking usageTracker) throws IOException {
        return createPartitionedReader(partitions, usageTracker);
    }

    @Override
    protected FulltextIndexReader createPartitionedReader(
            List<AbstractIndexPartition> partitions, IndexUsageTracking usageTracker) throws IOException {
        List<SearcherReference> searchers = acquireSearchers(partitions);
        return new FulltextIndexReader(
                searchers, propertyKeyTokenHolder, getDescriptor(), config, analyzer, propertyNames, usageTracker);
    }
}
