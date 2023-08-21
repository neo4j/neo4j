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
package org.neo4j.kernel.api.impl.index;

import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_merge_factor;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_min_merge;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_nocfs_ratio;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_population_max_buffered_docs;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_population_ram_buffer_size;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_standard_ram_buffer_size;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_writer_max_buffered_docs;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.vector_merge_factor;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.vector_population_ram_buffer_size;
import static org.neo4j.kernel.api.impl.schema.LuceneIndexType.VECTOR;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.impl.schema.LuceneIndexType;

/**
 * Helper factory for standard lucene index writer configuration.
 */
public final class IndexWriterConfigs {
    private static final Analyzer KEYWORD_ANALYZER = new KeywordAnalyzer();

    private IndexWriterConfigs() {
        throw new AssertionError("Not for instantiation!");
    }

    public static IndexWriterConfig standard(LuceneIndexType index, Config config) {
        return standard(index, config, KEYWORD_ANALYZER);
    }

    public static IndexWriterConfig standard(LuceneIndexType index, Config config, Analyzer analyzer) {
        final var writerConfig = new IndexWriterConfig(analyzer);

        writerConfig.setMaxBufferedDocs(config.get(lucene_writer_max_buffered_docs));
        writerConfig.setIndexDeletionPolicy(new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy()));
        writerConfig.setUseCompoundFile(true);
        writerConfig.setMaxFullFlushMergeWaitMillis(0);
        writerConfig.setRAMBufferSizeMB(config.get(lucene_standard_ram_buffer_size));

        final var mergePolicy = new LogByteSizeMergePolicy();
        mergePolicy.setNoCFSRatio(config.get(lucene_nocfs_ratio));
        mergePolicy.setMinMergeMB(config.get(lucene_min_merge));
        mergePolicy.setMergeFactor(config.get(index == VECTOR ? vector_merge_factor : lucene_merge_factor));
        writerConfig.setMergePolicy(mergePolicy);

        return writerConfig;
    }

    public static IndexWriterConfig population(LuceneIndexType index, Config config) {
        return population(index, config, KEYWORD_ANALYZER);
    }

    public static IndexWriterConfig population(LuceneIndexType index, Config config, Analyzer analyzer) {
        final var writerConfig = standard(index, config, analyzer);
        writerConfig.setMaxBufferedDocs(config.get(lucene_population_max_buffered_docs));
        writerConfig.setRAMBufferSizeMB(
                config.get(index == VECTOR ? vector_population_ram_buffer_size : lucene_population_ram_buffer_size));
        if (config.get(LuceneSettings.lucene_population_serial_merge_scheduler)) {
            // With this setting 'true' we respect the GraphDatabaseInternalSettings.index_population_workers setting
            // and
            // don't use separate lucene threads for merging during population.
            // Population is a background task, and it is probably more important to limit CPU usage
            // than be as fast as possible here.
            writerConfig.setMergeScheduler(new OnThreadConcurrentMergeScheduler());
        }
        return writerConfig;
    }

    public static IndexWriterConfig transactionState(LuceneIndexType index, Config config, Analyzer analyzer) {
        final var writerConfig = standard(index, config, analyzer);
        // Index transaction state is never directly persisted, so never commit it on close.
        writerConfig.setCommitOnClose(false);
        return writerConfig;
    }
}
