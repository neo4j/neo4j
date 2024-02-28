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

import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_max_merge;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_min_merge;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_nocfs_ratio;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.neo4j.configuration.Config;

/**
 * Helper factory for standard lucene index writer configuration.
 */
public final class IndexWriterConfigBuilder {

    private static final Analyzer KEYWORD_ANALYZER = new KeywordAnalyzer();

    private final IndexWriterConfigModes.Mode mode;
    private final Config config;
    private Analyzer analyzer = KEYWORD_ANALYZER;
    private Codec codec;

    public IndexWriterConfigBuilder(IndexWriterConfigModes.Mode mode, Config config) {
        this.mode = mode;
        this.config = config;
    }

    public IndexWriterConfigBuilder withAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public IndexWriterConfigBuilder withCodec(Codec codec) {
        this.codec = codec;
        return this;
    }

    public IndexWriterConfig build() {
        final var writerConfig = new IndexWriterConfig(analyzer)
                .setIndexDeletionPolicy(new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy()))
                .setUseCompoundFile(true)
                .setMaxFullFlushMergeWaitMillis(0);

        if (codec != null) {
            writerConfig.setCodec(codec);
        }

        final var mergePolicy = new LogByteSizeMergePolicy();
        mergePolicy.setNoCFSRatio(config.get(lucene_nocfs_ratio));
        mergePolicy.setMinMergeMB(config.get(lucene_min_merge));
        mergePolicy.setMaxMergeMB(config.get(lucene_max_merge));
        writerConfig.setMergePolicy(mode.visitWithConfig(mergePolicy, config));

        return mode.visitWithConfig(writerConfig, config);
    }
}
