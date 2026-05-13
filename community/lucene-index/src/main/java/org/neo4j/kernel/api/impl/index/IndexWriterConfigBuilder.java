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

import static org.neo4j.kernel.api.impl.index.lucene.LuceneSettings.lucene_max_cfs_segment_size_mb;
import static org.neo4j.kernel.api.impl.index.lucene.LuceneSettings.lucene_max_merge;
import static org.neo4j.kernel.api.impl.index.lucene.LuceneSettings.lucene_min_merge;
import static org.neo4j.kernel.api.impl.index.lucene.LuceneSettings.lucene_nocfs_ratio;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig;
import org.neo4j.kernel.api.impl.index.lucene.codec.LuceneCodec;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

/**
 * Helper factory for standard lucene index writer configuration.
 */
public final class IndexWriterConfigBuilder {

    private static final Analyzer KEYWORD_ANALYZER = new KeywordAnalyzer();

    private final IndexWriterConfigMode mode;
    private final Config config;
    private LogProvider logProvider = NullLogProvider.getInstance();
    private Analyzer analyzer = KEYWORD_ANALYZER;
    private LuceneCodec codec;

    public IndexWriterConfigBuilder(IndexWriterConfigMode mode, Config config) {
        this.mode = mode;
        this.config = config;
    }

    public IndexWriterConfigBuilder withLogProvider(LogProvider logProvider) {
        this.logProvider = logProvider;
        return this;
    }

    public IndexWriterConfigBuilder withAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public IndexWriterConfigBuilder withCodec(LuceneCodec codec) {
        this.codec = codec;
        return this;
    }

    public LuceneIndexWriterConfig build() {
        LuceneIndexWriterConfig writerConfig = new LuceneIndexWriterConfig(analyzer).setLogProvider(logProvider);
        if (codec != null) {
            writerConfig.setCodec(codec);
        }

        writerConfig.setMergingParameters(
                config.get(lucene_nocfs_ratio),
                config.get(lucene_max_cfs_segment_size_mb),
                mode.mergePolicy(config),
                config.get(lucene_min_merge),
                config.get(lucene_max_merge),
                mode.getMergeFactor(config),
                mode.segmentsPerTier(config),
                mode.maxMergeAtOnce(config));

        return mode.visitWithConfig(writerConfig, config);
    }
}
