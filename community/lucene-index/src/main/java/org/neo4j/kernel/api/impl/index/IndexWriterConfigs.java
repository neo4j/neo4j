/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;

import org.neo4j.util.FeatureToggles;

/**
 * Helper factory for standard lucene index writer configuration.
 */
public final class IndexWriterConfigs
{
    private static final Analyzer KEYWORD_ANALYZER = new KeywordAnalyzer();

    private static final int MAX_BUFFERED_DOCS = FeatureToggles.getInteger( IndexWriterConfigs.class, "max_buffered_docs", 100000 );
    private static final int POPULATION_MAX_BUFFERED_DOCS =
            FeatureToggles.getInteger( IndexWriterConfigs.class, "population_max_buffered_docs", IndexWriterConfig.DISABLE_AUTO_FLUSH );
    private static final int MERGE_POLICY_MERGE_FACTOR = FeatureToggles.getInteger( IndexWriterConfigs.class, "merge.factor", 2 );
    private static final double MERGE_POLICY_NO_CFS_RATIO = FeatureToggles.getDouble( IndexWriterConfigs.class, "nocfs.ratio", 1.0 );
    private static final double MERGE_POLICY_MIN_MERGE_MB = FeatureToggles.getDouble( IndexWriterConfigs.class, "min.merge", 0.1 );

    private static final double STANDARD_RAM_BUFFER_SIZE_MB =
            FeatureToggles.getDouble( IndexWriterConfigs.class, "standard.ram.buffer.size", IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB );
    private static final double POPULATION_RAM_BUFFER_SIZE_MB = FeatureToggles.getDouble( IndexWriterConfigs.class, "population.ram.buffer.size", 50 );

    private IndexWriterConfigs()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    public static IndexWriterConfig standard()
    {
        return standard( KEYWORD_ANALYZER );
    }

    public static IndexWriterConfig standard( Analyzer analyzer )
    {
        IndexWriterConfig writerConfig = new IndexWriterConfig( analyzer );

        writerConfig.setMaxBufferedDocs( MAX_BUFFERED_DOCS );
        writerConfig.setIndexDeletionPolicy( new SnapshotDeletionPolicy( new KeepOnlyLastCommitDeletionPolicy() ) );
        writerConfig.setUseCompoundFile( true );
        writerConfig.setRAMBufferSizeMB( STANDARD_RAM_BUFFER_SIZE_MB );

        LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
        mergePolicy.setNoCFSRatio( MERGE_POLICY_NO_CFS_RATIO );
        mergePolicy.setMinMergeMB( MERGE_POLICY_MIN_MERGE_MB );
        mergePolicy.setMergeFactor( MERGE_POLICY_MERGE_FACTOR );
        writerConfig.setMergePolicy( mergePolicy );

        return writerConfig;
    }

    public static IndexWriterConfig population()
    {
        return population( KEYWORD_ANALYZER );
    }

    public static IndexWriterConfig population( Analyzer analyzer )
    {
        IndexWriterConfig writerConfig = standard( analyzer );
        writerConfig.setMaxBufferedDocs( POPULATION_MAX_BUFFERED_DOCS );
        writerConfig.setRAMBufferSizeMB( POPULATION_RAM_BUFFER_SIZE_MB );
        return writerConfig;
    }

    public static IndexWriterConfig transactionState( Analyzer analyzer )
    {
        IndexWriterConfig config = standard( analyzer );
        // Index transaction state is never directly persisted, so never commit it on close.
        config.setCommitOnClose( false );
        return config;
    }
}
