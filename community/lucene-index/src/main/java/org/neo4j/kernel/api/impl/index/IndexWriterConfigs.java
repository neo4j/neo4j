/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.configuration.Config;

import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_merge_factor;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_min_merge;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_nocfs_ratio;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_population_max_buffered_docs;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_population_ram_buffer_size;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_standard_ram_buffer_size;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_writer_max_buffered_docs;

/**
 * Helper factory for standard lucene index writer configuration.
 */
public final class IndexWriterConfigs
{
    private static final Analyzer KEYWORD_ANALYZER = new KeywordAnalyzer();

    private IndexWriterConfigs()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    public static IndexWriterConfig standard( Config config )
    {
        return standard( config, KEYWORD_ANALYZER );
    }

    public static IndexWriterConfig standard( Config config, Analyzer analyzer )
    {
        IndexWriterConfig writerConfig = new IndexWriterConfig( analyzer );

        writerConfig.setMaxBufferedDocs( config.get( lucene_writer_max_buffered_docs ) );
        writerConfig.setIndexDeletionPolicy( new SnapshotDeletionPolicy( new KeepOnlyLastCommitDeletionPolicy() ) );
        writerConfig.setUseCompoundFile( true );
        writerConfig.setRAMBufferSizeMB( config.get( lucene_standard_ram_buffer_size ) );

        LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
        mergePolicy.setNoCFSRatio( config.get( lucene_nocfs_ratio ) );
        mergePolicy.setMinMergeMB( config.get( lucene_min_merge ) );
        mergePolicy.setMergeFactor( config.get( lucene_merge_factor ) );
        writerConfig.setMergePolicy( mergePolicy );

        return writerConfig;
    }

    public static IndexWriterConfig population( Config config )
    {
        return population( config, KEYWORD_ANALYZER );
    }

    public static IndexWriterConfig population( Config config, Analyzer analyzer )
    {
        IndexWriterConfig writerConfig = standard( config, analyzer );
        writerConfig.setMaxBufferedDocs( config.get( lucene_population_max_buffered_docs ) );
        writerConfig.setRAMBufferSizeMB( config.get( lucene_population_ram_buffer_size ) );
        return writerConfig;
    }

    public static IndexWriterConfig transactionState( Config config, Analyzer analyzer )
    {
        IndexWriterConfig writerConfig = standard( config, analyzer );
        // Index transaction state is never directly persisted, so never commit it on close.
        writerConfig.setCommitOnClose( false );
        return writerConfig;
    }
}
