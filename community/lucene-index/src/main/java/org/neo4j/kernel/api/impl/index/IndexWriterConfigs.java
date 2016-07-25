/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.blocktreeords.BlockTreeOrdsPostingsFormat;
import org.apache.lucene.codecs.lucene54.Lucene54Codec;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;

import org.neo4j.index.impl.lucene.legacy.LuceneDataSource;
import org.neo4j.index.impl.lucene.legacy.MultipleBackupDeletionPolicy;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

/**
 * Helper factory for standard lucene index writer configuration.
 */
public final class IndexWriterConfigs
{
    private static final int MAX_BUFFERED_DOCS =
            FeatureToggles.getInteger( IndexWriterConfigs.class, "max_buffered_docs", 100000 );
    private static final int MERGE_POLICY_MERGE_FACTOR =
            FeatureToggles.getInteger( IndexWriterConfigs.class, "merge.factor", 2 );
    private static final double MERGE_POLICY_NO_CFS_RATIO =
            FeatureToggles.getDouble( IndexWriterConfigs.class, "nocfs.ratio", 1.0 );
    private static final double MERGE_POLICY_MIN_MERGE_MB =
            FeatureToggles.getDouble( IndexWriterConfigs.class, "min.merge", 0.1 );
    private static final boolean CODEC_BLOCK_TREE_ORDS_POSTING_FORMAT =
            FeatureToggles.flag( IndexWriterConfigs.class, "block.tree.ords.posting.format", true );

    private static final int POPULATION_RAM_BUFFER_SIZE_MB =
            FeatureToggles.getInteger( IndexWriterConfigs.class, "population.ram.buffer.size", 50 );

    /**
     * Default postings format for schema and label scan store indexes.
     */
    private static final BlockTreeOrdsPostingsFormat blockTreeOrdsPostingsFormat = new BlockTreeOrdsPostingsFormat();

    private IndexWriterConfigs()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    public static IndexWriterConfig standard()
    {
        IndexWriterConfig writerConfig = new IndexWriterConfig( LuceneDataSource.KEYWORD_ANALYZER );

        writerConfig.setMaxBufferedDocs( MAX_BUFFERED_DOCS );
        writerConfig.setIndexDeletionPolicy( new MultipleBackupDeletionPolicy() );
        writerConfig.setUseCompoundFile( true );
        writerConfig.setCodec(new Lucene54Codec()
        {
            @Override
            public PostingsFormat getPostingsFormatForField( String field )
            {
                PostingsFormat postingFormat = super.getPostingsFormatForField( field );
                return CODEC_BLOCK_TREE_ORDS_POSTING_FORMAT ? blockTreeOrdsPostingsFormat : postingFormat;
            }
        });

        LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
        mergePolicy.setNoCFSRatio( MERGE_POLICY_NO_CFS_RATIO );
        mergePolicy.setMinMergeMB( MERGE_POLICY_MIN_MERGE_MB );
        mergePolicy.setMergeFactor( MERGE_POLICY_MERGE_FACTOR );
        writerConfig.setMergePolicy( mergePolicy );

        return writerConfig;
    }

    public static IndexWriterConfig population()
    {
        IndexWriterConfig writerConfig = standard();
        writerConfig.setMaxBufferedDocs( IndexWriterConfig.DISABLE_AUTO_FLUSH );
        writerConfig.setRAMBufferSizeMB( POPULATION_RAM_BUFFER_SIZE_MB );
        return writerConfig;
    }
}
