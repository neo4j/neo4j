/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;

import org.neo4j.index.impl.lucene.legacy.LuceneDataSource;
import org.neo4j.index.impl.lucene.legacy.MultipleBackupDeletionPolicy;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

public final class IndexWriterFactories
{

    private static final int MAX_BUFFERED_DOCS =
            FeatureToggles.getInteger( IndexWriterFactories.class, "max_buffered_docs", 100000 );
    private static final int MERGE_POLICY_MERGE_FACTOR =
            FeatureToggles.getInteger( IndexWriterFactories.class, "merge.factor", 2 );
    private static final double MERGE_POLICY_NO_CFS_RATIO =
            FeatureToggles.getDouble( IndexWriterFactories.class, "nocfs.ratio.", 1.0 );
    private static final double MERGE_POLICY_MIN_MERGE_MB =
            FeatureToggles.getDouble( IndexWriterFactories.class, "min.merge", 0.1 );

    private IndexWriterFactories()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    public static IndexWriterFactory<LuceneIndexWriter> standard()
    {
        return directory -> new LuceneIndexWriter( directory, standardConfig() );
    }

    public static IndexWriterFactory<LuceneIndexWriter> batchInsert( final IndexWriterConfig config )
    {
        return directory -> new LuceneIndexWriter( directory, config );
    }

    private static IndexWriterConfig standardConfig()
    {
        IndexWriterConfig writerConfig = new IndexWriterConfig( LuceneDataSource.KEYWORD_ANALYZER );

        writerConfig.setMaxBufferedDocs( MAX_BUFFERED_DOCS ); // TODO figure out depending on environment?
        writerConfig.setIndexDeletionPolicy( new MultipleBackupDeletionPolicy() );
        writerConfig.setUseCompoundFile( true );

        // TODO: TieredMergePolicy & possibly SortingMergePolicy
        LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
        mergePolicy.setNoCFSRatio( MERGE_POLICY_NO_CFS_RATIO );
        mergePolicy.setMinMergeMB( MERGE_POLICY_MIN_MERGE_MB );
        mergePolicy.setMergeFactor( MERGE_POLICY_MERGE_FACTOR );
        writerConfig.setMergePolicy( mergePolicy );

        return writerConfig;
    }
}
