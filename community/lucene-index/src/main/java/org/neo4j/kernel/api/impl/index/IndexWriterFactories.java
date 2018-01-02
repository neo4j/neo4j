/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import java.io.IOException;

import org.neo4j.index.impl.lucene.LuceneDataSource;
import org.neo4j.index.impl.lucene.MultipleBackupDeletionPolicy;

public final class IndexWriterFactories
{
    private IndexWriterFactories()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    public static IndexWriterFactory<ReservingLuceneIndexWriter> reserving()
    {
        return new IndexWriterFactory<ReservingLuceneIndexWriter>()
        {
            @Override
            public ReservingLuceneIndexWriter create( Directory directory ) throws IOException
            {
                return new ReservingLuceneIndexWriter( directory, standardConfig() );
            }
        };
    }

    public static IndexWriterFactory<LuceneIndexWriter> tracking()
    {
        return new IndexWriterFactory<LuceneIndexWriter>()
        {
            @Override
            public LuceneIndexWriter create( Directory directory ) throws IOException
            {
                return new TrackingLuceneIndexWriter( directory, standardConfig() );
            }
        };
    }

    public static IndexWriterFactory<LuceneIndexWriter> batchInsert( final IndexWriterConfig config )
    {
        return new IndexWriterFactory<LuceneIndexWriter>()
        {
            @Override
            public LuceneIndexWriter create( Directory directory ) throws IOException
            {
                return new TrackingLuceneIndexWriter( directory, config );
            }
        };
    }

    private static IndexWriterConfig standardConfig()
    {
        IndexWriterConfig writerConfig = new IndexWriterConfig( Version.LUCENE_36, LuceneDataSource.KEYWORD_ANALYZER );

        writerConfig.setMaxBufferedDocs( 100000 ); // TODO figure out depending on environment?
        writerConfig.setIndexDeletionPolicy( new MultipleBackupDeletionPolicy() );
        writerConfig.setTermIndexInterval( 14 );

        LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
        mergePolicy.setUseCompoundFile( true );
        mergePolicy.setNoCFSRatio( 1.0 );
        mergePolicy.setMinMergeMB( 0.1 );
        mergePolicy.setMergeFactor( 2 );
        writerConfig.setMergePolicy( mergePolicy );

        return writerConfig;
    }
}
