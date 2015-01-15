/**
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

    public static LuceneIndexWriterFactory reserving()
    {
        return new LuceneIndexWriterFactory()
        {
            @Override
            public LuceneIndexWriter create( Directory directory ) throws IOException
            {
                return new ReservingLuceneIndexWriter( directory, standardConfig() );
            }
        };
    }

    public static LuceneIndexWriterFactory tracking()
    {
        return new LuceneIndexWriterFactory()
        {
            @Override
            public LuceneIndexWriter create( Directory directory ) throws IOException
            {
                return new TrackingLuceneIndexWriter( directory, standardConfig() );
            }
        };
    }

    public static LuceneIndexWriterFactory batchInsert( final IndexWriterConfig config )
    {
        return new LuceneIndexWriterFactory()
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
        return writerConfig;
    }
}
