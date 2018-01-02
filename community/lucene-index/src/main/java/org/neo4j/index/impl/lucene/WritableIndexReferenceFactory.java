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
package org.neo4j.index.impl.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;

import static org.neo4j.index.impl.lucene.LuceneDataSource.LUCENE_VERSION;

class WritableIndexReferenceFactory extends IndexReferenceFactory
{
    WritableIndexReferenceFactory( LuceneDataSource.LuceneFilesystemFacade filesystemFacade, File baseStorePath,
            IndexTypeCache typeCache )
    {
        super( filesystemFacade, baseStorePath, typeCache );
    }

    @Override
    IndexReference createIndexReference( IndexIdentifier identifier ) throws IOException
    {
        IndexWriter writer = newIndexWriter( identifier );
        IndexReader reader = IndexReader.open( writer, true );
        IndexSearcher indexSearcher = newIndexSearcher( identifier, reader );
        return new WritableIndexReference( identifier, indexSearcher, writer );
    }

    /**
     * If nothing has changed underneath (since the searcher was last created or refreshed) same instance is
     * returned. But if something has changed refreshed index reference is returned. It makes use if the
     * {@link IndexReader#openIfChanged(IndexReader, IndexWriter, boolean)} which faster than opening an index from
     * scratch.
     *
     * @param indexReference index reference to refresh
     *
     * @return a refreshed version of the searcher or current if nothing has changed.
     * @throws RuntimeException if there's a problem with the index.
     */
    @Override
    IndexReference refresh( IndexReference indexReference )
    {
        try
        {
            IndexReader reader = indexReference.getSearcher().getIndexReader();
            IndexWriter writer = indexReference.getWriter();
            IndexReader reopened = IndexReader.openIfChanged( reader, writer, true );
            if ( reopened != null )
            {
                IndexSearcher newSearcher = newIndexSearcher( indexReference.getIdentifier(), reopened );
                indexReference.detachOrClose();
                return new WritableIndexReference( indexReference.getIdentifier(), newSearcher, writer );
            }
            return indexReference;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private IndexWriter newIndexWriter( IndexIdentifier identifier )
    {
        try
        {
            Directory indexDirectory = getIndexDirectory( identifier );
            IndexType type = getType( identifier );
            IndexWriterConfig writerConfig = new IndexWriterConfig( LUCENE_VERSION, type.analyzer );
            writerConfig.setIndexDeletionPolicy( new MultipleBackupDeletionPolicy() );
            Similarity similarity = type.getSimilarity();
            if ( similarity != null )
            {
                writerConfig.setSimilarity( similarity );
            }
            return new IndexWriter( indexDirectory, writerConfig );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
