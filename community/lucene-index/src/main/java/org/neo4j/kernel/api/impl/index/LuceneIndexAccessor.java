/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;

import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

import static org.neo4j.kernel.api.impl.index.DirectorySupport.deleteDirectoryContents;

abstract class LuceneIndexAccessor implements IndexAccessor
{
    protected final LuceneDocumentStructure documentStructure;
    protected final SearcherManager searcherManager;
    protected final IndexWriter writer;
    private final IndexWriterStatus writerStatus;
    private final Directory dir;

    LuceneIndexAccessor( LuceneDocumentStructure documentStructure, LuceneIndexWriterFactory indexWriterFactory,
                         IndexWriterStatus writerStatus, DirectoryFactory dirFactory, File dirFile )
            throws IOException
    {
        this.documentStructure = documentStructure;
        this.dir = dirFactory.open( dirFile );
        this.writer = indexWriterFactory.create( dir );
        this.writerStatus = writerStatus;
        this.searcherManager = new SearcherManager( writer, true, new SearcherFactory() );
    }

    @Override
    public void drop() throws IOException
    {
        closeIndexResources();
        deleteDirectoryContents( dir );
    }

    @Override
    public abstract void updateAndCommit( Iterable<NodePropertyUpdate> updates ) throws IOException, IndexEntryConflictException;

    @Override
    public abstract void recover( Iterable<NodePropertyUpdate> updates ) throws IOException;

    @Override
    public void force() throws IOException
    {
        writerStatus.commitAsOnline( writer );
    }

    @Override
    public void close() throws IOException
    {
        closeIndexResources();
        dir.close();
    }

    private void closeIndexResources() throws IOException
    {
        writerStatus.close( writer );
        searcherManager.close();
    }

    @Override
    public IndexReader newReader()
    {
        return new LuceneIndexAccessorReader( searcherManager, documentStructure );
    }
}
