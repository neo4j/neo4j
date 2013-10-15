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

import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;

import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.UniquePropertyIndexUpdater;

class UniqueLuceneIndexAccessor extends LuceneIndexAccessor implements UniquePropertyIndexUpdater.Lookup
{
    public UniqueLuceneIndexAccessor( LuceneDocumentStructure documentStructure,
                                      LuceneIndexWriterFactory indexWriterFactory, IndexWriterStatus writerStatus,
                                      DirectoryFactory dirFactory, File dirFile ) throws IOException
    {
        super( documentStructure, indexWriterFactory, writerStatus, dirFactory, dirFile );
    }

    @Override
    public IndexUpdater newUpdater( final IndexUpdateMode mode ) throws IOException
    {
        return new UniquePropertyIndexUpdater( this ) {

            final IndexUpdater delegate = UniqueLuceneIndexAccessor.super.newUpdater( mode );

            @Override
            protected void flushUpdates( Iterable<NodePropertyUpdate> updates )
                    throws IOException, IndexEntryConflictException
            {
                for ( NodePropertyUpdate update : updates )
                {
                    delegate.process( update );
                    delegate.close();
                }
            }
        };
    }

    public Long currentlyIndexedNode( Object value ) throws IOException
    {
        IndexSearcher searcher = searcherManager.acquire();
        try
        {
            TopDocs docs = searcher.search( documentStructure.newQuery( value ), 1 );
            if ( docs.scoreDocs.length > 0 )
            {
                Document doc = searcher.getIndexReader().document( docs.scoreDocs[0].doc );
                return documentStructure.getNodeId( doc );
            }
        }
        finally
        {
            searcherManager.release( searcher );
        }
        return null;
    }
}
