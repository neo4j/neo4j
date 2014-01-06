/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
    public IndexUpdater newUpdater( final IndexUpdateMode mode )
    {
        if ( mode != IndexUpdateMode.RECOVERY )
        {
            return new LuceneUniquePropertyIndexUpdater( super.newUpdater( mode ) );
        }
        else
        {
            /* If we are in recovery, don't handle the business logic of validating uniqueness. */
            return super.newUpdater( mode );
        }
    }

    @Override
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

    /* The fact that this is here is a sign of a design error, and we should revisit and
     * remove this later on. Specifically, this is here because the unique indexes do validation
     * of uniqueness, which they really shouldn't be doing. In fact, they shouldn't exist, the unique
     * indexes are just indexes, and the logic of how they are used is not the responsibility of the
     * storage system to handle, that should go in the kernel layer.
     *
     * Anyway, where was I.. right: The kernel depends on the unique indexes to handle the business
     * logic of verifying domain uniqueness, and if they did not do that, race conditions appear for
     * index creation (not online operations, note) where concurrent violation of a currently created
     * index may break uniqueness.
     *
     * Phew. So, unique indexes currently have to pick up the slack here. The problem is that while
     * they serve the role of business logic execution, they also happen to be indexes, which is part
     * of the storage layer. There is one golden rule in the storage layer, which must never ever be
     * violated: Operations are idempotent. All operations against the storage layer have to be
     * executable over and over and have the same result, this is the basis of data recovery and
     * system consistency.
     *
     * Clearly, the uniqueness indexes don't do this, and so they fail in fulfilling their primary
     * contract in order to pick up the slack for the kernel not fulfilling it's contract. We hack
     * around this issue by tracking state - we know that probably the only time the idempotent
     * requirement will be invoked is during recovery, and we know that by happenstance, recovery is
     * single-threaded. As such, when we are in recovery, we turn off the business logic part and
     * correctly fulfill our actual contract. As soon as the database is online, we flip back to
     * running business logic in the storage layer and incorrectly implementing the storage layer
     * contract.
     *
     * One day, we should fix this.
     */
    private class LuceneUniquePropertyIndexUpdater extends UniquePropertyIndexUpdater
    {
        final IndexUpdater delegate;

        public LuceneUniquePropertyIndexUpdater( IndexUpdater delegate )
        {
            super( UniqueLuceneIndexAccessor.this );
            this.delegate = delegate;
        }

        @Override
        protected void flushUpdates( Iterable<NodePropertyUpdate> updates )
                throws IOException, IndexEntryConflictException
        {
            for ( NodePropertyUpdate update : updates )
            {
                delegate.process( update );
            }
            delegate.close();
        }

        @Override
        public void remove( Iterable<Long> nodeIds ) throws IOException
        {
            delegate.remove( nodeIds );
        }
    }
}
