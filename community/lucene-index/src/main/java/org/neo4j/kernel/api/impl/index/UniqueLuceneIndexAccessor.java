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

import org.apache.lucene.search.IndexSearcher;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.Reservation;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.UniquePropertyIndexUpdater;

/**
 * Variant of {@link LuceneIndexAccessor} that also verifies uniqueness constraints.
 */
class UniqueLuceneIndexAccessor extends LuceneIndexAccessor
{
    public UniqueLuceneIndexAccessor( LuceneDocumentStructure documentStructure,
            boolean readOnly, IndexWriterFactory<ReservingLuceneIndexWriter> indexWriterFactory,
            DirectoryFactory dirFactory, File indexFolder ) throws IOException
    {
        super( documentStructure, readOnly, indexWriterFactory, dirFactory, indexFolder, -1 /* unused */ );
    }

    @Override
    public IndexUpdater newUpdater( final IndexUpdateMode mode )
    {
        return new LuceneUniquePropertyIndexUpdater( super.newUpdater( mode ) );
    }

    @Override
    protected IndexReader makeNewReader( IndexSearcher searcher, Closeable closeable, CancellationRequest cancellation )
    {
        return new LuceneUniqueIndexAccessorReader( searcher, documentStructure, closeable, cancellation );
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
            this.delegate = delegate;
        }

        @Override
        protected void flushUpdates( Iterable<NodePropertyUpdate> updates )
                throws IOException, IndexEntryConflictException, IndexCapacityExceededException
        {
            for ( NodePropertyUpdate update : updates )
            {
                delegate.process( update );
            }
            delegate.close();
        }

        @Override
        public Reservation validate( Iterable<NodePropertyUpdate> updates )
                throws IOException, IndexCapacityExceededException
        {
            return delegate.validate( updates );
        }

        @Override
        public void remove( PrimitiveLongSet nodeIds ) throws IOException
        {
            delegate.remove( nodeIds );
        }
    }
}
