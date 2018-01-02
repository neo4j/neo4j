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
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;

/**
 * Index writer that supports a concept of reservations {@see org.neo4j.kernel.api.index.Reservation}.
 * Reservation via {@link org.neo4j.kernel.api.impl.index.ReservingLuceneIndexWriter#reserveInsertions(int)} should
 * precede any operation that modifies size of the index like insert or update (atomic delete + insert in Lucene).
 * This index writer is intended to be used for online index at runtime.
 */
class ReservingLuceneIndexWriter extends LuceneIndexWriter
{
    private final AtomicLong reservedDocs = new AtomicLong();

    ReservingLuceneIndexWriter( Directory directory, IndexWriterConfig config ) throws IOException
    {
        super( directory, config );
    }

    synchronized void reserveInsertions( int insertionsCount ) throws IOException, IndexCapacityExceededException
    {
        if ( insertionsCount <= 0 )
        {
            return;
        }

        if ( totalNumberOfDocumentsExceededLuceneCapacity( insertionsCount ) )
        {
            // maxDoc is about to overflow, let's try to save our index

            // try to merge deletes, maybe this will fix maxDoc
            writer.forceMergeDeletes();

            if ( totalNumberOfDocumentsExceededLuceneCapacity( insertionsCount ) )
            {
                // if it did not then merge everything in single segment - horribly slow, last resort
                writer.forceMerge( 1 );

                if ( totalNumberOfDocumentsExceededLuceneCapacity( insertionsCount ) )
                {
                    // merging did not help - throw exception
                    throw new IndexCapacityExceededException( insertionsCount, writer.maxDoc(), maxDocLimit() );
                }
            }
        }

        // everything fine - able to reserve 'space' for new documents
        reservedDocs.addAndGet( insertionsCount );
    }

    void removeReservedInsertions( int insertionsCount )
    {
        if ( insertionsCount > 0 )
        {
            reservedDocs.addAndGet( -insertionsCount );
        }
    }

    private boolean totalNumberOfDocumentsExceededLuceneCapacity( int newAdditions )
    {
        return (reservedDocs.get() + writer.maxDoc() + newAdditions) >= maxDocLimit();
    }
}
