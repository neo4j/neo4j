/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.newapi;

import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.storageengine.api.lock.LockTracer;
import org.neo4j.storageengine.api.schema.IndexReader;

import static org.neo4j.kernel.impl.locking.ResourceTypes.INDEX_ENTRY;
import static org.neo4j.kernel.impl.locking.ResourceTypes.indexEntryResourceId;

public class LockingNodeUniqueIndexSeek
{
    public static <CURSOR extends NodeValueIndexCursor> long apply( Locks.Client locks,
                                                                    LockTracer lockTracer,
                                                                    Supplier<CURSOR> cursors,
                                                                    UniqueNodeIndexSeeker<CURSOR> nodeIndexSeeker,
                                                                    Read read,
                                                                    IndexReference index,
                                                                    IndexQuery.ExactPredicate... predicates )
            throws IndexNotApplicableKernelException, IndexNotFoundKernelException
    {
        int[] entityTokenIds = index.schema().getEntityTokenIds();
        if ( entityTokenIds.length != 1 )
        {
            throw new IndexNotApplicableKernelException( "Multi-token index " + index + " does not support uniqueness." );
        }
        long indexEntryId = indexEntryResourceId( entityTokenIds[0], predicates );

        //First try to find node under a shared lock
        //if not found upgrade to exclusive and try again
        locks.acquireShared( lockTracer, INDEX_ENTRY, indexEntryId );
        try ( CURSOR cursor = cursors.get();
              IndexReaders readers = new IndexReaders( index, read ) )
        {
            nodeIndexSeeker.nodeIndexSeekWithFreshIndexReader( cursor, readers.createReader(), predicates );
            if ( !cursor.next() )
            {
                locks.releaseShared( INDEX_ENTRY, indexEntryId );
                locks.acquireExclusive( lockTracer, INDEX_ENTRY, indexEntryId );
                nodeIndexSeeker.nodeIndexSeekWithFreshIndexReader( cursor, readers.createReader(), predicates );
                if ( cursor.next() ) // we found it under the exclusive lock
                {
                    // downgrade to a shared lock
                    locks.acquireShared( lockTracer, INDEX_ENTRY, indexEntryId );
                    locks.releaseExclusive( INDEX_ENTRY, indexEntryId );
                }
            }

            return cursor.nodeReference();
        }
    }

    @FunctionalInterface
    interface UniqueNodeIndexSeeker<CURSOR extends NodeValueIndexCursor>
    {
        void nodeIndexSeekWithFreshIndexReader( CURSOR cursor,
                                                IndexReader indexReader,
                                                IndexQuery.ExactPredicate... predicates ) throws IndexNotApplicableKernelException;
    }
}
