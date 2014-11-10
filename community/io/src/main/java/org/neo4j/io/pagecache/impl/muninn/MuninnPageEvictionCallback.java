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
package org.neo4j.io.pagecache.impl.muninn;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.io.pagecache.Page;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.jsr166e.StampedLock;

final class MuninnPageEvictionCallback implements PageEvictionCallback
{
    private final PrimitiveLongObjectMap<MuninnPage>[] translationTables;
    private final StampedLock[] translationTableLocks;

    public MuninnPageEvictionCallback(
            PrimitiveLongObjectMap<MuninnPage>[] translationTables,
            StampedLock[] translationTableLocks )
    {
        this.translationTables = translationTables;
        this.translationTableLocks = translationTableLocks;
    }

    @Override
    public void onEvict( long pageId, Page page )
    {
        int stripe = (int) (pageId & MuninnPagedFile.translationTableStripeMask);
        StampedLock translationTableLock = translationTableLocks[stripe];
        PrimitiveLongObjectMap<MuninnPage> translationTable = translationTables[stripe];

        // We use tryWriteLock here, because this call is in the way of
        // releasing new pages to the freelist. This means that threads might
        // be holding the translation table locks while they are in the middle
        // of a page fault, waiting for a free page to become available. In
        // that case, we would dead-lock with that thread, if we were to try
        // and take the lock for real.
        // On the other hand, it is perfectly fine for pinning threads to
        // discover that a translation table has gone stale. In that case they
        // will do a page fault, and fix the translation table themselves.
        // As such, doing this clean up on eviction is not strictly necessary,
        // though it helps keep the tables small.
        long stamp = translationTableLock.tryWriteLock();
        if ( stamp != 0 )
        {
            try
            {
                MuninnPage removed = translationTable.remove( pageId );
                assert removed == page: "Removed unexpected page when " +
                        "cleaning up translation table for filePageId " +
                        pageId + ". Evicted " + page + " but removed " +
                        removed + " from the " + "translation table.";
            }
            finally
            {
                translationTableLock.unlockWrite( stamp );
            }
        }
    }
}
