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
package org.neo4j.internal.id.indexed;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.id.IdGenerator.CommitMarker;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.util.concurrent.Work;
import org.neo4j.util.concurrent.WorkSync;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.test.Race.throwing;

/**
 * This test is great to have during development of transactional IDs. Although it's ignored because it's not clear what the assertions
 * should be. Now it can be used to see what happens in concurrent allocations, especially how free id scanner behaves in that regard.
 */
@Disabled
@PageCacheExtension
class LargeFreelistCreationDeletionIT
{
    private static final int THREADS = Runtime.getRuntime().availableProcessors();
    private static final int TRANSACTIONS_PER_THREAD = 1_000;
    private static final int ALLOCATIONS_PER_TRANSACTION = 50;
    private static final int ALLOCATIONS_PER_THREAD = TRANSACTIONS_PER_THREAD * ALLOCATIONS_PER_TRANSACTION;

    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    @Test
    void shouldAlternateLargeCreationsAndDeletionsAndNotLoseIds() throws Throwable
    {
        long[][] allocatedIds = new long[THREADS][];
        for ( int i = 0; i < THREADS; i++ )
        {
            allocatedIds[i] = new long[ALLOCATIONS_PER_THREAD];
        }

        for ( int r = 0; r < 3; r++ )
        {
            // Create
            try ( IndexedIdGenerator freelist =
                    new IndexedIdGenerator( pageCache, directory.file( "file.id" ), immediate(), IdType.NODE, 128, () -> 0, Long.MAX_VALUE ) )
            {
                // Make sure ID cache is filled so that initial allocations won't slide highId unnecessarily.
                freelist.maintenance();

                Race race = new Race();
                WorkSync<IndexedIdGenerator,Ids> workSync = new WorkSync<>( freelist );
                for ( int t = 0; t < THREADS; t++ )
                {
                    int thread = t;
                    race.addContestant( throwing( () ->
                    {
                        int cursor = 0;
                        for ( int i = 0; i < TRANSACTIONS_PER_THREAD; i++ )
                        {
                            long[] txIds = new long[ALLOCATIONS_PER_TRANSACTION];
                            for ( int a = 0; a < txIds.length; a++ )
                            {
                                long id = freelist.nextId();
                                allocatedIds[thread][cursor++] = id;
                                txIds[a] = id;
                            }
                            workSync.apply( new Ids( txIds ) );
                            Thread.sleep( 1 );
                        }
                    } ), 1 );
                }
                race.go();
                assertAllUnique( allocatedIds );

                // Delete
                try ( CommitMarker commitMarker = freelist.commitMarker() )
                {
                    for ( long[] perThread : allocatedIds )
                    {
                        for ( long id : perThread )
                        {
                            commitMarker.markDeleted( id );
                        }
                    }
                }

                // Checkpoint
                freelist.checkpoint( IOLimiter.UNLIMITED );
                System.out.println( freelist.getHighId() );
            }
        }
    }

    private static void assertAllUnique( long[][] allocatedIds )
    {
        MutableLongSet set = LongSets.mutable.empty();
        for ( long[] allocatedId : allocatedIds )
        {
            for ( long id : allocatedId )
            {
                assertTrue( set.add( id ) );
            }
        }
    }

    private static class Ids implements Work<IndexedIdGenerator,Ids>
    {
        private final List<long[]> idLists = new ArrayList<>();

        Ids( long[] ids )
        {
            idLists.add( ids );
        }

        @Override
        public Ids combine( Ids work )
        {
            idLists.addAll( work.idLists );
            return this;
        }

        @Override
        public void apply( IndexedIdGenerator freelist )
        {
            try ( CommitMarker commitMarker = freelist.commitMarker() )
            {
                for ( long[] ids : idLists )
                {
                    for ( long id : ids )
                    {
                        commitMarker.markUsed( id );
                    }
                }
            }
        }
    }
}
