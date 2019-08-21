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

import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGenerator.CommitMarker;
import org.neo4j.internal.id.IdGenerator.ReuseMarker;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.id.FreeIds.NO_FREE_IDS;
import static org.neo4j.io.pagecache.IOLimiter.UNLIMITED;
import static org.neo4j.test.rule.PageCacheConfig.config;

@EphemeralPageCacheExtension
class IndexedIdGeneratorRecoverabilityTest
{
    private static final IdType ID_TYPE = IdType.LABEL_TOKEN;

    private static final String ID_FILE_NAME = "some.id";

    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Test
    void persistHighIdBetweenCleanRestarts()
    {
        try ( IdGenerator freelist = instantiateFreelist() )
        {
            freelist.nextId();
            assertEquals( 1, freelist.getHighId() );
            freelist.nextId();
            assertEquals( 2, freelist.getHighId() );
            freelist.checkpoint( UNLIMITED );
        }
        try ( IdGenerator freelist = instantiateFreelist() )
        {
            assertEquals( 2, freelist.getHighId() );
        }
    }

    @Test
    void doNotPersistHighIdBetweenCleanRestartsWithoutCheckpoint()
    {
        try ( IdGenerator freelist = instantiateFreelist() )
        {
            freelist.nextId();
            assertEquals( 1, freelist.getHighId() );
            freelist.nextId();
            assertEquals( 2, freelist.getHighId() );
        }
        try ( IdGenerator freelist = instantiateFreelist() )
        {
            assertEquals( 0, freelist.getHighId() );
        }
    }

    @Test
    void simpleCrashTest() throws Exception
    {
        final EphemeralFileSystemAbstraction snapshot;
        final long id1;
        final long id2;
        try ( IdGenerator freelist = instantiateFreelist() )
        {
            id1 = freelist.nextId();
            id2 = freelist.nextId();
            markUsed( freelist, id1, id2 );
            freelist.checkpoint( UNLIMITED );
            markDeleted( freelist, id1, id2 );
            pageCache.flushAndForce();
            snapshot = fs.snapshot();
        }

        try ( PageCache newPageCache = getPageCache( snapshot );
                IdGenerator freelist = instantiateFreelist() )
        {
            markDeleted( freelist, id1, id2 );

            // Recovery is completed ^^^
            freelist.start( NO_FREE_IDS );
            markFree( freelist, id1, id2 );

            final ImmutableLongSet reused = LongSets.immutable.of( freelist.nextId(), freelist.nextId() );
            assertEquals( LongSets.immutable.of( id1, id2 ), reused, "IDs are not reused" );
        }
        finally
        {
            snapshot.close();
        }
    }

    @Test
    void resetUsabilityOnRestart() throws IOException
    {
        // Create the freelist
        try ( IdGenerator freelist = instantiateFreelist() )
        {
            freelist.checkpoint( UNLIMITED );
        }

        final long id1;
        final long id2;
        try ( IdGenerator freelist = instantiateFreelist() )
        {
            id1 = freelist.nextId();
            id2 = freelist.nextId();
            markUsed( freelist, id1, id2 );
            markDeleted( freelist, id1, id2 );
            freelist.checkpoint( UNLIMITED );
        }

        try ( IdGenerator freelist = instantiateFreelist() )
        {
            freelist.start( NO_FREE_IDS );
            final ImmutableLongSet reused = LongSets.immutable.of( freelist.nextId(), freelist.nextId() );
            assertEquals( LongSets.immutable.of( id1, id2 ), reused, "IDs are not reused" );
        }
    }

    @Test
    void resetUsabilityOnRestartWithSomeWrites() throws IOException
    {
        // Create the freelist
        try ( IdGenerator freelist = instantiateFreelist() )
        {
            freelist.checkpoint( UNLIMITED );
        }

        final long id1;
        final long id2;
        final long id3;
        try ( IdGenerator freelist = instantiateFreelist() )
        {
            id1 = freelist.nextId();
            id2 = freelist.nextId();
            id3 = freelist.nextId();
            markUsed( freelist, id1, id2, id3 );
            markDeleted( freelist, id1, id2 ); // <-- Don't delete id3
            // Intentionally don't mark the ids as reusable
            freelist.checkpoint( UNLIMITED );
        }

        try ( IdGenerator freelist = instantiateFreelist() )
        {
            freelist.start( NO_FREE_IDS );

            // Here we expected that id1 and id2 will be reusable, even if they weren't marked as such in the previous session
            // Making changes to the tree entry where they live will update the generation and all of a sudden the reusable bits
            // in that entry will matter when we want to allocate. This is why we now want to make a change to that tree entry
            // and after that do an allocation to see if we still get them.
            markDeleted( freelist, id3 );

            final ImmutableLongSet reused = LongSets.immutable.of( freelist.nextId(), freelist.nextId() );
            assertEquals( LongSets.immutable.of( id1, id2 ), reused, "IDs are not reused" );
        }
    }

    @Test
    void avoidNormalizationDuringRecovery() throws IOException
    {
        long id;
        long neighbourId;
        try ( IdGenerator freelist = instantiateFreelist() )
        {
            freelist.start( NO_FREE_IDS );
            id = freelist.nextId();
            neighbourId = freelist.nextId();
            markUsed( freelist, id, neighbourId );
            markDeleted( freelist, id, neighbourId );
            // Crash (no checkpoint)
        }

        try ( IdGenerator freelist = instantiateFreelist() )
        {
            // Recovery
            markUsed( freelist, id, neighbourId );
            markDeleted( freelist, id, neighbourId );
            // Neo4j does this on recovery, setHighId and checkpoint
            freelist.setHighId( neighbourId + 1 );
            freelist.checkpoint( UNLIMITED ); // mostly to get the generation persisted

            // Normal operations
            freelist.start( NO_FREE_IDS );
            markFree( freelist, id );
            long idAfterRecovery = freelist.nextId();
            assertEquals( id, idAfterRecovery );
            markUsed( freelist, id );
        }

        try ( IdGenerator freelist = instantiateFreelist() )
        {
            // Recovery
            // If normalization happens on recovery then this transition, which really should be DELETED (last check-pointed state) -> USED
            // instead becomes normalized from DELETED -> FREE and the real transition becomes FREE -> RESERVED
            markUsed( freelist, id );

            // Normal operations
            freelist.start( NO_FREE_IDS );
            markDeleted( freelist, id ); // <-- this must be OK

            // And as an extra measure of verification
            markFree( freelist, id );
            MutableLongSet expected = LongSets.mutable.with( id, neighbourId );
            assertTrue( expected.remove( freelist.nextId() ) );
            assertTrue( expected.remove( freelist.nextId() ) );
            assertTrue( expected.isEmpty() );
        }
    }

    private IndexedIdGenerator instantiateFreelist()
    {
        return new IndexedIdGenerator( pageCache, testDirectory.file( ID_FILE_NAME ), immediate(), ID_TYPE, 128, () -> 0, Long.MAX_VALUE );
    }

    private static PageCache getPageCache( FileSystemAbstraction fs )
    {
        return new PageCacheRule().getPageCache( fs, config() );
    }

    private static void markUsed( IdGenerator freelist, long... ids )
    {
        try ( CommitMarker commitMarker = freelist.commitMarker() )
        {
            for ( long id : ids )
            {
                commitMarker.markUsed( id );
            }
        }
    }

    private static void markDeleted( IdGenerator freelist, long... ids )
    {
        try ( CommitMarker commitMarker = freelist.commitMarker() )
        {
            for ( long id : ids )
            {
                commitMarker.markDeleted( id );
            }
        }
    }

    private static void markFree( IdGenerator freelist, long... ids )
    {
        try ( ReuseMarker reuseMarker = freelist.reuseMarker() )
        {
            for ( long id : ids )
            {
                reuseMarker.markFree( id );
            }
        }
    }
}
