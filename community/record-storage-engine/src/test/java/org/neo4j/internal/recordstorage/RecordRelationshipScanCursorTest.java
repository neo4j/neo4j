/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.RandomSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;

@PageCacheExtension
@Neo4jLayoutExtension
@ExtendWith( RandomExtension.class )
class RecordRelationshipScanCursorTest
{
    private static final long RELATIONSHIP_ID = 1L;

    @Inject
    private RandomSupport random;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;
    @Inject
    private RecordDatabaseLayout databaseLayout;

    private NeoStores neoStores;
    private StoreCursors storeCursors;

    @AfterEach
    void tearDown()
    {
        closeAllUnchecked( storeCursors, neoStores );
    }

    @BeforeEach
    void setUp()
    {
        StoreFactory storeFactory = getStoreFactory();
        neoStores = storeFactory.openAllNeoStores( true );
        storeCursors = new CachedStoreCursors( neoStores, NULL );
    }

    @Test
    void retrieveUsedRelationship()
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        try ( var writeCursor = storeCursors.writeCursor( RELATIONSHIP_CURSOR ) )
        {
            createRelationshipRecord( RELATIONSHIP_ID, 1, relationshipStore, writeCursor, true );
        }

        try ( RecordRelationshipScanCursor cursor = createRelationshipCursor() )
        {
            cursor.single( RELATIONSHIP_ID );
            assertTrue( cursor.next() );
            assertEquals( RELATIONSHIP_ID, cursor.entityReference() );
        }
    }

    @Test
    void retrieveUnusedRelationship()
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        relationshipStore.setHighId( 10 );
        try ( var writeCursor = storeCursors.writeCursor( RELATIONSHIP_CURSOR ) )
        {
            createRelationshipRecord( RELATIONSHIP_ID, 1, relationshipStore, writeCursor, false );
        }

        try ( RecordRelationshipScanCursor cursor = createRelationshipCursor() )
        {
            cursor.single( RELATIONSHIP_ID );
            assertFalse( cursor.next() );
        }
    }

    @Test
    void shouldScanAllInUseRelationships()
    {
        // given
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        int count = 100;
        relationshipStore.setHighId( count );
        Set<Long> expected = new HashSet<>();
        try ( var cursor = storeCursors.writeCursor( RELATIONSHIP_CURSOR ) )
        {
            for ( long id = 0; id < count; id++ )
            {
                boolean inUse = random.nextBoolean();
                createRelationshipRecord( id, 1, relationshipStore, cursor, inUse );
                if ( inUse )
                {
                    expected.add( id );
                }
            }
        }

        // when
        assertSeesRelationships( expected );
    }

    private void assertSeesRelationships( Set<Long> expected )
    {
        try ( RecordRelationshipScanCursor cursor = createRelationshipCursor() )
        {
            cursor.scan();
            while ( cursor.next() )
            {
                // then
                assertTrue( expected.remove( cursor.entityReference() ), cursor.toString() );
            }
        }
        assertTrue( expected.isEmpty() );
    }

    private static void createRelationshipRecord( long id, int type, RelationshipStore relationshipStore, PageCursor pageCursor, boolean used )
    {
        relationshipStore.updateRecord( new RelationshipRecord( id ).initialize( used, -1, 1, 2, type, -1, -1, -1, -1, true, true ), pageCursor, NULL,
                StoreCursors.NULL );
    }

    private StoreFactory getStoreFactory()
    {
        return new StoreFactory( databaseLayout, Config.defaults(), new DefaultIdGeneratorFactory( fileSystem, immediate(), databaseLayout.getDatabaseName() ),
                pageCache, fileSystem, NullLogProvider.getInstance(), PageCacheTracer.NULL, writable() );
    }

    private RecordRelationshipScanCursor createRelationshipCursor()
    {
        return new RecordRelationshipScanCursor( neoStores.getRelationshipStore(), NULL );
    }
}
