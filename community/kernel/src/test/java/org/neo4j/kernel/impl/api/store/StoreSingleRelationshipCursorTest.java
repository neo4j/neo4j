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
package org.neo4j.kernel.impl.api.store;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class StoreSingleRelationshipCursorTest
{

    private static final long RELATIONSHIP_ID = 1L;

    private NeoStores neoStores;
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( pageCacheRule )
            .around( fileSystemRule );

    @After
    public void tearDown()
    {
        if ( neoStores != null )
        {
            neoStores.close();
        }
    }

    @Before
    public void setUp()
    {
        StoreFactory storeFactory = getStoreFactory();
        neoStores = storeFactory.openAllNeoStores( true );
    }

    @Test
    public void retrieveUsedRelationship()
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        createRelationshipRecord( relationshipStore, true );

        try ( StoreSingleRelationshipCursor cursor = createRelationshipCursor() )
        {
            cursor.init( RELATIONSHIP_ID );
            assertTrue( cursor.next() );
            assertEquals( RELATIONSHIP_ID, cursor.get().id() );
        }
    }

    @Test
    public void retrieveUnusedRelationship()
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        relationshipStore.setHighId( 10 );
        createRelationshipRecord( relationshipStore, false );

        try ( StoreSingleRelationshipCursor cursor = createRelationshipCursor() )
        {
            cursor.init( RELATIONSHIP_ID );
            assertFalse( cursor.next() );
        }
    }

    private void createRelationshipRecord( RelationshipStore relationshipStore, boolean used )
    {
       relationshipStore.updateRecord(
                new RelationshipRecord( RELATIONSHIP_ID, used, 1, 2, 1, -1, -1, -1, -1, true, true ) );
    }

    private StoreFactory getStoreFactory()
    {
        return new StoreFactory(
                testDirectory.directory(), Config.defaults(), new DefaultIdGeneratorFactory( fileSystemRule.get() ),
                pageCacheRule.getPageCache( fileSystemRule.get() ), fileSystemRule.get(),
                NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
    }

    private StoreSingleRelationshipCursor createRelationshipCursor()
    {
        InstanceCache<StoreSingleRelationshipCursor> instanceCache = mock(InstanceCache.class);
        return new StoreSingleRelationshipCursor( new RelationshipRecord( -1 ), instanceCache,
                new RecordCursors( neoStores ), LockService.NO_LOCK_SERVICE );
    }
}
