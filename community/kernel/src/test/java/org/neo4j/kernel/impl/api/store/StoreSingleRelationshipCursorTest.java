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
package org.neo4j.kernel.impl.api.store;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.DefaultFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class StoreSingleRelationshipCursorTest
{

    private static final long RELATIONSHIP_ID = 1L;

    private final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( pageCacheRule )
            .around( fileSystemRule );

    private NeoStores neoStores;

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
        neoStores = storeFactory.openNeoStores( true, NeoStores.StoreType.RELATIONSHIP_GROUP,
                NeoStores.StoreType.RELATIONSHIP );
    }

    @Test
    public void retrieveUsedRelationship() throws Exception
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        createRelationshipRecrod( relationshipStore, true );

        try ( StoreSingleRelationshipCursor cursor = createRelationshipCursor() )
        {
            cursor.init( RELATIONSHIP_ID );
            assertTrue( cursor.next() );
            assertEquals( RELATIONSHIP_ID, cursor.get().id() );
        }
    }

    @Test
    public void impossibleToRetrieveUnusedRelationship()
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        createRelationshipRecrod( relationshipStore, false );

        try ( StoreSingleRelationshipCursor cursor = createRelationshipCursor() )
        {
            cursor.init( RELATIONSHIP_ID );
            assertFalse( cursor.next() );
        }
    }

    private void createRelationshipRecrod( RelationshipStore relationshipStore, boolean used )
    {
        relationshipStore.forceUpdateRecord(
                new RelationshipRecord( RELATIONSHIP_ID, used, 1, 2, 1, -1, -1, -1, -1, true, true ) );
    }

    private StoreFactory getStoreFactory()
    {
        return new StoreFactory( fileSystemRule.get(), testDirectory.directory(),
                pageCacheRule.getPageCache( fileSystemRule.get() ), NullLogProvider.getInstance() );
    }

    private StoreSingleRelationshipCursor createRelationshipCursor()
    {
        StoreStatement storeStatement = mock( StoreStatement.class );
        InstanceCache<StoreSingleRelationshipCursor> instanceCache = new TestCursorCache();
        return new StoreSingleRelationshipCursor( new RelationshipRecord( -1 ), neoStores, storeStatement,
                instanceCache, LockService.NO_LOCK_SERVICE );
    }

    private class TestCursorCache extends InstanceCache<StoreSingleRelationshipCursor>
    {
        @Override
        protected StoreSingleRelationshipCursor create()
        {
            return null;
        }
    }
}