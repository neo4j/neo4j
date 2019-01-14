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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.kernel.api.Read.ANY_RELATIONSHIP_TYPE;

public class RecordRelationshipScanCursorTest
{
    private static final long RELATIONSHIP_ID = 1L;

    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule().with( new DefaultFileSystemRule() );
    @Rule
    public final RandomRule random = new RandomRule();

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
        neoStores = storeFactory.openAllNeoStores( true );
    }

    @Test
    public void retrieveUsedRelationship()
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        createRelationshipRecord( RELATIONSHIP_ID, 1, relationshipStore, true );

        try ( RecordRelationshipScanCursor cursor = createRelationshipCursor() )
        {
            cursor.single( RELATIONSHIP_ID );
            assertTrue( cursor.next() );
            assertEquals( RELATIONSHIP_ID, cursor.entityReference() );
        }
    }

    @Test
    public void retrieveUnusedRelationship()
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        relationshipStore.setHighId( 10 );
        createRelationshipRecord( RELATIONSHIP_ID, 1, relationshipStore, false );

        try ( RecordRelationshipScanCursor cursor = createRelationshipCursor() )
        {
            cursor.single( RELATIONSHIP_ID );
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void shouldScanAllInUseRelationships()
    {
        // given
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        int count = 100;
        relationshipStore.setHighId( count );
        Set<Long> expected = new HashSet<>();
        for ( long id = 0; id < count; id++ )
        {
            boolean inUse = random.nextBoolean();
            createRelationshipRecord( id, 1, relationshipStore, inUse );
            if ( inUse )
            {
                expected.add( id );
            }
        }

        // when
        assertSeesRelationships( expected, ANY_RELATIONSHIP_TYPE );
    }

    @Test
    public void shouldScanAllInUseRelationshipsOfCertainType()
    {
        // given
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        int count = 100;
        relationshipStore.setHighId( count );
        Set<Long> expected = new HashSet<>();
        int theType = 1;
        for ( long id = 0; id < count; id++ )
        {
            boolean inUse = random.nextBoolean();
            int type = random.nextInt( 3 );
            createRelationshipRecord( id, type, relationshipStore, inUse );
            if ( inUse && type == theType )
            {
                expected.add( id );
            }
        }

        // when
        assertSeesRelationships( expected, theType );
    }

    private void assertSeesRelationships( Set<Long> expected, int type )
    {
        try ( RecordRelationshipScanCursor cursor = createRelationshipCursor() )
        {
            cursor.scan( type );
            while ( cursor.next() )
            {
                // then
                assertTrue( cursor.toString(), expected.remove( cursor.entityReference() ) );
            }
        }
        assertTrue( expected.isEmpty() );
    }

    private void createRelationshipRecord( long id, int type, RelationshipStore relationshipStore, boolean used )
    {
       relationshipStore.updateRecord( new RelationshipRecord( id ).initialize( used, -1, 1, 2, type, -1, -1, -1, -1, true, true ) );
    }

    private StoreFactory getStoreFactory()
    {
        return new StoreFactory(
                storage.directory().databaseLayout(), Config.defaults(), new DefaultIdGeneratorFactory( storage.fileSystem() ),
                storage.pageCache(), storage.fileSystem(), NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
    }

    private RecordRelationshipScanCursor createRelationshipCursor()
    {
        return new RecordRelationshipScanCursor( neoStores.getRelationshipStore() );
    }
}
