/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.function.Consumer;

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.storageengine.api.Direction.BOTH;
import static org.neo4j.storageengine.api.Direction.INCOMING;
import static org.neo4j.storageengine.api.Direction.OUTGOING;

@RunWith( Parameterized.class )
public class StoreNodeRelationshipCursorTest
{

    private static final long FIRST_OWNING_NODE = 1;
    private static final long SECOND_OWNING_NODE = 2;
    private static final int TYPE = 0;

    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( pageCacheRule )
            .around( fileSystemRule );
    @Parameterized.Parameter
    public Direction direction;
    @Parameterized.Parameter( value = 1 )
    public boolean dense;
    private NeoStores neoStores;

    @Parameterized.Parameters
    public static Iterable<Object[]> parameters()
    {
        return Arrays.asList( new Object[][]{
                {Direction.BOTH, false},
                {Direction.BOTH, true},
                {Direction.OUTGOING, false},
                {Direction.OUTGOING, true},
                {Direction.INCOMING, false},
                {Direction.INCOMING, true}
        } );
    }

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
        StoreFactory storeFactory = new StoreFactory( testDirectory.directory(),
                pageCacheRule.getPageCache( fileSystemRule.get() ), fileSystemRule.get(),
                NullLogProvider.getInstance() );
        neoStores = storeFactory.openAllNeoStores( true );
    }

    @Test
    public void retrieveNodeRelationships() throws Exception
    {
        createNodeRelationships();

        try ( StoreNodeRelationshipCursor cursor = getNodeRelationshipCursor() )
        {
            cursor.init( dense, 1L, FIRST_OWNING_NODE, direction );
            assertTrue( cursor.next() );

            cursor.init( dense, 2, FIRST_OWNING_NODE, direction );
            assertTrue( cursor.next() );

            cursor.init( dense, 3, FIRST_OWNING_NODE, direction );
            assertTrue( cursor.next() );
        }
    }

    @Test
    public void retrieveUsedRelationshipChain()
    {
        createRelationshipChain( 4 );
        long expectedNodeId = 1;
        try ( StoreNodeRelationshipCursor cursor = getNodeRelationshipCursor() )
        {
            cursor.init( dense, 1, FIRST_OWNING_NODE, direction );
            while ( cursor.next() )
            {
                assertEquals( "Should load next relationship in a sequence", expectedNodeId++, cursor.get().id() );
            }
        }
    }

    @Test
    public void retrieveRelationshipChainWithUnusedLink()
    {
        neoStores.getRelationshipStore().setHighId( 10 );
        createRelationshipChain( 4 );
        unUseRecord( 3 );
        int[] expectedRelationshipIds = new int[]{1, 2, 4};
        int relationshipIndex = 0;
        try ( StoreNodeRelationshipCursor cursor = getNodeRelationshipCursor() )
        {
            cursor.init( dense, 1, FIRST_OWNING_NODE, direction );
            while ( cursor.next() )
            {
                assertEquals( "Should load next relationship in a sequence",
                        expectedRelationshipIds[relationshipIndex++], cursor.get().id() );
            }
        }
    }

    @Test
    public void shouldHandleDenseNodeWithNoRelationships() throws Exception
    {
        // This can actually happen, since we upgrade sparse node --> dense node when creating relationships,
        // but we don't downgrade dense --> sparse when we delete relationships. So if we have a dense node
        // which no longer has relationships, there was this assumption that we could just call getRecord
        // on the NodeRecord#getNextRel() value. Although that value could actually be -1
        try ( StoreNodeRelationshipCursor cursor = getNodeRelationshipCursor() )
        {
            // WHEN
            cursor.init( dense, NO_NEXT_RELATIONSHIP.intValue(), FIRST_OWNING_NODE, direction );

            // THEN
            assertFalse( cursor.next() );
        }
    }

    private void createNodeRelationships()
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        if ( dense )
        {
            RecordStore<RelationshipGroupRecord> relationshipGroupStore = neoStores.getRelationshipGroupStore();
            relationshipGroupStore.updateRecord( createRelationshipGroup( 1, 1 ) );
            relationshipGroupStore.updateRecord( createRelationshipGroup( 2, 2 ) );
            relationshipGroupStore.updateRecord( createRelationshipGroup( 3, 3 ) );
        }

        relationshipStore.updateRecord( createRelationship( 1, NO_NEXT_RELATIONSHIP.intValue() ) );
        relationshipStore.updateRecord( createRelationship( 2, NO_NEXT_RELATIONSHIP.intValue() ) );
        relationshipStore.updateRecord( createRelationship( 3, NO_NEXT_RELATIONSHIP.intValue() ) );
    }

    private void unUseRecord( long recordId )
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        RelationshipRecord relationshipRecord = relationshipStore.getRecord( recordId, new RelationshipRecord( -1 ),
                RecordLoad.FORCE );
        relationshipRecord.setInUse( false );
        relationshipStore.updateRecord( relationshipRecord );
    }

    private RelationshipGroupRecord createRelationshipGroup( long id, long relationshipId )
    {
        return new RelationshipGroupRecord( id, TYPE, getFirstOut( relationshipId ),
                getFirstIn( relationshipId ), getFirstLoop( relationshipId ), FIRST_OWNING_NODE, true );
    }

    private long getFirstLoop( long firstLoop )
    {
        return direction == BOTH ? firstLoop : Record.NO_NEXT_RELATIONSHIP.intValue();
    }

    private long getFirstIn( long firstIn )
    {
        return direction == INCOMING ? firstIn : Record.NO_NEXT_RELATIONSHIP.intValue();
    }

    private long getFirstOut( long firstOut )
    {
        return direction == OUTGOING ? firstOut : Record.NO_NEXT_RELATIONSHIP.intValue();
    }

    private void createRelationshipChain( int recordsInChain )
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        for ( int i = 1; i < recordsInChain; i++ )
        {
            relationshipStore.updateRecord( createRelationship( i, i + 1 ) );
        }
        relationshipStore.updateRecord( createRelationship( recordsInChain, NO_NEXT_RELATIONSHIP.intValue() ) );
        if ( dense )
        {
            RecordStore<RelationshipGroupRecord> relationshipGroupStore = neoStores.getRelationshipGroupStore();
            for ( int i = 1; i < recordsInChain; i++ )
            {
                relationshipGroupStore.updateRecord( createRelationshipGroup( i, i ) );
            }
            relationshipGroupStore
                    .updateRecord( createRelationshipGroup( recordsInChain, NO_NEXT_RELATIONSHIP.intValue() ) );
        }
    }

    private RelationshipRecord createRelationship( long id, long nextRelationship )
    {
        return new RelationshipRecord( id, true, getFirstNode(), getSecondNode(), TYPE, NO_NEXT_RELATIONSHIP.intValue(),
                nextRelationship, NO_NEXT_RELATIONSHIP.intValue(), nextRelationship, false, false );
    }

    private long getSecondNode()
    {
        return getFirstNode() == FIRST_OWNING_NODE ? SECOND_OWNING_NODE : FIRST_OWNING_NODE;
    }

    private long getFirstNode()
    {
        return direction == OUTGOING ? FIRST_OWNING_NODE : SECOND_OWNING_NODE;
    }

    private StoreNodeRelationshipCursor getNodeRelationshipCursor()
    {
        return new StoreNodeRelationshipCursor(
                new RelationshipRecord( -1 ),
                new RelationshipGroupRecord( -1, -1 ),
                mock( Consumer.class ),
                new RecordCursors( neoStores ),
                NO_LOCK_SERVICE );
    }
}
