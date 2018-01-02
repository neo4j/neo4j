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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;

import org.neo4j.function.Consumer;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.DefaultFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

@RunWith( Parameterized.class )
public class StoreNodeRelationshipCursorTest
{

    private static final int FIRST_OWNING_NODE = 1;
    private static final int SECOND_OWNING_NODE = 2;
    private static final int TYPE = 0;

    private final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( pageCacheRule )
            .around( fileSystemRule );
    @Parameter
    public Direction direction;
    @Parameter( value = 1 )
    public boolean dense;
    private NeoStores neoStores;

    @Parameters
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
        StoreFactory storeFactory = new StoreFactory( fileSystemRule.get(), testDirectory.directory(),
                pageCacheRule.getPageCache( fileSystemRule.get() ), NullLogProvider.getInstance() );
        neoStores = storeFactory.openNeoStores( true, NeoStores.StoreType.NODE,
                NeoStores.StoreType.RELATIONSHIP_GROUP,
                NeoStores.StoreType.RELATIONSHIP );
    }

    @Test
    public void retrieveNodeRelationships() throws Exception
    {
        createNodeRelationships();

        try ( StoreNodeRelationshipCursor cursor = getNodeRelationshipCursor() )
        {
            cursor.init( dense, 1, FIRST_OWNING_NODE, direction );
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
            RelationshipGroupStore relationshipGroupStore = neoStores.getRelationshipGroupStore();
            relationshipGroupStore.forceUpdateRecord( createRelationshipGroup( 1, 1 ) );
            relationshipGroupStore.forceUpdateRecord( createRelationshipGroup( 2, 2 ) );
            relationshipGroupStore.forceUpdateRecord( createRelationshipGroup( 3, 3 ) );
        }

        relationshipStore.forceUpdateRecord( createRelationship( 1, NO_NEXT_RELATIONSHIP.intValue() ) );
        relationshipStore.forceUpdateRecord( createRelationship( 2, NO_NEXT_RELATIONSHIP.intValue() ) );
        relationshipStore.forceUpdateRecord( createRelationship( 3, NO_NEXT_RELATIONSHIP.intValue() ) );
    }

    private void unUseRecord( long recordId )
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        RelationshipRecord relationshipRecord = relationshipStore.forceGetRecord( recordId );
        relationshipRecord.setInUse( false );
        relationshipStore.forceUpdateRecord( relationshipRecord );
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
            relationshipStore.forceUpdateRecord( createRelationship( i, i + 1 ) );
        }
        relationshipStore.forceUpdateRecord( createRelationship( recordsInChain, NO_NEXT_RELATIONSHIP.intValue() ) );
        if ( dense )
        {
            RelationshipGroupStore relationshipGroupStore = neoStores.getRelationshipGroupStore();
            for ( int i = 1; i < recordsInChain; i++ )
            {
                relationshipGroupStore.forceUpdateRecord( createRelationshipGroup( i, i ) );
            }
            relationshipGroupStore
                    .forceUpdateRecord( createRelationshipGroup( recordsInChain, NO_NEXT_RELATIONSHIP.intValue() ) );
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
        return direction == Direction.OUTGOING ? FIRST_OWNING_NODE : SECOND_OWNING_NODE;
    }

    private StoreNodeRelationshipCursor getNodeRelationshipCursor()
    {
        return new StoreNodeRelationshipCursor(
                new RelationshipRecord( -1 ), neoStores,
                new RelationshipGroupRecord( -1, -1 ),
                mock( StoreStatement.class ),
                mock( Consumer.class ),
                NO_LOCK_SERVICE );
    }
}
