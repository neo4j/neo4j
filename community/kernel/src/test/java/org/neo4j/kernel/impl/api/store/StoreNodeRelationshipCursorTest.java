/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.api.state.StubCursors.relationship;
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

    @ClassRule
    public static TestDirectory directory = TestDirectory.testDirectory( StoreNodeRelationshipCursorTest.class );

    private static FileSystemAbstraction fs;
    private static PageCache pageCache;
    private static NeoStores neoStores;

    @Parameterized.Parameter
    public Direction direction;
    @Parameterized.Parameter( value = 1 )
    public boolean dense;

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

    @BeforeClass
    public static void setupStores()
    {
        File storeDir = directory.absolutePath();
        fs = new DefaultFileSystemAbstraction();
        pageCache = new ConfiguringPageCacheFactory( fs,
                Config.defaults().augment( stringMap( pagecache_memory.name(), "8m" ) ), NULL,
                PageCursorTracerSupplier.NULL, NullLog.getInstance() )
                .getOrCreatePageCache();
        StoreFactory storeFactory = new StoreFactory( storeDir, pageCache, fs, NullLogProvider.getInstance() );
        neoStores = storeFactory.openAllNeoStores( true );
    }

    @AfterClass
    public static void shutDownStores() throws Exception
    {
        neoStores.close();
        pageCache.close();
        fs.close();
    }

    @Test
    public void retrieveNodeRelationships() throws Exception
    {
        createNodeRelationships();

        try ( StoreNodeRelationshipCursor cursor = nodeRelationshipCursor() )
        {
            cursor.init( dense, 1L, FIRST_OWNING_NODE, direction, null );
            assertTrue( cursor.next() );

            cursor.init( dense, 2, FIRST_OWNING_NODE, direction, null );
            assertTrue( cursor.next() );

            cursor.init( dense, 3, FIRST_OWNING_NODE, direction, null );
            assertTrue( cursor.next() );
        }
    }

    @Test
    public void retrieveUsedRelationshipChain()
    {
        createRelationshipChain( 4 );
        long expectedNodeId = 1;
        try ( StoreNodeRelationshipCursor cursor = nodeRelationshipCursor() )
        {
            cursor.init( dense, 1, FIRST_OWNING_NODE, direction, null );
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
        try ( StoreNodeRelationshipCursor cursor = nodeRelationshipCursor() )
        {
            cursor.init( dense, 1, FIRST_OWNING_NODE, direction, null );
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
        try ( StoreNodeRelationshipCursor cursor = nodeRelationshipCursor() )
        {
            // WHEN
            cursor.init( dense, NO_NEXT_RELATIONSHIP.intValue(), FIRST_OWNING_NODE, direction, null );

            // THEN
            assertFalse( cursor.next() );
        }
    }

    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldObserveCorrectAugmentedNodeRelationshipsState() throws Exception
    {
        // GIVEN random committed state
        TxState state = new TxState();
        for ( int i = 0; i < 100; i++ )
        {
            state.nodeDoCreate( i );
        }
        for ( int i = 0; i < 5; i++ )
        {
            state.relationshipTypeDoCreateForName( "Type-" + i, i );
        }
        Map<Long,RelationshipItem> committedRelationships = new HashMap<>();
        long relationshipId = 0;
        int nodeCount = 100;
        int relationshipTypeCount = 5;
        for ( int i = 0; i < 30; i++ )
        {
            RelationshipItem relationship = relationship( relationshipId++, random.nextInt(
                    relationshipTypeCount ),
                    random.nextInt( nodeCount ), random.nextInt( nodeCount ) );
            committedRelationships.put( relationship.id(), relationship );
        }
        Map<Long,RelationshipItem> allRelationships = new HashMap<>( committedRelationships );
        // and some random changes to that
        for ( int i = 0; i < 10; i++ )
        {
            if ( random.nextBoolean() )
            {
                RelationshipItem relationship = relationship( relationshipId++, random.nextInt( relationshipTypeCount ),
                        random.nextInt( nodeCount ), random.nextInt( nodeCount ) );
                allRelationships.put( relationship.id(), relationship );
                state.relationshipDoCreate( relationship.id(), relationship.type(), relationship.startNode(),
                        relationship.endNode() );
            }
            else
            {
                RelationshipItem relationship = Iterables
                        .fromEnd( committedRelationships.values(), random.nextInt( committedRelationships.size() ) );
                state.relationshipDoDelete( relationship.id(), relationship.type(), relationship.startNode(),
                        relationship.endNode() );
                allRelationships.remove( relationship.id() );
            }
        }
        // WHEN
        for ( int nodeId = 0; nodeId < nodeCount; nodeId++ )
        {
            Direction direction = Direction.values()[random.nextInt( Direction.values().length )];
            int[] relationshipTypes = randomTypes( relationshipTypeCount, random.random() );
            Map<Long,RelationshipItem> rels =
                    relationshipsForNode( nodeId, allRelationships, direction, relationshipTypes );
            Cursor<RelationshipItem> cursor = cursor( nodeId, rels, state, direction, relationshipTypes );

            Map<Long,RelationshipItem> expectedRelationships =
                    relationshipsForNode( nodeId, allRelationships, direction, relationshipTypes );
            // THEN
            while ( cursor.next() )
            {
                RelationshipItem relationship = cursor.get();
                RelationshipItem actual = expectedRelationships.remove( relationship.id() );
                assertNotNull( "Augmented cursor returned relationship " + relationship + ", but shouldn't have",
                        actual );
                assertRelationshipEquals( actual, relationship );
            }

            assertTrue( "Augmented cursor didn't return some expected relationships: " + expectedRelationships,
                    expectedRelationships.isEmpty() );
        }
    }

    @Test
    public void shouldClosePageCursorsWhenDisposed()
    {
        RelationshipStore relationshipStore = mock( RelationshipStore.class );
        PageCursor relationshipCursor = mock( PageCursor.class );
        when( relationshipStore.newPageCursor() ).thenReturn( relationshipCursor );
        RelationshipGroupStore relationshipGroupStore = mock( RelationshipGroupStore.class );
        PageCursor relationshipGroupCursor = mock( PageCursor.class );
        when( relationshipGroupStore.newPageCursor() ).thenReturn( relationshipGroupCursor );
        StoreNodeRelationshipCursor cursor =
                new StoreNodeRelationshipCursor( relationshipStore, relationshipGroupStore, this::noCache,
                        NO_LOCK_SERVICE );

        cursor.close();
        cursor.dispose();

        verify( relationshipCursor ).close();
        verify( relationshipGroupCursor ).close();
    }

    private Cursor<RelationshipItem> cursor( long nodeId, Map<Long,RelationshipItem> rels, TxState state,
            Direction direction, int[] relationshipTypes ) throws IOException
    {
        Iterator<Long> relIds = relsFromDisk( nodeId, rels, state, direction, relationshipTypes );
        long firstRelId = relIds.hasNext() ? relIds.next() : -1;
        RelationshipRecord relationshipRecord = new RelationshipRecord( -1 );
        RelationshipStore relationshipStore = mock( RelationshipStore.class );
        when( relationshipStore.newRecord() ).thenReturn( relationshipRecord );
        doAnswer( invocationOnMock ->
        {
            long id = (long) invocationOnMock.getArguments()[0];
            RelationshipRecord record = (RelationshipRecord) invocationOnMock.getArguments()[1];
            RelationshipItem relationshipItem = rels.get( id );
            if ( relationshipItem != null )
            {
                record.setInUse( true );
                record.setId( id );
                record.setType( relationshipItem.type() );
                record.setFirstNode( relationshipItem.startNode() );
                record.setSecondNode( relationshipItem.endNode() );
                long nextRelId = relIds.hasNext() ? relIds.next() : -1;
                // this is a trick but it is good enough for this test
                record.setFirstNextRel( nextRelId );
                record.setSecondNextRel( nextRelId );
            }
            else
            {
                record.clear();
                record.setFirstNode( nodeId );
            }
            return relationshipItem != null;
        } ).when( relationshipStore ).readIntoRecord( anyLong(), eq( relationshipRecord ), any( RecordLoad.class ),
                any( PageCursor.class ) );
        StoreNodeRelationshipCursor cursor =
                new StoreNodeRelationshipCursor( relationshipStore, mock( RelationshipGroupStore.class ), this::noCache,
                        NO_LOCK_SERVICE );

        return relationshipTypes == null
                ? cursor.init( false, firstRelId, nodeId, direction, state )
                : cursor.init( false, firstRelId, nodeId, direction, relationshipTypes, state );
    }

    private Iterator<Long> relsFromDisk( long nodeId, Map<Long,RelationshipItem> rels, TxState state,
            Direction direction, int[] relationshipTypes )
    {
        Set<Long> relationships = rels.keySet();
        PrimitiveLongIterator iterator = relationshipTypes == null
                         ? state.getNodeState( nodeId ).getAddedRelationships( direction )
                         : state.getNodeState( nodeId ).getAddedRelationships( direction, relationshipTypes );

        if ( iterator != null )
        {
            while ( iterator.hasNext() )
            {
                relationships.remove( iterator.next() );
            }
        }

        return relationships.iterator();
    }

    private Map<Long,RelationshipItem> relationshipsForNode( long nodeId, Map<Long,RelationshipItem> allRelationships,
            Direction direction, int[] relationshipTypes )
    {
        Map<Long,RelationshipItem> result = new HashMap<>();
        for ( RelationshipItem relationship : allRelationships.values() )
        {
            switch ( direction )
            {
            case OUTGOING:
                if ( relationship.startNode() != nodeId )
                {
                    continue;
                }
                break;
            case INCOMING:
                if ( relationship.endNode() != nodeId )
                {
                    continue;
                }
                break;
            case BOTH:
                if ( relationship.startNode() != nodeId && relationship.endNode() != nodeId )
                {
                    continue;
                }
                break;
            default:
                throw new IllegalStateException( "Unknown direction: " + direction );
            }

            if ( relationshipTypes != null )
            {
                if ( !contains( relationshipTypes, relationship.type() ) )
                {
                    continue;
                }
            }

            result.put( relationship.id(), relationship );
        }
        return result;
    }

    private void assertRelationshipEquals( RelationshipItem expected, RelationshipItem relationship )
    {
        assertEquals( expected.id(), relationship.id() );
        assertEquals( expected.type(), relationship.type() );
        assertEquals( expected.startNode(), relationship.startNode() );
        assertEquals( expected.endNode(), relationship.endNode() );
    }

    private int[] randomTypes( int high, Random random )
    {
        int count = random.nextInt( high );
        if ( count == 0 )
        {
            return null;
        }
        int[] types = new int[count];
        Arrays.fill( types, -1 );
        for ( int i = 0; i < count; )
        {
            int candidate = random.nextInt( high );
            if ( !contains( types, candidate ) )
            {
                types[i++] = candidate;
            }
        }
        return types;
    }

    private boolean contains( int[] array, int candidate )
    {
        for ( int i : array )
        {
            if ( i == candidate )
            {
                return true;
            }
        }
        return false;
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

    private StoreNodeRelationshipCursor nodeRelationshipCursor()
    {
        return new StoreNodeRelationshipCursor( neoStores.getRelationshipStore(), neoStores.getRelationshipGroupStore(),
                this::noCache, NO_LOCK_SERVICE );
    }

    private void noCache( Cursor<RelationshipItem> cursor )
    {
    }
}
