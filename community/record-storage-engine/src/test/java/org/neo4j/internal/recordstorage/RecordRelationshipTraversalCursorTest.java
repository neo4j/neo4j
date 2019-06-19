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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;

@PageCacheExtension
class RecordRelationshipTraversalCursorTest
{
    private static final long FIRST_OWNING_NODE = 1;
    private static final long SECOND_OWNING_NODE = 2;
    private static final int TYPE = 0;

    @Inject
    private PageCache pageCache;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private NeoStores neoStores;

    private static Stream<Arguments> parameters()
    {
        return Stream.of(
            of( LOOP, false ),
            of( LOOP, true ),
            of( OUTGOING, false ),
            of( OUTGOING, true ),
            of( INCOMING, false ),
            of( INCOMING, true )
        );
    }

    @BeforeEach
    void setupStores()
    {
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs, immediate() );
        StoreFactory storeFactory = new StoreFactory( testDirectory.databaseLayout(), Config.defaults(), idGeneratorFactory, pageCache, fs,
            NullLogProvider.getInstance() );
        neoStores = storeFactory.openAllNeoStores( true );
    }

    @AfterEach
    void shutDownStores()
    {
        neoStores.close();
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void retrieveNodeRelationships( RelationshipDirection direction, boolean dense )
    {
        createNodeRelationships( dense, direction );

        try ( RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor() )
        {
            cursor.init( FIRST_OWNING_NODE, 1L, dense );
            assertTrue( cursor.next() );

            cursor.init( FIRST_OWNING_NODE, 2, dense );
            assertTrue( cursor.next() );

            cursor.init( FIRST_OWNING_NODE, 3, dense );
            assertTrue( cursor.next() );
        }
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void retrieveUsedRelationshipChain( RelationshipDirection direction, boolean dense )
    {
        createRelationshipChain( 4, dense, direction );
        long expectedNodeId = 1;
        try ( RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor() )
        {
            cursor.init( FIRST_OWNING_NODE, 1, dense );
            while ( cursor.next() )
            {
                assertEquals( expectedNodeId++, cursor.entityReference(), "Should load next relationship in a sequence" );
            }
        }
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void retrieveRelationshipChainWithUnusedLink( RelationshipDirection direction, boolean dense )
    {
        neoStores.getRelationshipStore().setHighId( 10 );
        createRelationshipChain( 4, dense, direction );
        unUseRecord( 3 );
        int[] expectedRelationshipIds = new int[]{1, 2, 4};
        int relationshipIndex = 0;
        try ( RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor() )
        {
            cursor.init( FIRST_OWNING_NODE, 1, dense );
            while ( cursor.next() )
            {
                assertEquals(
                    expectedRelationshipIds[relationshipIndex++], cursor.entityReference(), "Should load next relationship in a sequence" );
            }
        }
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldHandleDenseNodeWithNoRelationships( RelationshipDirection direction, boolean dense )
    {
        // This can actually happen, since we upgrade sparse node --> dense node when creating relationships,
        // but we don't downgrade dense --> sparse when we delete relationships. So if we have a dense node
        // which no longer has relationships, there was this assumption that we could just call getRecord
        // on the NodeRecord#getNextRel() value. Although that value could actually be -1
        try ( RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor() )
        {
            // WHEN
            cursor.init( FIRST_OWNING_NODE, NO_NEXT_RELATIONSHIP.intValue(), dense );

            // THEN
            assertFalse( cursor.next() );
        }
    }

    private void createNodeRelationships( boolean dense, RelationshipDirection direction )
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        if ( dense )
        {
            RecordStore<RelationshipGroupRecord> relationshipGroupStore = neoStores.getRelationshipGroupStore();
            relationshipGroupStore.updateRecord( createRelationshipGroup( 1, 1, direction ) );
            relationshipGroupStore.updateRecord( createRelationshipGroup( 2, 2, direction ) );
            relationshipGroupStore.updateRecord( createRelationshipGroup( 3, 3, direction ) );
        }

        relationshipStore.updateRecord( createRelationship( 1, NO_NEXT_RELATIONSHIP.intValue(), direction ) );
        relationshipStore.updateRecord( createRelationship( 2, NO_NEXT_RELATIONSHIP.intValue(), direction ) );
        relationshipStore.updateRecord( createRelationship( 3, NO_NEXT_RELATIONSHIP.intValue(), direction ) );
    }

    private void unUseRecord( long recordId )
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        RelationshipRecord relationshipRecord = relationshipStore.getRecord( recordId, new RelationshipRecord( -1 ),
                RecordLoad.FORCE );
        relationshipRecord.setInUse( false );
        relationshipStore.updateRecord( relationshipRecord );
    }

    private RelationshipGroupRecord createRelationshipGroup( long id, long relationshipId, RelationshipDirection direction )
    {
        return new RelationshipGroupRecord( id, TYPE, getFirstOut( relationshipId, direction ),
                getFirstIn( relationshipId, direction ), getFirstLoop( relationshipId, direction ), FIRST_OWNING_NODE, true );
    }

    private long getFirstLoop( long firstLoop, RelationshipDirection direction )
    {
        return direction == LOOP ? firstLoop : Record.NO_NEXT_RELATIONSHIP.intValue();
    }

    private long getFirstIn( long firstIn, RelationshipDirection direction )
    {
        return direction == INCOMING ? firstIn : Record.NO_NEXT_RELATIONSHIP.intValue();
    }

    private long getFirstOut( long firstOut, RelationshipDirection direction )
    {
        return direction == OUTGOING ? firstOut : Record.NO_NEXT_RELATIONSHIP.intValue();
    }

    private void createRelationshipChain( int recordsInChain, boolean dense, RelationshipDirection direction )
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        for ( int i = 1; i < recordsInChain; i++ )
        {
            relationshipStore.updateRecord( createRelationship( i, i + 1, direction ) );
        }
        relationshipStore.updateRecord( createRelationship( recordsInChain, NO_NEXT_RELATIONSHIP.intValue(), direction ) );
        if ( dense )
        {
            RecordStore<RelationshipGroupRecord> relationshipGroupStore = neoStores.getRelationshipGroupStore();
            for ( int i = 1; i < recordsInChain; i++ )
            {
                relationshipGroupStore.updateRecord( createRelationshipGroup( i, i, direction ) );
            }
            relationshipGroupStore
                    .updateRecord( createRelationshipGroup( recordsInChain, NO_NEXT_RELATIONSHIP.intValue(), direction ) );
        }
    }

    private RelationshipRecord createRelationship( long id, long nextRelationship, RelationshipDirection direction )
    {
        return new RelationshipRecord( id, true, getFirstNode( direction ), getSecondNode( direction ), TYPE, NO_NEXT_RELATIONSHIP.intValue(),
                nextRelationship, NO_NEXT_RELATIONSHIP.intValue(), nextRelationship, false, false );
    }

    private long getSecondNode( RelationshipDirection direction )
    {
        return getFirstNode( direction ) == FIRST_OWNING_NODE ? SECOND_OWNING_NODE : FIRST_OWNING_NODE;
    }

    private long getFirstNode( RelationshipDirection direction )
    {
        return direction == OUTGOING ? FIRST_OWNING_NODE : SECOND_OWNING_NODE;
    }

    private RecordRelationshipTraversalCursor getNodeRelationshipCursor()
    {
        return new RecordRelationshipTraversalCursor( neoStores.getRelationshipStore(), neoStores.getRelationshipGroupStore() );
    }
}
