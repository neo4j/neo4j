/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.ArrayUtil.concatArrays;
import static org.neo4j.internal.recordstorage.RecordNodeCursor.relationshipsReferenceWithDenseMarker;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

@PageCacheExtension
@Neo4jLayoutExtension
public class RecordRelationshipTraversalCursorTest
{
    protected static final long NULL = Record.NULL_REFERENCE.longValue();
    protected static final long FIRST_OWNING_NODE = 1;
    protected static final long SECOND_OWNING_NODE = 2;
    protected static final int TYPE1 = 0;
    protected static final int TYPE2 = 1;
    protected static final int TYPE3 = 2;

    @Inject
    protected PageCache pageCache;
    @Inject
    protected FileSystemAbstraction fs;
    @Inject
    protected DatabaseLayout databaseLayout;

    protected NeoStores neoStores;

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

    private static Stream<Arguments> density()
    {
        return Stream.of( of( false ), of( true ) );
    }

    @BeforeEach
    void setupStores()
    {
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs, immediate() );
        StoreFactory storeFactory = new StoreFactory( databaseLayout, Config.defaults(), idGeneratorFactory, pageCache, fs,
                getRecordFormats(), NullLogProvider.getInstance(), PageCacheTracer.NULL, Sets.immutable.empty() );
        neoStores = storeFactory.openAllNeoStores( true );
    }

    protected RecordFormats getRecordFormats()
    {
        return Standard.LATEST_RECORD_FORMATS;
    }

    @AfterEach
    void shutDownStores()
    {
        neoStores.close();
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void retrieveUsedRelationshipChain( RelationshipDirection direction, boolean dense )
    {
        long reference = createRelationshipStructure( dense, homogenousRelationships( 4, TYPE1, direction ) );
        try ( RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor() )
        {
            cursor.init( FIRST_OWNING_NODE, reference, ALL_RELATIONSHIPS );
            assertRelationships( cursor, 4, Direction.BOTH, TYPE1 );
        }
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void retrieveRelationshipChainWithUnusedLink( RelationshipDirection direction, boolean dense )
    {
        neoStores.getRelationshipStore().setHighId( 10 );
        long reference = createRelationshipStructure( dense, homogenousRelationships( 4, TYPE1, direction ) );
        unUseRecord( 2 );
        int[] expectedRelationshipIds = new int[]{0, 1, 3};
        int relationshipIndex = 0;
        try ( RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor() )
        {
            cursor.init( FIRST_OWNING_NODE, reference, ALL_RELATIONSHIPS );
            while ( cursor.next() )
            {
                assertEquals( expectedRelationshipIds[relationshipIndex++], cursor.entityReference(), "Should load next relationship in a sequence" );
            }
        }
    }

    @Test
    void shouldHandleDenseNodeWithNoRelationships()
    {
        // This can actually happen, since we upgrade sparse node --> dense node when creating relationships,
        // but we don't downgrade dense --> sparse when we delete relationships. So if we have a dense node
        // which no longer has relationships, there was this assumption that we could just call getRecord
        // on the NodeRecord#getNextRel() value. Although that value could actually be -1
        try ( RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor() )
        {
            // WHEN
            cursor.init( FIRST_OWNING_NODE, NO_NEXT_RELATIONSHIP.intValue(), ALL_RELATIONSHIPS );

            // THEN
            assertFalse( cursor.next() );
        }
    }

    @ParameterizedTest
    @MethodSource( "density" )
    void shouldSelectRelationshipsOfCertainDirection( boolean dense )
    {
        // given
        long reference = createRelationshipStructure( dense, concatArrays(
                homogenousRelationships( 4, TYPE1, OUTGOING ),
                homogenousRelationships( 3, TYPE1, INCOMING ),
                homogenousRelationships( 1, TYPE1, LOOP ) ) );

        try ( RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor() )
        {
            // outgoing
            cursor.init( FIRST_OWNING_NODE, reference, selection( Direction.OUTGOING ) );
            assertRelationships( cursor, 5, Direction.OUTGOING, TYPE1 );

            // incoming
            cursor.init( FIRST_OWNING_NODE, reference, selection( Direction.INCOMING ) );
            assertRelationships( cursor, 4, Direction.INCOMING, TYPE1 );

            // incoming
            cursor.init( FIRST_OWNING_NODE, reference, selection( Direction.BOTH ) );
            assertRelationships( cursor, 8, Direction.BOTH, TYPE1 );
        }
    }

    @ParameterizedTest
    @MethodSource( "density" )
    void shouldSelectRelationshipsOfCertainTypeAndDirection( boolean dense )
    {
        // given
        long reference = createRelationshipStructure( dense, concatArrays(
                homogenousRelationships( 4, TYPE1, OUTGOING ),
                homogenousRelationships( 3, TYPE1, INCOMING ),
                homogenousRelationships( 1, TYPE1, LOOP ),
                homogenousRelationships( 2, TYPE2, OUTGOING ),
                homogenousRelationships( 5, TYPE2, INCOMING ),
                homogenousRelationships( 6, TYPE2, LOOP ) ) );

        try ( RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor() )
        {
            // === TYPE1 ===
            cursor.init( FIRST_OWNING_NODE, reference, selection( TYPE1, Direction.OUTGOING ) );
            assertRelationships( cursor, 5, Direction.OUTGOING, TYPE1 );
            cursor.init( FIRST_OWNING_NODE, reference, selection( TYPE1, Direction.INCOMING ) );
            assertRelationships( cursor, 4, Direction.INCOMING, TYPE1 );
            cursor.init( FIRST_OWNING_NODE, reference, selection( TYPE1, Direction.BOTH ) );
            assertRelationships( cursor, 8, Direction.BOTH, TYPE1 );

            // === TYPE2 ===
            cursor.init( FIRST_OWNING_NODE, reference, selection( TYPE2, Direction.OUTGOING ) );
            assertRelationships( cursor, 8, Direction.OUTGOING, TYPE2 );
            cursor.init( FIRST_OWNING_NODE, reference, selection( TYPE2, Direction.INCOMING ) );
            assertRelationships( cursor, 11, Direction.INCOMING, TYPE2 );
            cursor.init( FIRST_OWNING_NODE, reference, selection( TYPE2, Direction.BOTH ) );
            assertRelationships( cursor, 13, Direction.BOTH, TYPE2 );
        }
    }

    @ParameterizedTest
    @MethodSource( "density" )
    void shouldSelectRelationshipsOfCertainTypesAndDirection( boolean dense )
    {
        // given
        long reference = createRelationshipStructure( dense, concatArrays(
                homogenousRelationships( 4, TYPE1, OUTGOING ),
                homogenousRelationships( 1, TYPE1, LOOP ),
                homogenousRelationships( 5, TYPE2, INCOMING ),
                homogenousRelationships( 6, TYPE2, LOOP ),
                homogenousRelationships( 2, TYPE3, OUTGOING ),
                homogenousRelationships( 3, TYPE3, INCOMING ) ) );

        try ( RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor() )
        {
            // === TYPE1+TYPE3 ===
            {
                int[] types = {TYPE1, TYPE3};
                cursor.init( FIRST_OWNING_NODE, reference, selection( types, Direction.OUTGOING ) );
                assertRelationships( cursor, 7, Direction.OUTGOING, types );
                cursor.init( FIRST_OWNING_NODE, reference, selection( types, Direction.INCOMING ) );
                assertRelationships( cursor, 4, Direction.INCOMING, types );
                cursor.init( FIRST_OWNING_NODE, reference, selection( types, Direction.BOTH ) );
                assertRelationships( cursor, 10, Direction.BOTH, types );
            }

            // === TYPE1+TYPE2 ===
            {
                int[] types = {TYPE1, TYPE2};
                cursor.init( FIRST_OWNING_NODE, reference, selection( types, Direction.OUTGOING ) );
                assertRelationships( cursor, 11, Direction.OUTGOING, types );
                cursor.init( FIRST_OWNING_NODE, reference, selection( types, Direction.INCOMING ) );
                assertRelationships( cursor, 12, Direction.INCOMING, types );
                cursor.init( FIRST_OWNING_NODE, reference, selection( types, Direction.BOTH ) );
                assertRelationships( cursor, 16, Direction.BOTH, types );
            }
        }
    }

    private void assertRelationships( RecordRelationshipTraversalCursor cursor, int count, Direction direction, int... types )
    {
        IntSet expectedTypes = IntSets.immutable.of( types );
        int found = 0;
        while ( cursor.next() )
        {
            found++;
            assertTrue( expectedTypes.contains( cursor.type() ) );
            switch ( direction )
            {
            case OUTGOING:
                assertEquals( FIRST_OWNING_NODE, cursor.sourceNodeReference() );
                break;
            case INCOMING:
                assertEquals( FIRST_OWNING_NODE, cursor.targetNodeReference() );
                break;
            case BOTH:
                assertTrue( FIRST_OWNING_NODE == cursor.sourceNodeReference() || FIRST_OWNING_NODE == cursor.targetNodeReference() );
                break;
            default:
                throw new UnsupportedOperationException( direction.name() );
            }
        }
        assertEquals( count, found );
    }

    protected void unUseRecord( long recordId )
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        RelationshipRecord relationshipRecord = relationshipStore.getRecord( recordId, new RelationshipRecord( -1 ),
                RecordLoad.FORCE, PageCursorTracer.NULL );
        relationshipRecord.setInUse( false );
        relationshipStore.updateRecord( relationshipRecord, PageCursorTracer.NULL );
    }

    protected RelationshipGroupRecord createRelationshipGroup( long id, int type, long[] firstIds, long next )
    {
        return new RelationshipGroupRecord( id ).initialize( true, type, firstIds[0], firstIds[1], firstIds[2], FIRST_OWNING_NODE, next );
    }

    protected long createRelationshipStructure( boolean dense, RelationshipSpec... relationshipSpecs )
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        if ( !dense )
        {
            // a single chain
            for ( int i = 0; i < relationshipSpecs.length; i++ )
            {
                long nextRelationshipId = i == relationshipSpecs.length - 1 ? NULL : i + 1;
                relationshipStore.updateRecord( createRelationship( i, nextRelationshipId, relationshipSpecs[i] ), PageCursorTracer.NULL );
            }
            return 0;
        }
        else
        {
            // split chains on type/direction
            Arrays.sort( relationshipSpecs );
            RelationshipGroupStore relationshipGroupStore = neoStores.getRelationshipGroupStore();
            int currentType = -1;
            long[] currentGroup = null;
            long nextGroupId = relationshipGroupStore.getNumberOfReservedLowIds();
            for ( int i = 0; i < relationshipSpecs.length; i++ )
            {
                RelationshipSpec spec = relationshipSpecs[i];
                if ( spec.type != currentType || currentGroup == null )
                {
                    if ( currentGroup != null )
                    {
                        relationshipGroupStore.updateRecord( createRelationshipGroup( nextGroupId++, currentType, currentGroup, nextGroupId ),
                                PageCursorTracer.NULL );
                    }
                    currentType = spec.type;
                    currentGroup = new long[]{NULL, NULL, NULL};
                }

                int relationshipOrdinal = relationshipSpecs[i].direction.ordinal();
                long relationshipId = i;
                long nextRelationshipId = i < relationshipSpecs.length - 1 && relationshipSpecs[i + 1].equals( spec ) ? i + 1 : NULL;
                relationshipStore.updateRecord( createRelationship( relationshipId, nextRelationshipId, relationshipSpecs[i] ), PageCursorTracer.NULL );
                if ( currentGroup[relationshipOrdinal] == NULL )
                {
                    currentGroup[relationshipOrdinal] = relationshipId;
                }
            }
            relationshipGroupStore.updateRecord( createRelationshipGroup( nextGroupId, currentType, currentGroup, NULL ), PageCursorTracer.NULL );
            return relationshipsReferenceWithDenseMarker( relationshipGroupStore.getNumberOfReservedLowIds(), true );
        }
    }

    protected RelationshipRecord createRelationship( long id, long nextRelationship, RelationshipSpec relationshipSpec )
    {
        RelationshipRecord relationship = new RelationshipRecord( id );
        relationship.initialize( true, NO_NEXT_PROPERTY.intValue(), getFirstNode( relationshipSpec.direction ),
                getSecondNode( relationshipSpec.direction ), relationshipSpec.type, NO_NEXT_RELATIONSHIP.intValue(), nextRelationship,
                NO_NEXT_RELATIONSHIP.intValue(), nextRelationship, false, false );
        return relationship;
    }

    protected long getSecondNode( RelationshipDirection direction )
    {
        return direction == INCOMING || direction == LOOP ? FIRST_OWNING_NODE : SECOND_OWNING_NODE;
    }

    protected long getFirstNode( RelationshipDirection direction )
    {
        return direction == OUTGOING || direction == LOOP ? FIRST_OWNING_NODE : SECOND_OWNING_NODE;
    }

    protected RecordRelationshipTraversalCursor getNodeRelationshipCursor()
    {
        return new RecordRelationshipTraversalCursor( neoStores.getRelationshipStore(), neoStores.getRelationshipGroupStore(), PageCursorTracer.NULL );
    }

    protected RelationshipSpec[] homogenousRelationships( int count, int type, RelationshipDirection direction )
    {
        RelationshipSpec[] specs = new RelationshipSpec[count];
        Arrays.fill( specs, new RelationshipSpec( type, direction ) );
        return specs;
    }

    protected static class RelationshipSpec implements Comparable<RelationshipSpec>
    {
        final int type;
        final RelationshipDirection direction;

        RelationshipSpec( int type, RelationshipDirection direction )
        {
            this.type = type;
            this.direction = direction;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            RelationshipSpec that = (RelationshipSpec) o;
            return type == that.type && direction == that.direction;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( type, direction );
        }

        @Override
        public int compareTo( RelationshipSpec o )
        {
            int typeCompare = Integer.compare( type, o.type );
            if ( typeCompare != 0 )
            {
                return typeCompare;
            }
            return Integer.compare( direction.ordinal(), o.direction.ordinal() );
        }
    }
}
