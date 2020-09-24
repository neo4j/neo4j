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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.LongFunction;

import org.neo4j.kernel.impl.store.id.BatchingIdSequence;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.state.RecordAccess;
import org.neo4j.kernel.impl.transaction.state.RecordAccessSet;
import org.neo4j.kernel.impl.transaction.state.RecordChangeSet;
import org.neo4j.storageengine.api.lock.ResourceLocker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

class RelationshipDeleterTest
{
    private static final long NULL = NULL_REFERENCE.longValue();

    private RelationshipDeleter deleter;
    private MutableLongObjectMap<NodeRecord> nodeStore;
    private MutableLongObjectMap<RelationshipRecord> relationshipStore;
    private MutableLongObjectMap<RelationshipGroupRecord> groupStore;
    private RecordAccessSet recordChanges;
    private final ResourceLocker ignoreLocks = ( tracer, resourceType, resourceIds ) ->
    {
    };

    @BeforeEach
    void setUp()
    {
        RelationshipGroupGetter relationshipGroupGetter = new RelationshipGroupGetter( new BatchingIdSequence() );
        PropertyTraverser propertyTraverser = new PropertyTraverser();
        PropertyDeleter propertyDeleter = new PropertyDeleter( propertyTraverser );
        deleter = new RelationshipDeleter( relationshipGroupGetter, propertyDeleter );
        nodeStore = LongObjectMaps.mutable.empty();
        relationshipStore = LongObjectMaps.mutable.empty();
        groupStore = LongObjectMaps.mutable.empty();
        recordChanges = new RecordChangeSet( loader( nodeStore, NodeRecord::new ), noLoader(), loader( relationshipStore, RelationshipRecord::new ),
                loader( groupStore, RelationshipGroupRecord::new ), noLoader(), noLoader(), noLoader(), noLoader() );
    }

    @Test
    void shouldDecrementDegreeOnceOnFirstIfLoopOnSparseChain()
    {
        // given
        // a node w/ relationship chain (A) -> (B) -> (C) from start node POV where (A) is a loop
        long startNode = 1;
        long otherNode1 = 2;
        long otherNode2 = 3;
        long relA = 11;
        long relB = 12;
        long relC = 13;
        int type = 0;
        nodeStore.put( startNode, new NodeRecord( startNode ).initialize( true, NULL, false, relA, NO_LABELS_FIELD.longValue() ) );
        nodeStore.put( otherNode1, new NodeRecord( otherNode1 ).initialize( true, NULL, false, relB, NO_LABELS_FIELD.longValue() ) );
        nodeStore.put( otherNode2, new NodeRecord( otherNode2 ).initialize( true, NULL, false, relC, NO_LABELS_FIELD.longValue() ) );
        relationshipStore.put( relA, new RelationshipRecord( relA ).initialize( true, NULL, startNode, startNode, type, 3, relB, 3, relB, true, true ) );
        relationshipStore.put( relB, new RelationshipRecord( relB ).initialize( true, NULL, startNode, otherNode1, type, relA, relC, 1, NULL, false, true ) );
        relationshipStore.put( relC, new RelationshipRecord( relC ).initialize( true, NULL, startNode, otherNode2, type, relB, NULL, 1, NULL, false, true ) );

        // when deleting relB
        deleter.relDelete( relB, recordChanges, ignoreLocks );

        // then relA should be updated with correct degrees, i.e. from 3 -> 2 on both its chains
        RecordAccess.RecordProxy<RelationshipRecord,Void> relAChange = recordChanges.getRelRecords().getIfLoaded( relA );
        assertTrue( relAChange.isChanged() );
        assertEquals( 2, relAChange.forReadingData().getFirstPrevRel() );
        assertEquals( 2, relAChange.forReadingData().getSecondPrevRel() );
    }

    @Test
    void shouldDecrementDegreeOnceOnFirstIfLoopOnDenseLoopChain()
    {
        // given
        // a node w/ relationship group chain (G,type:0) -> (H,type:1)
        // where (H) has relationship chain (A) -> (B) -> (C) from start node POV where (A) is a loop
        long node = 1;
        long groupG = 7;
        long groupH = 8;
        long relA = 11;
        long relB = 12;
        long relC = 13;
        int type = 1;
        groupStore.put( groupG, new RelationshipGroupRecord( groupG ).initialize( true, 0, NULL, NULL, NULL, node, groupH ) );
        groupStore.put( groupH, new RelationshipGroupRecord( groupH ).initialize( true, type, NULL, NULL, relA, node, NULL ) );
        nodeStore.put( node, new NodeRecord( node ).initialize( true, NULL, true, groupG, NO_LABELS_FIELD.longValue() ) );
        relationshipStore.put( relA, new RelationshipRecord( relA ).initialize( true, NULL, node, node, type, 3, relB, 3, relB, true, true ) );
        relationshipStore.put( relB, new RelationshipRecord( relB ).initialize( true, NULL, node, node, type, relA, relC, relA, relC, false, false ) );
        relationshipStore.put( relC, new RelationshipRecord( relC ).initialize( true, NULL, node, node, type, relB, NULL, relB, NULL, false, false ) );

        // when deleting relB
        deleter.relDelete( relB, recordChanges, ignoreLocks );

        // then relA should be updated with correct degrees, i.e. from 3 -> 2 on both its chains
        RecordAccess.RecordProxy<RelationshipRecord,Void> relAChange = recordChanges.getRelRecords().getIfLoaded( relA );
        assertTrue( relAChange.isChanged() );
        assertEquals( 2, relAChange.forReadingData().getFirstPrevRel() );
        assertEquals( 2, relAChange.forReadingData().getSecondPrevRel() );
    }

    private <T extends AbstractBaseRecord,R> RecordAccess.Loader<T,R> noLoader()
    {
        return loader( LongObjectMaps.mutable.empty(), id ->
        {
            throw new IllegalStateException( "Should not be needed" );
        } );
    }

    private <T extends AbstractBaseRecord,R> RecordAccess.Loader<T,R> loader( MutableLongObjectMap<T> store, LongFunction<T> factory )
    {
        return new RecordAccess.Loader<T,R>()
        {
            @Override
            public T newUnused( long key, R additionalData )
            {
                T record = factory.apply( key );
                record.setCreated();
                return record;
            }

            @Override
            public T load( long key, R additionalData )
            {
                T record = store.get( key );
                assert record != null;
                return record;
            }

            @Override
            public void ensureHeavy( T record )
            {
                // ignore
            }

            @Override
            public T clone( T record )
            {
                return (T) record.clone();
            }
        };
    }
}
