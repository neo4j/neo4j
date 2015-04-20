/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import org.neo4j.kernel.impl.core.FirstRelationshipIds;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.state.ControlledLoaders;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

import static org.junit.Assert.assertEquals;

public class RecordStateForCacheAccessorTest
{
    @Test
    public void shouldProvideFirstRelationshipIdOfSparseNode() throws Exception
    {
        // GIVEN
        ControlledLoaders fakeStore = new ControlledLoaders();
        long nodeId = 0L, relId = 3L;
        fakeStore.getNodes().put( nodeId, new NodeRecord( nodeId, false, relId, -1, true ) );
        fakeStore.getRelationships().put( relId,
                new RelationshipRecord( relId, true, 0, 0, 0, -1, -1, -1, -1, true, true ) );
        RecordStateForCacheAccessor accessor = new RecordStateForCacheAccessor( fakeStore.newAccessSet() );

        // WHEN
        FirstRelationshipIds firstIds = accessor.firstRelationshipIdsOf( nodeId );

        // THEN
        assertEquals( relId, firstIds.firstIdOf( 0, DirectionWrapper.OUTGOING ) );
        assertEquals( relId, firstIds.firstIdOf( 2, DirectionWrapper.BOTH ) );
    }

    @Test
    public void shouldProvideFirstRelationshipIdsOfDenseNode() throws Exception
    {
        // GIVEN
        ControlledLoaders fakeStore = new ControlledLoaders();
        long nodeId = 2L;
        int firstType = 1, secondType = 2;
        long firstGroupId = 5L;
        long firstGroupOut = 10, firstGroupIn = -1, firstGroupLoop = 7;
        long secondGroupId = 7L;
        long secondGroupOut = -1, secondGroupIn = 20, secondGroupLoop = 22;
        fakeStore.getNodes().put( nodeId, new NodeRecord( nodeId, true, firstGroupId, -1, true ) );
        fakeStore.getRelationshipGroups().put( firstGroupId, new RelationshipGroupRecord( firstGroupId, firstType,
                firstGroupOut, firstGroupIn, firstGroupLoop, nodeId, secondGroupId, true ) );
        fakeStore.getRelationshipGroups().put( secondGroupId, new RelationshipGroupRecord( secondGroupId, secondType,
                secondGroupOut, secondGroupIn, secondGroupLoop, nodeId, true ) );
        RecordStateForCacheAccessor accessor = new RecordStateForCacheAccessor( fakeStore.newAccessSet() );

        // WHEN
        FirstRelationshipIds firstIds = accessor.firstRelationshipIdsOf( nodeId );

        // THEN
        assertEquals( firstGroupOut, firstIds.firstIdOf( firstType, DirectionWrapper.OUTGOING ) );
        assertEquals( firstGroupIn, firstIds.firstIdOf( firstType, DirectionWrapper.INCOMING ) );
        assertEquals( firstGroupLoop, firstIds.firstIdOf( firstType, DirectionWrapper.BOTH ) );
        assertEquals( secondGroupOut, firstIds.firstIdOf( secondType, DirectionWrapper.OUTGOING ) );
        assertEquals( secondGroupIn, firstIds.firstIdOf( secondType, DirectionWrapper.INCOMING ) );
        assertEquals( secondGroupLoop, firstIds.firstIdOf( secondType, DirectionWrapper.BOTH ) );
    }
}
