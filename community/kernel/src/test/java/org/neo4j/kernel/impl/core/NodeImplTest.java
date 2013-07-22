/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeImplTest
{
    private static final int TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_TYPE = 0;
    private static final long TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID = 1;

    /**
     * This behaviour is a workaround until we have proper concurrency support in the kernel.
     * It fixes a problem at the lower levels whereby a source containing a relationship from disk
     * gets merged with a add COW map containing the same relationship in an uncommitted transaction
     * giving unhelpful duplicates at the API level. One day this unit test can be removed,
     * but that day is not today.
     */
    @Test
    public void shouldQuietlyIgnoreSingleDuplicateEntryWhenGetSingleRelationshipCalled() throws Exception
    {
        // given
        NodeImpl nodeImpl = new NodeImpl( 1 );
        RelationshipType loves = DynamicRelationshipType.withName( "LOVES" );

        TransactionState txState = mock( TransactionState.class );
        ThreadToStatementContextBridge stmCtxBridge = mock( ThreadToStatementContextBridge.class );

        NodeManager nodeManager = mock( NodeManager.class );
        RelationshipProxy.RelationshipLookups relLookup = mock( RelationshipProxy.RelationshipLookups.class );
        when( relLookup.getNodeManager() ).thenReturn( nodeManager );

        when( nodeManager.getRelationshipTypeById( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_TYPE ) )
                .thenReturn( loves );
        when( relLookup.lookupRelationship( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID ) )
                .thenReturn( new RelationshipImpl( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID, 1, 2,
                        TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_TYPE, false ) );
        when( nodeManager.getMoreRelationships( nodeImpl ) ).thenReturn( tripletWithValues(
                TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID, TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID
        ) ).thenReturn( noMoreRelationshipsTriplet() );
        when( nodeManager.getTransactionState() ).thenReturn( txState );
        when( nodeManager.newRelationshipProxyById( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID ) ).thenReturn(
                new RelationshipProxy( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID, relLookup, stmCtxBridge ) );

        // when
        final Relationship singleRelationship = nodeImpl.getSingleRelationship( nodeManager, loves,
                Direction.OUTGOING );

        // then
        assertNotNull( singleRelationship );
        assertEquals( loves, singleRelationship.getType() );
    }

    @Test
    public void shouldThrowExceptionIfMultipleDifferentEntries() throws Exception
    {
        // given
        NodeImpl nodeImpl = new NodeImpl( 1 );
        RelationshipType loves = DynamicRelationshipType.withName( "LOVES" );

        TransactionState txState = mock( TransactionState.class );
        ThreadToStatementContextBridge stmCtxBridge = mock( ThreadToStatementContextBridge.class );

        NodeManager nodeManager = mock( NodeManager.class );
        RelationshipProxy.RelationshipLookups relLookup = mock( RelationshipProxy.RelationshipLookups.class );
        when( relLookup.getNodeManager() ).thenReturn( nodeManager );

        when( nodeManager.getRelationshipTypeById( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_TYPE ) )
                .thenReturn( loves );
        when( relLookup.lookupRelationship( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID ) )
                .thenReturn( new RelationshipImpl( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID, 1, 2,
                        TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_TYPE, false ) );

        when( relLookup.lookupRelationship( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID + 1 ) )
                .thenReturn( new RelationshipImpl( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID + 1, 1, 2,
                        TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_TYPE, false ) );

        when( nodeManager.getMoreRelationships( nodeImpl ) ).thenReturn( tripletWithValues(
                TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID, TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID + 1
        ) ).thenReturn( noMoreRelationshipsTriplet() );
        when( nodeManager.getTransactionState() ).thenReturn( txState );

        when( nodeManager.newRelationshipProxyById( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID ) )
                .thenReturn( new RelationshipProxy( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID, relLookup,
                        stmCtxBridge ) );
        when( nodeManager.newRelationshipProxyById( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID + 1 ) )
                .thenReturn( new RelationshipProxy( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID + 1, relLookup,
                        stmCtxBridge ) );

        // when
        try
        {
            nodeImpl.getSingleRelationship( nodeManager, loves, Direction.OUTGOING );
            fail();
        }
        catch ( NotFoundException expected )
        {
        }
    }

    private Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>, Long> noMoreRelationshipsTriplet()
    {
        return Triplet.of( new ArrayMap<Integer, RelIdArray>(), Collections.<RelationshipImpl>emptyList(), 0l );
    }

    @Test
    public void shouldThrowExceptionIfMultipleDifferentEntriesWithTwoOfThemBeingIdentical() throws Exception
    {
        // given
        NodeImpl nodeImpl = new NodeImpl( 1 );
        RelationshipType loves = DynamicRelationshipType.withName( "LOVES" );

        TransactionState txState = mock( TransactionState.class );
        ThreadToStatementContextBridge stmCtxBridge = mock( ThreadToStatementContextBridge.class );

        NodeManager nodeManager = mock( NodeManager.class );
        RelationshipProxy.RelationshipLookups relLookup = mock( RelationshipProxy.RelationshipLookups.class );
        when( relLookup.getNodeManager() ).thenReturn( nodeManager );

        when( nodeManager.getRelationshipTypeById( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_TYPE ) )
                .thenReturn( loves );
        when( relLookup.lookupRelationship( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID ) )
                .thenReturn( new RelationshipImpl( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID, 1, 2,
                        TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_TYPE, false ) );

        when( relLookup.lookupRelationship( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID + 1 ) )
                .thenReturn( new RelationshipImpl( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID + 1, 1, 2,
                        TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_TYPE, false ) );

        when( nodeManager.getMoreRelationships( nodeImpl ) ).thenReturn( tripletWithValues(
                TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID, TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID,
                TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID + 1
        ) ).thenReturn( noMoreRelationshipsTriplet() );

        when( nodeManager.getTransactionState() ).thenReturn( txState );

        when( nodeManager.newRelationshipProxyById( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID ) )
                .thenReturn( new RelationshipProxy( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID, relLookup,
                        stmCtxBridge ) );
        when( nodeManager.newRelationshipProxyById( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID + 1 ) )
                .thenReturn( new RelationshipProxy( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_ID + 1, relLookup,
                        stmCtxBridge) );

        // when
        try
        {
            nodeImpl.getSingleRelationship( nodeManager, loves, Direction.OUTGOING );
            fail();
        }
        catch ( NotFoundException expected )
        {
        }
    }

    private Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>,
            Long> tripletWithValues( long... ids )
    {

        final RelIdArray relIdArray = createRelIdArrayWithValues( ids );

        ArrayMap<Integer, RelIdArray> arrayMap = new ArrayMap<Integer, RelIdArray>();
        arrayMap.put( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_TYPE, relIdArray );

        return Triplet.of( arrayMap,
                Collections.<RelationshipImpl>emptyList(), 0l );
    }


    private RelIdArray createRelIdArrayWithValues( long... ids )
    {
        RelIdArray relIdArray = new RelIdArray( TOTALLY_ARBITRARY_VALUE_DENOTING_RELATIONSHIP_TYPE );
        for ( long id : ids )
        {
            relIdArray.add( id, RelIdArray.DirectionWrapper.OUTGOING );

        }

        return relIdArray;
    }
}
