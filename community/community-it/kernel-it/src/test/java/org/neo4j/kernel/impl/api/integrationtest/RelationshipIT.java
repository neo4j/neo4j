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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.test.rule.OtherThreadRule;

import static org.apache.commons.lang3.ArrayUtils.toObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;

class RelationshipIT extends KernelIntegrationTest
{
    private final OtherThreadRule<Object> otherThread = new OtherThreadRule<>();

    @BeforeEach
    void setUp()
    {
        otherThread.init( "t2" );
    }

    @AfterEach
    void tearDown()
    {
        otherThread.close();
    }

    @Test
    void shouldListRelationshipsInCurrentAndSubsequentTx() throws Exception
    {
        // given
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        int relType1 = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "Type1" );
        int relType2 = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "Type2" );

        long refNode = transaction.dataWrite().nodeCreate();
        long otherNode = transaction.dataWrite().nodeCreate();
        long fromRefToOther1 = transaction.dataWrite().relationshipCreate( refNode, relType1, otherNode );
        long fromRefToOther2 = transaction.dataWrite().relationshipCreate( refNode, relType2, otherNode );
        long fromOtherToRef = transaction.dataWrite().relationshipCreate( otherNode, relType1, refNode );
        long fromRefToRef = transaction.dataWrite().relationshipCreate( refNode, relType2, refNode );
        long endNode = transaction.dataWrite().nodeCreate();
        long fromRefToThird = transaction.dataWrite().relationshipCreate( refNode, relType2, endNode );

        // when & then
        assertRels( nodeGetRelationships( transaction, refNode, BOTH ), fromRefToOther1, fromRefToOther2,
                fromRefToRef, fromRefToThird, fromOtherToRef );

        assertRels( nodeGetRelationships( transaction, refNode, BOTH, new int[]{relType1} ),
                fromRefToOther1,
                fromOtherToRef );

        assertRels( nodeGetRelationships( transaction, refNode, BOTH, new int[]{relType1, relType2} ),
                fromRefToOther1, fromRefToOther2, fromRefToRef, fromRefToThird, fromOtherToRef );

        assertRels( nodeGetRelationships( transaction, refNode, INCOMING ), fromOtherToRef );

        assertRels( nodeGetRelationships( transaction, refNode, INCOMING, new int[]{relType1} ), fromOtherToRef );

        assertRels( nodeGetRelationships( transaction, refNode, OUTGOING, new int[]{relType1, relType2} ),
                fromRefToOther1, fromRefToOther2, fromRefToThird, fromRefToRef );

        // when
        commit();
        transaction = newTransaction();

        // when & then
        assertRels( nodeGetRelationships( transaction, refNode, BOTH ), fromRefToOther1, fromRefToOther2,
                fromRefToRef, fromRefToThird, fromOtherToRef );

        assertRels( nodeGetRelationships( transaction, refNode, BOTH, new int[]{relType1} ), fromRefToOther1,
                fromOtherToRef );

        assertRels( nodeGetRelationships( transaction, refNode, BOTH, new int[]{relType1, relType2} ),
                fromRefToOther1, fromRefToOther2, fromRefToRef, fromRefToThird, fromOtherToRef );

        assertRels( nodeGetRelationships( transaction, refNode, INCOMING ), fromOtherToRef );

        assertRels( nodeGetRelationships( transaction, refNode, INCOMING, new int[]{relType1} ), fromOtherToRef );

        assertRels( nodeGetRelationships( transaction, refNode, OUTGOING, new int[]{relType1, relType2} ),
                fromRefToOther1, fromRefToOther2, fromRefToThird, fromRefToRef );
        commit();
    }

    @Test
    void shouldInterleaveModifiedRelationshipsWithExistingOnes() throws Exception
    {
        // given
        long refNode;
        long fromRefToOther1;
        long fromRefToOther2;
        int relType1;
        int relType2;
        {
            KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );

            relType1 = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "Type1" );
            relType2 = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "Type2" );

            refNode = transaction.dataWrite().nodeCreate();
            long otherNode = transaction.dataWrite().nodeCreate();
            fromRefToOther1 = transaction.dataWrite().relationshipCreate( refNode,  relType1, otherNode );
            fromRefToOther2 = transaction.dataWrite().relationshipCreate( refNode,  relType2, otherNode );
            commit();
        }
        {
            KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );

            // When
            transaction.dataWrite().relationshipDelete( fromRefToOther1 );
            long endNode = transaction.dataWrite().nodeCreate();
            long localTxRel = transaction.dataWrite().relationshipCreate( refNode, relType1, endNode );

            // Then
            assertRels( nodeGetRelationships( transaction, refNode, BOTH ), fromRefToOther2, localTxRel);
            assertRelsInSeparateTx( refNode, BOTH, fromRefToOther1, fromRefToOther2);
            commit();
        }
    }

    @Test
    void shouldReturnRelsWhenAskingForRelsWhereOnlySomeTypesExistInCurrentRel() throws Exception
    {
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );

        int relType1 = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "Type1" );
        int relType2 = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "Type2" );

        long refNode = transaction.dataWrite().nodeCreate();
        long otherNode = transaction.dataWrite().nodeCreate();
        long theRel = transaction.dataWrite().relationshipCreate( refNode, relType1, otherNode );

        assertRels( nodeGetRelationships( transaction, refNode, OUTGOING, new int[]{relType2,relType1} ), theRel );
        commit();
    }

    @Test
    void askingForNonExistantReltypeOnDenseNodeShouldNotCorruptState() throws Exception
    {
        // Given a dense node with one type of rels
        long[] rels = new long[200];
        long refNode;
        int relTypeTheNodeDoesUse;
        int relTypeTheNodeDoesNotUse;
        {
            KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );

            relTypeTheNodeDoesUse = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "Type1" );
            relTypeTheNodeDoesNotUse = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "Type2" );

            refNode = transaction.dataWrite().nodeCreate();
            long otherNode = transaction.dataWrite().nodeCreate();

            for ( int i = 0; i < rels.length; i++ )
            {
                rels[i] =
                        transaction.dataWrite().relationshipCreate( refNode, relTypeTheNodeDoesUse, otherNode );
            }
            commit();
        }
        KernelTransaction transaction = newTransaction();

        // When I've asked for rels that the node does not have
        assertRels( nodeGetRelationships( transaction, refNode, INCOMING, new int[]{relTypeTheNodeDoesNotUse} ) );

        // Then the node should still load the real rels
        assertRels( nodeGetRelationships( transaction, refNode, BOTH, new int[]{relTypeTheNodeDoesUse} ), rels );
        commit();
    }

    private void assertRelsInSeparateTx( final long refNode, final Direction both, final long ... longs ) throws
            InterruptedException, ExecutionException, TimeoutException
    {
        assertTrue( otherThread.execute( state ->
        {
            try ( KernelTransaction ktx = kernel.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED ) )
            {
                assertRels( nodeGetRelationships( ktx, refNode, both ), longs );
            }
            return true;
        } ).get( 10, TimeUnit.SECONDS ) );
    }

    private static void assertRels( Iterator<Long> it, long... rels )
    {
        List<Long> list = Iterators.asList( it );
        assertThat( list ).contains( toObject( rels ) );
    }
}
