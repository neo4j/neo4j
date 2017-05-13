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
package org.neo4j.kernel.impl.api.integrationtest;

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.test.rule.concurrent.OtherThreadRule;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

public class RelationshipIT extends KernelIntegrationTest
{
    @Rule
    public OtherThreadRule<Object> otherThread = new OtherThreadRule<>( 10, TimeUnit.SECONDS );

    @Test
    public void shouldListRelationshipsInCurrentAndSubsequentTx() throws Exception
    {
        // given
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        int relType1 = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( "Type1" );
        int relType2 = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( "Type2" );

        long refNode = statement.dataWriteOperations().nodeCreate();
        long otherNode = statement.dataWriteOperations().nodeCreate();
        long fromRefToOther1 = statement.dataWriteOperations().relationshipCreate( relType1, refNode, otherNode );
        long fromRefToOther2 = statement.dataWriteOperations().relationshipCreate( relType2, refNode, otherNode );
        long fromOtherToRef = statement.dataWriteOperations().relationshipCreate( relType1, otherNode, refNode );
        long fromRefToRef = statement.dataWriteOperations().relationshipCreate( relType2, refNode, refNode );
        long endNode = statement.dataWriteOperations().nodeCreate();
        long fromRefToThird = statement.dataWriteOperations().relationshipCreate( relType2, refNode, endNode );

        // when & then
        assertRels( statement.readOperations().nodeGetRelationships( refNode, BOTH ), fromRefToOther1, fromRefToOther2,
                fromRefToRef, fromRefToThird, fromOtherToRef );

        assertRels( statement.readOperations().nodeGetRelationships( refNode, BOTH, new int[]{relType1} ),
                fromRefToOther1,
                fromOtherToRef );

        assertRels( statement.readOperations().nodeGetRelationships( refNode, BOTH, new int[]{relType1, relType2} ),
                fromRefToOther1, fromRefToOther2, fromRefToRef, fromRefToThird, fromOtherToRef );

        assertRels( statement.readOperations().nodeGetRelationships( refNode, INCOMING ), fromOtherToRef );

        assertRels( statement.readOperations().nodeGetRelationships( refNode, INCOMING, new int[]{relType1}
                /* none */ ) );

        assertRels( statement.readOperations().nodeGetRelationships( refNode, OUTGOING, new int[]{relType1, relType2} ),
                fromRefToOther1, fromRefToOther2, fromRefToThird, fromRefToRef );

        // when
        commit();
        ReadOperations readOperations = readOperationsInNewTransaction();

        // when & then
        assertRels( readOperations.nodeGetRelationships( refNode, BOTH ), fromRefToOther1, fromRefToOther2,
                fromRefToRef, fromRefToThird, fromOtherToRef );

        assertRels( readOperations.nodeGetRelationships( refNode, BOTH, new int[]{relType1} ), fromRefToOther1,
                fromOtherToRef );

        assertRels( readOperations.nodeGetRelationships( refNode, BOTH, new int[]{relType1, relType2} ),
                fromRefToOther1, fromRefToOther2, fromRefToRef, fromRefToThird, fromOtherToRef );

        assertRels( readOperations.nodeGetRelationships( refNode, INCOMING ), fromOtherToRef );

        assertRels( readOperations.nodeGetRelationships( refNode, INCOMING, new int[]{relType1} )
                /* none */ );

        assertRels( readOperations.nodeGetRelationships( refNode, OUTGOING, new int[]{relType1, relType2} ),
                fromRefToOther1, fromRefToOther2, fromRefToThird, fromRefToRef );
    }

    @Test
    public void shouldInterleaveModifiedRelationshipsWithExistingOnes() throws Exception
    {
        // given
        long refNode, fromRefToOther1, fromRefToOther2;
        int relType1, relType2;
        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

            relType1 = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( "Type1" );
            relType2 = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( "Type2" );

            refNode = statement.dataWriteOperations().nodeCreate();
            long otherNode = statement.dataWriteOperations().nodeCreate();
            fromRefToOther1 = statement.dataWriteOperations().relationshipCreate( relType1, refNode, otherNode );
            fromRefToOther2 = statement.dataWriteOperations().relationshipCreate( relType2, refNode, otherNode );
            commit();
        }
        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

            // When
            statement.dataWriteOperations().relationshipDelete( fromRefToOther1 );
            long endNode = statement.dataWriteOperations().nodeCreate();
            long localTxRel = statement.dataWriteOperations().relationshipCreate( relType1, refNode, endNode );

            // Then
            assertRels( statement.readOperations().nodeGetRelationships( refNode, BOTH ), fromRefToOther2, localTxRel);
            assertRelsInSeparateTx( refNode, BOTH, fromRefToOther1, fromRefToOther2);
        }
    }

    @Test
    public void shouldReturnRelsWhenAskingForRelsWhereOnlySomeTypesExistInCurrentRel() throws Exception
    {
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        int relType1 = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( "Type1" );
        int relType2 = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( "Type2" );

        long refNode = statement.dataWriteOperations().nodeCreate();
        long otherNode = statement.dataWriteOperations().nodeCreate();
        long theRel = statement.dataWriteOperations().relationshipCreate( relType1, refNode, otherNode );

        assertRels( statement.readOperations().nodeGetRelationships( refNode, OUTGOING, new int[]{relType2,relType1} ),
                theRel );
        commit();
    }

    @Test
    public void askingForNonExistantReltypeOnDenseNodeShouldNotCorruptState() throws Exception
    {
        // Given a dense node with one type of rels
        long[] rels = new long[200];
        long refNode;
        int relTypeTheNodeDoesUse, relTypeTheNodeDoesNotUse;
        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

            relTypeTheNodeDoesUse = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( "Type1" );
            relTypeTheNodeDoesNotUse = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( "Type2" );

            refNode = statement.dataWriteOperations().nodeCreate();
            long otherNode = statement.dataWriteOperations().nodeCreate();

            for ( int i = 0; i < rels.length; i++ )
            {
                rels[i] =
                        statement.dataWriteOperations().relationshipCreate( relTypeTheNodeDoesUse, refNode, otherNode );
            }
            commit();
        }
        ReadOperations stmt = readOperationsInNewTransaction();

        // When I've asked for rels that the node does not have
        assertRels( stmt.nodeGetRelationships( refNode, Direction.INCOMING, new int[]{relTypeTheNodeDoesNotUse} ) );

        // Then the node should still load the real rels
        assertRels( stmt.nodeGetRelationships( refNode, Direction.BOTH, new int[]{relTypeTheNodeDoesUse} ), rels );
    }

    private void assertRelsInSeparateTx( final long refNode, final Direction both, final long ... longs ) throws
            InterruptedException, ExecutionException, TimeoutException
    {
        assertTrue( otherThread.execute( state ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                ReadOperations stmt = statementContextSupplier.get().readOperations();
                assertRels( stmt.nodeGetRelationships( refNode, both ), longs );
            }
            return true;
        } ).get( 10, TimeUnit.SECONDS ) );
    }

    private void assertRels( PrimitiveLongIterator it, long ... rels )
    {
        List<Matcher<? super Iterable<Long>>> all = new ArrayList<>( rels.length );
        for ( long element : rels )
        {
            all.add(hasItem(element));
        }

        List<Long> list = PrimitiveLongCollections.asList( it );
        assertThat( list, allOf(all));
    }
}
