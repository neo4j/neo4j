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
package org.neo4j.kernel.impl.api.integrationtest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadRule;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.helpers.collection.IteratorUtil.asList;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.IteratorUtil.toJavaIterator;

public class RelationshipIT extends KernelIntegrationTest
{
    @Rule
    public OtherThreadRule<Object> otherThread = new OtherThreadRule<>( 10, TimeUnit.SECONDS );

    @Test
    public void shouldListRelationshipsInCurrentAndSubsequentTx() throws Exception
    {
        // given
        long refNode, fromRefToOther1, fromRefToOther2, fromOtherToRef, fromRefToRef, fromRefToThird;
        int relType1, relType2;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            relType1 = statement.relationshipTypeGetOrCreateForName( "Type1" );
            relType2 = statement.relationshipTypeGetOrCreateForName( "Type2" );

            refNode = statement.nodeCreate();
            long otherNode = statement.nodeCreate();
            fromRefToOther1 = statement.relationshipCreate( relType1, refNode, otherNode );
            fromRefToOther2 = statement.relationshipCreate( relType2, refNode, otherNode );
            fromOtherToRef = statement.relationshipCreate( relType1, otherNode, refNode );
            fromRefToRef = statement.relationshipCreate( relType2, refNode, refNode );
            fromRefToThird = statement.relationshipCreate( relType2, refNode, statement.nodeCreate() );

            // when & then
            assertRels( statement.nodeGetRelationships( refNode, BOTH ),
                        fromRefToOther1, fromRefToOther2, fromRefToRef, fromRefToThird, fromOtherToRef);

            assertRels( statement.nodeGetRelationships( refNode, BOTH, new int[]{relType1} ),
                    fromRefToOther1, fromOtherToRef);

            assertRels( statement.nodeGetRelationships( refNode, BOTH, new int[]{relType1, relType2} ),
                    fromRefToOther1, fromRefToOther2, fromRefToRef, fromRefToThird, fromOtherToRef);


            assertRels( statement.nodeGetRelationships( refNode, INCOMING ), fromOtherToRef );

            assertRels( statement.nodeGetRelationships( refNode, INCOMING, new int[]{relType1} ) /* none */);

            assertRels( statement.nodeGetRelationships( refNode, OUTGOING, new int[]{relType1, relType2} ),
                    fromRefToOther1, fromRefToOther2, fromRefToThird, fromRefToRef);

            // when
            commit();
        }
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            // when & then
            assertRels( statement.nodeGetRelationships( refNode, BOTH ),
                    fromRefToOther1, fromRefToOther2, fromRefToRef, fromRefToThird, fromOtherToRef);

            assertRels( statement.nodeGetRelationships( refNode, BOTH, new int[]{relType1}),
                    fromRefToOther1, fromOtherToRef);

            assertRels( statement.nodeGetRelationships( refNode, BOTH, new int[]{relType1, relType2} ),
                    fromRefToOther1, fromRefToOther2, fromRefToRef, fromRefToThird, fromOtherToRef);

            assertRels( statement.nodeGetRelationships( refNode, INCOMING ), fromOtherToRef );

            assertRels( statement.nodeGetRelationships( refNode, INCOMING, new int[]{relType1} ) /* none */);

            assertRels( statement.nodeGetRelationships( refNode, OUTGOING, new int[]{relType1, relType2} ),
                    fromRefToOther1, fromRefToOther2, fromRefToThird, fromRefToRef);
        }
    }

    @Test
    public void shouldInterleaveModifiedRelationshipsWithExistingOnes() throws Exception
    {
        // given
        long refNode, fromRefToOther1, fromRefToOther2;
        int relType1, relType2;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            relType1 = statement.relationshipTypeGetOrCreateForName( "Type1" );
            relType2 = statement.relationshipTypeGetOrCreateForName( "Type2" );

            refNode = statement.nodeCreate();
            long otherNode = statement.nodeCreate();
            fromRefToOther1 = statement.relationshipCreate( relType1, refNode, otherNode );
            fromRefToOther2 = statement.relationshipCreate( relType2, refNode, otherNode );
            commit();
        }
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            // When
            statement.relationshipDelete( fromRefToOther1 );
            long localTxRel = statement.relationshipCreate( relType1, refNode, statement.nodeCreate() );

            // Then
            assertRels( statement.nodeGetRelationships( refNode, BOTH ), fromRefToOther2, localTxRel);
            assertRelsInSeparateTx( refNode, BOTH, fromRefToOther1, fromRefToOther2);
        }
    }

    @Test
    public void shouldAllowIteratingAndDeletingRelsAtTheSameTime() throws Exception
    {
        // given
        long refNode, fromRefToOther1, fromRefToOther2;
        int relType1, relType2;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            relType1 = statement.relationshipTypeGetOrCreateForName( "Type1" );
            relType2 = statement.relationshipTypeGetOrCreateForName( "Type2" );

            refNode = statement.nodeCreate();
            long otherNode = statement.nodeCreate();
            fromRefToOther1 = statement.relationshipCreate( relType1, refNode, otherNode );
            fromRefToOther2 = statement.relationshipCreate( relType2, refNode, otherNode );
            commit();
        }
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            // When
            statement.relationshipDelete( fromRefToOther1 );
            long localTxRel = statement.relationshipCreate( relType1, refNode, statement.nodeCreate() );

            // Then
            assertRels( statement.nodeGetRelationships( refNode, BOTH ), fromRefToOther2, localTxRel);
            assertRelsInSeparateTx( refNode, BOTH, fromRefToOther1, fromRefToOther2);
        }
    }

    @Test
    public void shouldReturnRelsWhenAskingForRelsWhereOnlySomeTypesExistInCurrentRel() throws Exception
    {
        // given
        long refNode, theRel;
        int relType1, relType2;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            relType1 = statement.relationshipTypeGetOrCreateForName( "Type1" );
            relType2 = statement.relationshipTypeGetOrCreateForName( "Type2" );

            refNode = statement.nodeCreate();
            long otherNode = statement.nodeCreate();
            theRel = statement.relationshipCreate( relType1, refNode, otherNode );

            assertRels( statement.nodeGetRelationships( refNode, Direction.OUTGOING, new int[]{relType2, relType1} ), theRel );

            commit();
        }
    }

    @Test
    public void askingForNonExistantReltypeOnDenseNodeShouldNotCorruptState() throws Exception
    {
        // Given a dense node with one type of rels
        long[] rels = new long[200];
        long refNode;
        int relTypeTheNodeDoesUse, relTypeTheNodeDoesNotUse;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            relTypeTheNodeDoesUse = statement.relationshipTypeGetOrCreateForName( "Type1" );
            relTypeTheNodeDoesNotUse = statement.relationshipTypeGetOrCreateForName( "Type2" );

            refNode = statement.nodeCreate();
            long otherNode = statement.nodeCreate();

            for ( int i = 0; i < rels.length; i++ )
            {
                rels[i] = statement.relationshipCreate( relTypeTheNodeDoesUse, refNode, otherNode );
            }
            commit();
        }

        {
            ReadOperations stmt = readOperationsInNewTransaction();

            // When I've asked for rels that the node does not have
            assertRels( stmt.nodeGetRelationships(refNode, Direction.INCOMING, new int[]{relTypeTheNodeDoesNotUse}) );

            // Then the node should still load the real rels
            assertRels( stmt.nodeGetRelationships(refNode, Direction.BOTH, new int[]{relTypeTheNodeDoesUse}), rels );
        }
    }

    @Test
    public void shouldHandleInterleavedSmallAndLargeRelGroupLoading() throws Exception
    {
        int grabSize = Integer.parseInt(GraphDatabaseSettings.relationship_grab_size.getDefaultValue());

        // Given a dense node with one type of rels
        long[] largeRelGroup = new long[grabSize + 1];
        long[] smallRelGroup = new long[1];

        long refNode;
        int largeGroupType, smallGroupType;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            largeGroupType = statement.relationshipTypeGetOrCreateForName( "Type1" );
            smallGroupType = statement.relationshipTypeGetOrCreateForName( "Type2" );

            refNode = statement.nodeCreate();
            long otherNode = statement.nodeCreate();

            for ( int i = 0; i < largeRelGroup.length; i++ )
            {
                largeRelGroup[i] = statement.relationshipCreate( largeGroupType, refNode, otherNode );
            }
            for ( int i = 0; i < smallRelGroup.length; i++ )
            {
                smallRelGroup[i] = statement.relationshipCreate( smallGroupType, refNode, otherNode );
            }

            commit();
        }

        {
            ReadOperations stmt = readOperationsInNewTransaction();

            // When I exhaust up to the grab size
            PrimitiveLongIterator iter = stmt.nodeGetRelationships( refNode, Direction.BOTH, new int[]{largeGroupType} );
            for ( int i = 0; i < grabSize - 1; i++ )
            {
                assertTrue(iter.hasNext());
                iter.next();
            }

            // Then both the small rel group, the remaining rels in the iterator should work
            assertRels( stmt.nodeGetRelationships( refNode, Direction.BOTH, new int[]{smallGroupType} ) );
            assertThat( count( toJavaIterator( iter ) ), equalTo(2) );
            assertRels( stmt.nodeGetRelationships( refNode, Direction.BOTH, new int[]{largeGroupType} ), largeRelGroup );
        }
    }

    private void assertRelsInSeparateTx( final long refNode, final Direction both, final long ... longs ) throws
            InterruptedException, ExecutionException, TimeoutException
    {
        assertTrue( otherThread.execute( new OtherThreadExecutor.WorkerCommand<Object, Boolean>()
        {
            @Override
            public Boolean doWork( Object state ) throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    ReadOperations stmt = statementContextSupplier.get().readOperations();
                    assertRels( stmt.nodeGetRelationships( refNode, both ), longs );
                }
                return true;
            }
        } ).get( 10, TimeUnit.SECONDS ) );
    }

    private void assertRels( PrimitiveLongIterator it, long ... rels )
    {
        List<Matcher<? super Iterable<Long>>> all = new ArrayList<>(rels.length);
        for (long element : rels) {
            all.add(hasItem(element));
        }

        List<Long> list = asList( it );
        assertThat( list, allOf(all));
    }
}
