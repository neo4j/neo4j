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
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import java.util.Arrays;

import org.neo4j.helpers.collection.Iterables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;

public abstract class TransactionStateTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Test
    public void shouldSeeNodeInTransaction() throws Exception
    {
        long nodeId;
        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            try ( NodeCursor node = cursors.allocateNodeCursor() )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( "should access node", node.next() );
                assertEquals( nodeId, node.nodeReference() );
                assertFalse( "should only find one node", node.next() );
            }
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertEquals( nodeId, graphDb.getNodeById( nodeId ).getId() );
        }
    }

    @Test
    public void shouldSeeNewLabeledNodeInTransaction() throws Exception
    {
        long nodeId;
        int labelId;
        final String labelName = "Town";

        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            labelId = session.token().labelGetOrCreateForName( labelName );
            tx.dataWrite().nodeAddLabel( nodeId, labelId );

            try ( NodeCursor node = cursors.allocateNodeCursor() )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( "should access node", node.next() );

                LabelSet labels = node.labels();
                assertEquals( 1, labels.numberOfLabels() );
                assertEquals( labelId, labels.label( 0 ) );
                assertFalse( "should only find one node", node.next() );
            }
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( nodeId ).getLabels(),
                    equalTo( Iterables.iterable( label( labelName ) ) ) );
        }
    }

    @Test
    public void shouldSeeLabelChangesInTransaction() throws Exception
    {
        long nodeId;
        int toRetain, toDelete, toAdd;
        final String toRetainName = "ToRetain";
        final String toDeleteName = "ToDelete";
        final String toAddName = "ToAdd";

        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            toRetain = session.token().labelGetOrCreateForName( toRetainName );
            toDelete = session.token().labelGetOrCreateForName( toDeleteName );
            tx.dataWrite().nodeAddLabel( nodeId, toRetain );
            tx.dataWrite().nodeAddLabel( nodeId, toDelete );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( nodeId ).getLabels(),
                    containsInAnyOrder( label( toRetainName ), label( toDeleteName ) ) );
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            toAdd = session.token().labelGetOrCreateForName( toAddName );
            tx.dataWrite().nodeAddLabel( nodeId, toAdd );
            tx.dataWrite().nodeRemoveLabel( nodeId, toDelete );

            try ( NodeCursor node = cursors.allocateNodeCursor() )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( "should access node", node.next() );

                assertLabels( node.labels(), toRetain, toAdd );
                assertFalse( "should only find one node", node.next() );
            }
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignored = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( nodeId ).getLabels(),
                    containsInAnyOrder( label( toRetainName ), label( toAddName ) ) );
        }
    }

    @Test
    public void shouldDiscoverDeletedNodeInTransaction() throws Exception
    {
        long nodeId;
        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            assertTrue( tx.dataWrite().nodeDelete( nodeId ) );
            try ( NodeCursor node = cursors.allocateNodeCursor() )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertFalse( node.next() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldHandleMultipleNodeDeletions() throws Exception
    {
        long nodeId;
        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            assertTrue( tx.dataWrite().nodeDelete( nodeId ) );
            assertFalse( tx.dataWrite().nodeDelete( nodeId ) );
            tx.success();
        }
    }

    private void assertLabels( LabelSet labels, int... expected )
    {
        assertEquals( expected.length, labels.numberOfLabels() );
        Arrays.sort( expected );
        int[] labelArray = new int[labels.numberOfLabels()];
        for ( int i = 0; i < labels.numberOfLabels(); i++ )
        {
            labelArray[i] = labels.label( i );
        }
        Arrays.sort( labelArray );
        assertTrue( "labels match expected", Arrays.equals( expected, labelArray ) );
    }
}
