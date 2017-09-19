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

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.helpers.collection.Iterables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;

public abstract class NodeWriteTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Test
    public void shouldCreateNode() throws Exception
    {
        long node;
        try ( Transaction tx = kernel.beginTransaction() )
        {
            node = tx.nodeCreate();
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertEquals( node, graphDb.getNodeById( node ).getId() );
        }
    }

    @Test
    public void shouldRollbackOnFailure() throws Exception
    {
        long node;
        try ( Transaction tx = kernel.beginTransaction() )
        {
            node = tx.nodeCreate();
            tx.failure();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            graphDb.getNodeById( node );
            fail( "There should be no node" );
        }
        catch ( NotFoundException e )
        {
            // expected
        }
    }

    @Test
    public void shouldAddLabelNode() throws Exception
    {
        long node;
        int labelId;
        final String labelName = "Town";
        try ( Transaction tx = kernel.beginTransaction() )
        {
            node = tx.nodeCreate();
            labelId = kernel.token().labelGetOrCreateForName( labelName );
            tx.nodeAddLabel( node, labelId );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( node ).getLabels(),
                    equalTo( Iterables.iterable( label( labelName ) ) ) );
        }
    }

    @Test
    public void shouldRemoveLabel() throws Exception
    {
        long nodeId;
        int labelId;
        final String labelName = "Town";

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            nodeId = graphDb.createNode( label( labelName ) ).getId();
            tx.success();
        }

        try ( Transaction tx = kernel.beginTransaction() )
        {
            labelId = kernel.token().labelGetOrCreateForName( labelName );
            tx.nodeRemoveLabel( nodeId, labelId );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( nodeId ).getLabels(),
                    equalTo( Iterables.empty() ) );
        }
    }
}
