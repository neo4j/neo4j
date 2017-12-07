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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

@SuppressWarnings( "Duplicates" )
public abstract class NodeWriteTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldCreateNode() throws Exception
    {
        long node;
        try ( Transaction tx = session.beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertEquals( node, graphDb.getNodeById( node ).getId() );
        }
    }

    @Test
    public void shouldRollbackOnFailure() throws Exception
    {
        long node;
        try ( Transaction tx = session.beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();
            tx.failure();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
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
    public void shouldRemoveNode() throws Exception
    {
        long node;
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            node = graphDb.createNode().getId();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            tx.dataWrite().nodeDelete( node );
            tx.success();
        }
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            try
            {
                graphDb.getNodeById( node );
                fail( "Did not remove node" );
            }
            catch ( NotFoundException e )
            {
                // expected
            }
        }
    }

    @Test
    public void shouldNotRemoveNodeThatDoesNotExist() throws Exception
    {
        long node = 0;

        try ( Transaction tx = session.beginTransaction() )
        {
            assertFalse( tx.dataWrite().nodeDelete( node ) );
            tx.failure();
        }
        try ( Transaction tx = session.beginTransaction() )
        {
            assertFalse( tx.dataWrite().nodeDelete( node ) );
            tx.success();
        }
        // should not crash
    }

    @Test
    public void shouldAddLabelNode() throws Exception
    {
        // Given
        long node;
        int labelId;
        final String labelName = "Town";
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            node = graphDb.createNode().getId();
            ctx.success();
        }

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            labelId = session.token().labelGetOrCreateForName( labelName );
            tx.dataWrite().nodeAddLabel( node, labelId );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( graphDb.getNodeById( node ).getLabels(), equalTo( Iterables.iterable( label( labelName ) ) ) );
        }
    }

    @Test
    public void shouldAddLabelNodeOnce() throws Exception
    {
        long node;
        int labelId;
        final String labelName = "Town";

        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            node = graphDb.createNode( label( labelName ) ).getId();
            ctx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            labelId = session.token().labelGetOrCreateForName( labelName );
            assertFalse( tx.dataWrite().nodeAddLabel( node, labelId ) );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( graphDb.getNodeById( node ).getLabels(), equalTo( Iterables.iterable( label( labelName ) ) ) );
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

        try ( Transaction tx = session.beginTransaction() )
        {
            labelId = session.token().labelGetOrCreateForName( labelName );
            tx.dataWrite().nodeRemoveLabel( nodeId, labelId );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( graphDb.getNodeById( nodeId ).getLabels(), equalTo( Iterables.empty() ) );
        }
    }

    @Test
    public void shouldNotAddLabelToNonExistingNode() throws Exception
    {
        long node = 1337L;
        int labelId;
        final String labelName = "Town";
        try ( Transaction tx = session.beginTransaction() )
        {
            labelId = session.token().labelGetOrCreateForName( labelName );
            exception.expect( KernelException.class );
            tx.dataWrite().nodeAddLabel( node, labelId );
        }
    }

    @Test
    public void shouldRemoveLabelOnce() throws Exception
    {
        long nodeId;
        int labelId;
        final String labelName = "Town";

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            nodeId = graphDb.createNode( label( labelName ) ).getId();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            labelId = session.token().labelGetOrCreateForName( labelName );
            assertTrue( tx.dataWrite().nodeRemoveLabel( nodeId, labelId ) );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            labelId = session.token().labelGetOrCreateForName( labelName );
            assertFalse( tx.dataWrite().nodeRemoveLabel( nodeId, labelId ) );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( graphDb.getNodeById( nodeId ).getLabels(), equalTo( Iterables.empty() ) );
        }
    }

    @Test
    public void shouldAddPropertyToNode() throws Exception
    {
        // Given
        long node;
        String propertyKey = "prop";
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            node = graphDb.createNode().getId();
            ctx.success();
        }

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = session.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, stringValue( "hello" ) ), equalTo( NO_VALUE ) );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( graphDb.getNodeById( node ).getProperty( "prop" ), equalTo( "hello" ) );
        }
    }

    @Test
    public void shouldUpdatePropertyToNode() throws Exception
    {
        // Given
        long node;
        String propertyKey = "prop";
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            Node proxy = graphDb.createNode();
            proxy.setProperty( propertyKey, 42 );
            node = proxy.getId();
            ctx.success();
        }

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = session.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, stringValue( "hello" ) ),
                    equalTo( intValue( 42 ) ) );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( graphDb.getNodeById( node ).getProperty( "prop" ), equalTo( "hello" ) );
        }
    }

    @Test
    public void shouldRemovePropertyFromNode() throws Exception
    {
        // Given
        long node;
        String propertyKey = "prop";
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            Node proxy = graphDb.createNode();
            proxy.setProperty( propertyKey, 42 );
            node = proxy.getId();
            ctx.success();
        }

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = session.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeRemoveProperty( node, token ),
                    equalTo( intValue( 42 ) ) );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertFalse( graphDb.getNodeById( node ).hasProperty( "prop" ) );
        }
    }

    @Test
    public void shouldRemoveNonExistingPropertyFromNode() throws Exception
    {
        // Given
        long node;
        String propertyKey = "prop";
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            node = graphDb.createNode().getId();
            ctx.success();
        }

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = session.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeRemoveProperty( node, token ),
                    equalTo( NO_VALUE ) );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertFalse( graphDb.getNodeById( node ).hasProperty( "prop" ) );
        }
    }

    @Test
    public void shouldRemovePropertyFromNodeTwice() throws Exception
    {
        // Given
        long node;
        String propertyKey = "prop";
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            Node proxy = graphDb.createNode();
            proxy.setProperty( propertyKey, 42 );
            node = proxy.getId();
            ctx.success();
        }

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = session.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeRemoveProperty( node, token ),
                    equalTo( intValue( 42 ) ) );
            assertThat( tx.dataWrite().nodeRemoveProperty( node, token ),
                    equalTo( NO_VALUE ) );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertFalse( graphDb.getNodeById( node ).hasProperty( "prop" ) );
        }
    }

    @Test
    public void shouldUpdatePropertyToNodeInTransaction() throws Exception
    {
        // Given
        long node;
        String propertyKey = "prop";
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            node = graphDb.createNode().getId();
            ctx.success();
        }

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = session.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, stringValue( "hello" ) ), equalTo( NO_VALUE ) );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, stringValue( "world" ) ), equalTo( stringValue( "hello" ) ) );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, intValue( 1337 ) ), equalTo( stringValue( "world" ) ) );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( graphDb.getNodeById( node ).getProperty( "prop" ), equalTo( 1337 ) );
        }
    }
}
