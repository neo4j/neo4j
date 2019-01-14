/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.kernel.api;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
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

    private static final String propertyKey = "prop";
    private static final String labelName = "Town";

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
        long node = createNode();

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
        long node = createNode();

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int labelId = tx.token().labelGetOrCreateForName( labelName );
            assertTrue( tx.dataWrite().nodeAddLabel( node, labelId ) );
            tx.success();
        }

        // Then
        assertLabels( node, labelName );
    }

    @Test
    public void shouldAddLabelNodeOnce() throws Exception
    {
        long node = createNodeWithLabel( labelName );

        try ( Transaction tx = session.beginTransaction() )
        {
            int labelId = tx.token().labelGetOrCreateForName( labelName );
            assertFalse( tx.dataWrite().nodeAddLabel( node, labelId ) );
            tx.success();
        }

        assertLabels( node, labelName );
    }

    @Test
    public void shouldRemoveLabel() throws Exception
    {
        long nodeId = createNodeWithLabel( labelName );

        try ( Transaction tx = session.beginTransaction() )
        {
            int labelId = tx.token().labelGetOrCreateForName( labelName );
            assertTrue( tx.dataWrite().nodeRemoveLabel( nodeId, labelId ) );
            tx.success();
        }

        assertNoLabels( nodeId );
    }

    @Test
    public void shouldNotAddLabelToNonExistingNode() throws Exception
    {
        long node = 1337L;

        try ( Transaction tx = session.beginTransaction() )
        {
            int labelId = tx.token().labelGetOrCreateForName( labelName );
            exception.expect( KernelException.class );
            tx.dataWrite().nodeAddLabel( node, labelId );
        }
    }

    @Test
    public void shouldRemoveLabelOnce() throws Exception
    {
        int labelId;
        long nodeId = createNodeWithLabel( labelName );

        try ( Transaction tx = session.beginTransaction() )
        {
            labelId = tx.token().labelGetOrCreateForName( labelName );
            assertTrue( tx.dataWrite().nodeRemoveLabel( nodeId, labelId ) );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            labelId = tx.token().labelGetOrCreateForName( labelName );
            assertFalse( tx.dataWrite().nodeRemoveLabel( nodeId, labelId ) );
            tx.success();
        }

        assertNoLabels( nodeId );
    }

    @Test
    public void shouldAddPropertyToNode() throws Exception
    {
        // Given
        long node = createNode();

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, stringValue( "hello" ) ), equalTo( NO_VALUE ) );
            tx.success();
        }

        // Then
        assertProperty( node, propertyKey, "hello" );
    }

    @Test
    public void shouldRollbackSetNodeProperty() throws Exception
    {
        // Given
        long node = createNode();

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, stringValue( "hello" ) ), equalTo( NO_VALUE ) );
            tx.failure();
        }

        // Then
        assertNoProperty( node, propertyKey );
    }

    @Test
    public void shouldThrowWhenSettingPropertyOnDeletedNode() throws Exception
    {
        // Given
        long node = createNode();
        deleteNode( node );

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            tx.dataWrite().nodeSetProperty( node, token, stringValue( "hello" ) );
            fail( "Expected EntityNotFoundException" );
        }
        catch ( EntityNotFoundException e )
        {
            // wanted
        }
    }

    @Test
    public void shouldUpdatePropertyToNode() throws Exception
    {
        // Given
        long node = createNodeWithProperty( propertyKey, 42 );

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, stringValue( "hello" ) ),
                    equalTo( intValue( 42 ) ) );
            tx.success();
        }

        // Then
        assertProperty( node, propertyKey, "hello" );
    }

    @Test
    public void shouldRemovePropertyFromNode() throws Exception
    {
        // Given
        long node = createNodeWithProperty( propertyKey, 42 );

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeRemoveProperty( node, token ),
                    equalTo( intValue( 42 ) ) );
            tx.success();
        }

        // Then
        assertNoProperty( node, propertyKey );
    }

    @Test
    public void shouldRemoveNonExistingPropertyFromNode() throws Exception
    {
        // Given
        long node = createNode();

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeRemoveProperty( node, token ), equalTo( NO_VALUE ) );
            tx.success();
        }

        // Then
        assertNoProperty( node, propertyKey );
    }

    @Test
    public void shouldRemovePropertyFromNodeTwice() throws Exception
    {
        // Given
        long node = createNodeWithProperty( propertyKey, 42 );

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeRemoveProperty( node, token ),
                    equalTo( intValue( 42 ) ) );
            assertThat( tx.dataWrite().nodeRemoveProperty( node, token ),
                    equalTo( NO_VALUE ) );
            tx.success();
        }

        // Then
        assertNoProperty( node, propertyKey );
    }

    @Test
    public void shouldUpdatePropertyToNodeInTransaction() throws Exception
    {
        // Given
        long node = createNode();

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, stringValue( "hello" ) ), equalTo( NO_VALUE ) );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, stringValue( "world" ) ), equalTo( stringValue( "hello" ) ) );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, intValue( 1337 ) ), equalTo( stringValue( "world" ) ) );
            tx.success();
        }

        // Then
        assertProperty( node, propertyKey, 1337 );
    }

    @Test
    public void shouldRemoveReSetAndTwiceRemovePropertyOnNode() throws Exception
    {
        // given
        long node = createNodeWithProperty( propertyKey, "bar" );

        // when

        try ( Transaction tx = session.beginTransaction() )
        {
            int prop = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            tx.dataWrite().nodeRemoveProperty( node, prop );
            tx.dataWrite().nodeSetProperty( node, prop, Values.of( "bar" ) );
            tx.dataWrite().nodeRemoveProperty( node, prop );
            tx.dataWrite().nodeRemoveProperty( node, prop );
            tx.success();
        }

        // then
        assertNoProperty( node, propertyKey );
    }

    @Test
    public void shouldNotWriteWhenSettingPropertyToSameValue() throws Exception
    {
        // Given
        Value theValue = stringValue( "The Value" );
        long nodeId = createNodeWithProperty( propertyKey, theValue.asObject() );

        // When
        Transaction tx = session.beginTransaction();
        int property = tx.token().propertyKeyGetOrCreateForName( propertyKey );
        assertThat( tx.dataWrite().nodeSetProperty( nodeId, property, theValue ), equalTo( theValue ) );
        tx.success();

        assertThat( tx.closeTransaction(), equalTo( Transaction.READ_ONLY ) );
    }

    @Test
    public void shouldSetAndReadLargeByteArrayPropertyToNode() throws Exception
    {
        // Given
        int prop;
        long node = createNode();
        Value largeByteArray = Values.of( new byte[100_000] );

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            prop = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeSetProperty( node, prop, largeByteArray ), equalTo( NO_VALUE ) );
            tx.success();
        }

        // Then
        try ( Transaction tx = session.beginTransaction();
              NodeCursor nodeCursor = tx.cursors().allocateNodeCursor();
              PropertyCursor propertyCursor = tx.cursors().allocatePropertyCursor(); )
        {
            tx.dataRead().singleNode( node, nodeCursor );
            assertTrue( nodeCursor.next() );
            nodeCursor.properties( propertyCursor );
            assertTrue( propertyCursor.next() );
            assertEquals( propertyCursor.propertyKey(), prop );
            assertThat( propertyCursor.propertyValue(), equalTo( largeByteArray ) );
        }
    }

    // HELPERS

    private long createNode()
    {
        long node;
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            node = graphDb.createNode().getId();
            ctx.success();
        }
        return node;
    }

    private void deleteNode( long node )
    {
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            graphDb.getNodeById( node ).delete();
            ctx.success();
        }
    }

    private long createNodeWithLabel( String labelName )
    {
        long node;
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            node = graphDb.createNode( label( labelName ) ).getId();
            ctx.success();
        }
        return node;
    }

    private long createNodeWithProperty( String propertyKey, Object value )
    {
        Node node;
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            node = graphDb.createNode();
            node.setProperty( propertyKey, value );
            ctx.success();
        }
        return node.getId();
    }

    private void assertNoLabels( long nodeId )
    {
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( graphDb.getNodeById( nodeId ).getLabels(), equalTo( Iterables.empty() ) );
        }
    }

    private void assertLabels( long nodeId, String label )
    {
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( graphDb.getNodeById( nodeId ).getLabels(), containsInAnyOrder( label( label ) ) );
        }
    }

    private void assertNoProperty( long node, String propertyKey )
    {
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertFalse( graphDb.getNodeById( node ).hasProperty( propertyKey ) );
        }
    }

    private void assertProperty( long node, String propertyKey, Object value )
    {
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( graphDb.getNodeById( node ).getProperty( propertyKey ), equalTo( value ) );
        }
    }
}
