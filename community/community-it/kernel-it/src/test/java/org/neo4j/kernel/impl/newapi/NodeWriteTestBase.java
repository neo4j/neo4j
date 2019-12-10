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
package org.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Test;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

@SuppressWarnings( "Duplicates" )
public abstract class NodeWriteTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    private static final String propertyKey = "prop";
    private static final String labelName = "Town";

    @Test
    void shouldCreateNode() throws Exception
    {
        long node;
        try ( KernelTransaction tx = beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertEquals( node, tx.getNodeById( node ).getId() );
        }
    }

    @Test
    void shouldRollbackOnFailure() throws Exception
    {
        long node;
        try ( KernelTransaction tx = beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();
            tx.rollback();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            tx.getNodeById( node );
            fail( "There should be no node" );
        }
        catch ( NotFoundException e )
        {
            // expected
        }
    }

    @Test
    void shouldRemoveNode() throws Exception
    {
        long node = createNode();

        try ( KernelTransaction tx = beginTransaction() )
        {
            tx.dataWrite().nodeDelete( node );
            tx.commit();
        }
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            try
            {
                tx.getNodeById( node );
                fail( "Did not remove node" );
            }
            catch ( NotFoundException e )
            {
                // expected
            }
        }
    }

    @Test
    void shouldNotRemoveNodeThatDoesNotExist() throws Exception
    {
        long node = 0;

        try ( KernelTransaction tx = beginTransaction() )
        {
            assertFalse( tx.dataWrite().nodeDelete( node ) );
            tx.rollback();
        }
        try ( KernelTransaction tx = beginTransaction() )
        {
            assertFalse( tx.dataWrite().nodeDelete( node ) );
            tx.commit();
        }
        // should not crash
    }

    @Test
    void shouldAddLabelNode() throws Exception
    {
        // Given
        long node = createNode();

        // When
        try ( KernelTransaction tx = beginTransaction() )
        {
            int labelId = tx.token().labelGetOrCreateForName( labelName );
            assertTrue( tx.dataWrite().nodeAddLabel( node, labelId ) );
            tx.commit();
        }

        // Then
        assertLabels( node, labelName );
    }

    @Test
    void shouldAddLabelNodeOnce() throws Exception
    {
        long node = createNodeWithLabel( labelName );

        try ( KernelTransaction tx = beginTransaction() )
        {
            int labelId = tx.token().labelGetOrCreateForName( labelName );
            assertFalse( tx.dataWrite().nodeAddLabel( node, labelId ) );
            tx.commit();
        }

        assertLabels( node, labelName );
    }

    @Test
    void shouldRemoveLabel() throws Exception
    {
        long nodeId = createNodeWithLabel( labelName );

        try ( KernelTransaction tx = beginTransaction() )
        {
            int labelId = tx.token().labelGetOrCreateForName( labelName );
            assertTrue( tx.dataWrite().nodeRemoveLabel( nodeId, labelId ) );
            tx.commit();
        }

        assertNoLabels( nodeId );
    }

    @Test
    void shouldNotAddLabelToNonExistingNode() throws Exception
    {
        long node = 1337L;

        try ( KernelTransaction tx = beginTransaction() )
        {
            int labelId = tx.token().labelGetOrCreateForName( labelName );
            assertThrows( KernelException.class, () -> tx.dataWrite().nodeAddLabel( node, labelId ) );
        }
    }

    @Test
    void shouldRemoveLabelOnce() throws Exception
    {
        int labelId;
        long nodeId = createNodeWithLabel( labelName );

        try ( KernelTransaction tx = beginTransaction() )
        {
            labelId = tx.token().labelGetOrCreateForName( labelName );
            assertTrue( tx.dataWrite().nodeRemoveLabel( nodeId, labelId ) );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            labelId = tx.token().labelGetOrCreateForName( labelName );
            assertFalse( tx.dataWrite().nodeRemoveLabel( nodeId, labelId ) );
            tx.commit();
        }

        assertNoLabels( nodeId );
    }

    @Test
    void shouldAddPropertyToNode() throws Exception
    {
        // Given
        long node = createNode();

        // When
        try ( KernelTransaction tx = beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, stringValue( "hello" ) ) ).isEqualTo( NO_VALUE );
            tx.commit();
        }

        // Then
        assertProperty( node, propertyKey, "hello" );
    }

    @Test
    void shouldRollbackSetNodeProperty() throws Exception
    {
        // Given
        long node = createNode();

        // When
        try ( KernelTransaction tx = beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, stringValue( "hello" ) ) ).isEqualTo( NO_VALUE );
            tx.rollback();
        }

        // Then
        assertNoProperty( node, propertyKey );
    }

    @Test
    void shouldThrowWhenSettingPropertyOnDeletedNode() throws Exception
    {
        // Given
        long node = createNode();
        deleteNode( node );

        // When
        try ( KernelTransaction tx = beginTransaction() )
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
    void shouldUpdatePropertyToNode() throws Exception
    {
        // Given
        long node = createNodeWithProperty( propertyKey, 42 );

        // When
        try ( KernelTransaction tx = beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, stringValue( "hello" ) ) ).isEqualTo( intValue( 42 ) );
            tx.commit();
        }

        // Then
        assertProperty( node, propertyKey, "hello" );
    }

    @Test
    void shouldRemovePropertyFromNode() throws Exception
    {
        // Given
        long node = createNodeWithProperty( propertyKey, 42 );

        // When
        try ( KernelTransaction tx = beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeRemoveProperty( node, token ) ).isEqualTo( intValue( 42 ) );
            tx.commit();
        }

        // Then
        assertNoProperty( node, propertyKey );
    }

    @Test
    void shouldRemoveNonExistingPropertyFromNode() throws Exception
    {
        // Given
        long node = createNode();

        // When
        try ( KernelTransaction tx = beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeRemoveProperty( node, token ) ).isEqualTo( NO_VALUE );
            tx.commit();
        }

        // Then
        assertNoProperty( node, propertyKey );
    }

    @Test
    void shouldRemovePropertyFromNodeTwice() throws Exception
    {
        // Given
        long node = createNodeWithProperty( propertyKey, 42 );

        // When
        try ( KernelTransaction tx = beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeRemoveProperty( node, token ) ).isEqualTo( intValue( 42 ) );
            assertThat( tx.dataWrite().nodeRemoveProperty( node, token ) ).isEqualTo( NO_VALUE );
            tx.commit();
        }

        // Then
        assertNoProperty( node, propertyKey );
    }

    @Test
    void shouldUpdatePropertyToNodeInTransaction() throws Exception
    {
        // Given
        long node = createNode();

        // When
        try ( KernelTransaction tx = beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, stringValue( "hello" ) ) ).isEqualTo( NO_VALUE );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, stringValue( "world" ) ) ).isEqualTo( stringValue( "hello" ) );
            assertThat( tx.dataWrite().nodeSetProperty( node, token, intValue( 1337 ) ) ).isEqualTo( stringValue( "world" ) );
            tx.commit();
        }

        // Then
        assertProperty( node, propertyKey, 1337 );
    }

    @Test
    void shouldRemoveReSetAndTwiceRemovePropertyOnNode() throws Exception
    {
        // given
        long node = createNodeWithProperty( propertyKey, "bar" );

        // when

        try ( KernelTransaction tx = beginTransaction() )
        {
            int prop = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            tx.dataWrite().nodeRemoveProperty( node, prop );
            tx.dataWrite().nodeSetProperty( node, prop, Values.of( "bar" ) );
            tx.dataWrite().nodeRemoveProperty( node, prop );
            tx.dataWrite().nodeRemoveProperty( node, prop );
            tx.commit();
        }

        // then
        assertNoProperty( node, propertyKey );
    }

    @Test
    void shouldNotWriteWhenSettingPropertyToSameValue() throws Exception
    {
        // Given
        Value theValue = stringValue( "The Value" );
        long nodeId = createNodeWithProperty( propertyKey, theValue.asObject() );

        // When
        KernelTransaction tx = beginTransaction();
        int property = tx.token().propertyKeyGetOrCreateForName( propertyKey );
        assertThat( tx.dataWrite().nodeSetProperty( nodeId, property, theValue ) ).isEqualTo( theValue );

        assertThat( tx.commit() ).isEqualTo( KernelTransaction.READ_ONLY_ID );
    }

    @Test
    void shouldSetAndReadLargeByteArrayPropertyToNode() throws Exception
    {
        // Given
        int prop;
        long node = createNode();
        Value largeByteArray = Values.of( new byte[100_000] );

        // When
        try ( KernelTransaction tx = beginTransaction() )
        {
            prop = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().nodeSetProperty( node, prop, largeByteArray ) ).isEqualTo( NO_VALUE );
            tx.commit();
        }

        // Then
        try ( KernelTransaction tx = beginTransaction();
              NodeCursor nodeCursor = tx.cursors().allocateNodeCursor();
              PropertyCursor propertyCursor = tx.cursors().allocatePropertyCursor() )
        {
            tx.dataRead().singleNode( node, nodeCursor );
            assertTrue( nodeCursor.next() );
            nodeCursor.properties( propertyCursor );
            assertTrue( propertyCursor.next() );
            assertEquals( propertyCursor.propertyKey(), prop );
            assertThat( propertyCursor.propertyValue() ).isEqualTo( largeByteArray );
        }
    }

    // HELPERS

    private long createNode()
    {
        long node;
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            node = ctx.createNode().getId();
            ctx.commit();
        }
        return node;
    }

    private void deleteNode( long node )
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            tx.getNodeById( node ).delete();
            tx.commit();
        }
    }

    private long createNodeWithLabel( String labelName )
    {
        long node;
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            node = ctx.createNode( label( labelName ) ).getId();
            ctx.commit();
        }
        return node;
    }

    private long createNodeWithProperty( String propertyKey, Object value )
    {
        Node node;
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            node = ctx.createNode();
            node.setProperty( propertyKey, value );
            ctx.commit();
        }
        return node.getId();
    }

    private void assertNoLabels( long nodeId )
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat( tx.getNodeById( nodeId ).getLabels() ).isEqualTo( Iterables.empty() );
        }
    }

    private void assertLabels( long nodeId, String label )
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat( tx.getNodeById( nodeId ).getLabels() ).contains( label( label ) );
        }
    }

    private void assertNoProperty( long node, String propertyKey )
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertFalse( tx.getNodeById( node ).hasProperty( propertyKey ) );
        }
    }

    private void assertProperty( long node, String propertyKey, Object value )
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat( tx.getNodeById( node ).getProperty( propertyKey ) ).isEqualTo( value );
        }
    }
}
