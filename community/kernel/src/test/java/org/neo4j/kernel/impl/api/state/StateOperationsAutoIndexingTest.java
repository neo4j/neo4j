/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.state;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.legacyindex.AutoIndexing;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.legacyindex.InternalAutoIndexOperations;
import org.neo4j.kernel.impl.api.legacyindex.InternalAutoIndexing;
import org.neo4j.kernel.impl.index.LegacyIndexStore;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.api.properties.Property.property;

public class StateOperationsAutoIndexingTest
{
    // TODO: This level of mocking is a massive smell, we're violating law of demeter like nobody's business
    private final InternalAutoIndexOperations nodeOps = mock( InternalAutoIndexOperations.class );
    private final InternalAutoIndexOperations relOps = mock( InternalAutoIndexOperations.class );
    private final AutoIndexing idx = mock( InternalAutoIndexing.class );
    private final StorageStatement storeStmt = mock( StorageStatement.class );
    private final DataWriteOperations writeOps = mock(DataWriteOperations.class);
    private final KernelStatement stmt = mock( KernelStatement.class, RETURNS_MOCKS );
    private final StateHandlingStatementOperations context = new StateHandlingStatementOperations(
            mock(StoreReadLayer.class), idx, mock(ConstraintIndexCreator.class), mock(LegacyIndexStore.class) );

    @Before
    public void setup() throws InvalidTransactionTypeKernelException
    {
        when( idx.nodes() ).thenReturn( nodeOps );
        when( idx.relationships() ).thenReturn( relOps );
        when( stmt.getStoreStatement() ).thenReturn( storeStmt );
        when( stmt.dataWriteOperations() ).thenReturn( writeOps );
    }

    @Test
    public void shouldSignalNodeRemovedToAutoIndex() throws Exception
    {
        // Given
        when( storeStmt.acquireSingleNodeCursor( 1337 ) ).thenReturn( Cursors.cursor( mock( NodeItem.class )) );

        // When
        context.nodeDelete( stmt, 1337 );

        // Then
        verify( nodeOps ).entityRemoved( writeOps, 1337 );
    }

    @Test
    public void shouldSignalRelationshipRemovedToAutoIndex() throws Exception
    {
        // Given
        when( storeStmt.acquireSingleRelationshipCursor( 1337 ) ).thenReturn( Cursors.cursor( mock( RelationshipItem.class )) );

        // When
        context.relationshipDelete( stmt, 1337 );

        // Then
        verify( relOps ).entityRemoved( writeOps, 1337 );
    }

    @Test
    public void shouldSignalNodePropertyAddedToAutoIndex() throws Exception
    {
        // Given
        DefinedProperty property = property( 1, "Hello!" );

        NodeItem node = mock( NodeItem.class );
        when( node.property( property.propertyKeyId() )).thenReturn( Cursors.empty() );
        when( node.labels() ).thenReturn( Cursors.empty() );
        when( storeStmt.acquireSingleNodeCursor( 1337 ) ).thenReturn( Cursors.cursor( node ) );

        // When
        context.nodeSetProperty( stmt, 1337, property );

        // Then
        verify( nodeOps ).propertyAdded( writeOps, 1337, property );
    }

    @Test
    public void shouldSignalRelationshipPropertyAddedToAutoIndex() throws Exception
    {
        // Given
        DefinedProperty property = property( 1, "Hello!" );

        RelationshipItem rel = mock( RelationshipItem.class );
        when( rel.property( property.propertyKeyId() )).thenReturn( Cursors.empty() );
        when( storeStmt.acquireSingleRelationshipCursor( 1337 ) ).thenReturn( Cursors.cursor( rel ) );

        // When
        context.relationshipSetProperty( stmt, 1337, property );

        // Then
        verify( relOps ).propertyAdded( writeOps, 1337, property );
    }

    @Test
    public void shouldSignalNodePropertyChangedToAutoIndex() throws Exception
    {
        // Given
        DefinedProperty property = property( 1, "Hello!" );
        PropertyItem existingProperty = mock( PropertyItem.class );

        when(existingProperty.propertyKeyId()).thenReturn( property.propertyKeyId() );
        when(existingProperty.value()).thenReturn( "Goodbye!" );

        NodeItem node = mock( NodeItem.class );
        when( node.property( property.propertyKeyId() )).thenReturn( Cursors.cursor( existingProperty ) );
        when( node.labels() ).thenReturn( Cursors.empty() );
        when( storeStmt.acquireSingleNodeCursor( 1337 ) ).thenReturn( Cursors.cursor( node ) );

        // When
        context.nodeSetProperty( stmt, 1337, property );

        // Then
        verify( nodeOps ).propertyChanged( eq(writeOps), eq(1337l), any(Property.class), eq(property) );
    }

    @Test
    public void shouldSignalRelationshipPropertyChangedToAutoIndex() throws Exception
    {
        // Given
        DefinedProperty property = property( 1, "Hello!" );
        PropertyItem existingProperty = mock( PropertyItem.class );

        when(existingProperty.propertyKeyId()).thenReturn( property.propertyKeyId() );
        when(existingProperty.value()).thenReturn( "Goodbye!" );

        RelationshipItem rel = mock( RelationshipItem.class );
        when( rel.property( property.propertyKeyId() )).thenReturn( Cursors.cursor( existingProperty ) );
        when( storeStmt.acquireSingleRelationshipCursor( 1337 ) ).thenReturn( Cursors.cursor( rel ) );

        // When
        context.relationshipSetProperty( stmt, 1337, property );

        // Then
        verify( relOps ).propertyChanged( eq(writeOps), eq(1337l), any(Property.class), eq(property) );
    }

    @Test
    public void shouldSignalNodePropertyRemovedToAutoIndex() throws Exception
    {
        // Given
        PropertyItem existingProperty = mock( PropertyItem.class );

        when(existingProperty.propertyKeyId()).thenReturn( 1 );
        when(existingProperty.value()).thenReturn( "Goodbye!" );

        NodeItem node = mock( NodeItem.class );
        when( node.property( existingProperty.propertyKeyId() )).thenReturn( Cursors.cursor( existingProperty ) );
        when( node.labels() ).thenReturn( Cursors.empty() );
        when( storeStmt.acquireSingleNodeCursor( 1337 ) ).thenReturn( Cursors.cursor( node ) );

        // When
        context.nodeRemoveProperty( stmt, 1337, existingProperty.propertyKeyId() );

        // Then
        verify( nodeOps ).propertyRemoved( writeOps, 1337l, existingProperty.propertyKeyId() );
    }

    @Test
    public void shouldSignalRelationshipPropertyRemovedToAutoIndex() throws Exception
    {
        // Given
        PropertyItem existingProperty = mock( PropertyItem.class );

        when(existingProperty.propertyKeyId()).thenReturn( 1 );
        when(existingProperty.value()).thenReturn( "Goodbye!" );

        RelationshipItem rel = mock( RelationshipItem.class );
        when( rel.property( existingProperty.propertyKeyId() )).thenReturn( Cursors.cursor( existingProperty ) );
        when( storeStmt.acquireSingleRelationshipCursor( 1337 ) ).thenReturn( Cursors.cursor( rel ) );

        // When
        context.relationshipRemoveProperty( stmt, 1337, existingProperty.propertyKeyId() );

        // Then
        verify( relOps ).propertyRemoved( writeOps, 1337l, existingProperty.propertyKeyId() );
    }
}
