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
package org.neo4j.kernel.impl.api.state;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexOperations;
import org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexing;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.state.StubCursors.cursor;
import static org.neo4j.kernel.impl.util.Cursors.empty;

public class StateOperationsAutoIndexingTest
{
    // TODO: This level of mocking is a massive smell, we're violating law of demeter like nobody's business
    private final InternalAutoIndexOperations nodeOps = mock( InternalAutoIndexOperations.class );
    private final InternalAutoIndexOperations relOps = mock( InternalAutoIndexOperations.class );
    private final AutoIndexing idx = mock( InternalAutoIndexing.class );
    private final StorageStatement storeStmt = mock( StorageStatement.class );
    private final DataWriteOperations writeOps = mock(DataWriteOperations.class);
    private final KernelStatement stmt = mock( KernelStatement.class, RETURNS_MOCKS );
    private final StoreReadLayer storeLayer = mock( StoreReadLayer.class, RETURNS_MOCKS );
    private final StateHandlingStatementOperations context = new StateHandlingStatementOperations(
            storeLayer, idx, mock(ConstraintIndexCreator.class), mock(ExplicitIndexStore.class) );

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
        when( storeStmt.acquireSingleNodeCursor( 1337 ) ).thenReturn( cursor( mock( NodeItem.class )) );

        // When
        context.nodeDelete( stmt, 1337 );

        // Then
        verify( nodeOps ).entityRemoved( writeOps, 1337 );
    }

    @Test
    public void shouldSignalRelationshipRemovedToAutoIndex() throws Exception
    {
        // Given
        when( storeStmt.acquireSingleRelationshipCursor( 1337 ) ).thenReturn( cursor( mock( RelationshipItem.class )) );

        // When
        context.relationshipDelete( stmt, 1337 );

        // Then
        verify( relOps ).entityRemoved( writeOps, 1337 );
    }

    @Test
    public void shouldSignalNodePropertyAddedToAutoIndex() throws Exception
    {
        // Given
        int propertyKeyId = 1;
        Value value = Values.of( "Hello!" );

        NodeItem node = mock( NodeItem.class );
        when( node.labels() ).thenReturn( PrimitiveIntCollections.emptySet() );
        when( storeStmt.acquireSingleNodeCursor( 1337 ) ).thenReturn( cursor( node ) );
        when( storeLayer.nodeGetProperty( eq( storeStmt ), any( NodeItem.class ), eq( propertyKeyId ),
                any( AssertOpen.class ) ) )
                .thenReturn( cursor() );

        // When
        context.nodeSetProperty( stmt, 1337, propertyKeyId, value );

        // Then
        verify( nodeOps ).propertyAdded( writeOps, 1337, propertyKeyId, value );
    }

    @Test
    public void shouldSignalRelationshipPropertyAddedToAutoIndex() throws Exception
    {
        // Given
        int propertyKeyId = 1;
        Value value = Values.of( "Hello!" );

        RelationshipItem relationship = mock( RelationshipItem.class );
        when( storeStmt.acquireSingleRelationshipCursor( 1337 ) ).thenReturn( cursor( relationship ) );
        when( storeLayer.relationshipGetProperty( storeStmt, relationship, propertyKeyId, AssertOpen.ALWAYS_OPEN ) )
                .thenReturn( empty() );

        // When
        context.relationshipSetProperty( stmt, 1337, propertyKeyId, value );

        // Then
        verify( relOps ).propertyAdded( writeOps, 1337, propertyKeyId, value );
    }

    @Test
    public void shouldSignalNodePropertyChangedToAutoIndex() throws Exception
    {
        // Given
        int propertyKeyId = 1;
        Value value = Values.of( "Hello!" );
        PropertyItem existingProperty = mock( PropertyItem.class );

        when(existingProperty.propertyKeyId()).thenReturn( propertyKeyId );
        when(existingProperty.value()).thenReturn( Values.of( "Goodbye!" ) );

        NodeItem node = mock( NodeItem.class );
        when( node.labels() ).thenReturn( PrimitiveIntCollections.emptySet() );
        when( storeStmt.acquireSingleNodeCursor( 1337 ) ).thenReturn( cursor( node ) );
        when( storeLayer.nodeGetProperty( eq( storeStmt ), any( NodeItem.class ), eq( propertyKeyId ),
                any( AssertOpen.class ) ) )
                .thenReturn( cursor( existingProperty ) );

        // When
        context.nodeSetProperty( stmt, 1337, propertyKeyId, value );

        // Then
        verify( nodeOps ).propertyChanged( eq(writeOps), eq(1337L),
                eq( propertyKeyId ), any( Value.class ), eq( value ) );
    }

    @Test
    public void shouldSignalRelationshipPropertyChangedToAutoIndex() throws Exception
    {
        // Given
        int propertyKeyId = 1;
        Value value = Values.of( "Hello!" );
        PropertyItem existingProperty = mock( PropertyItem.class );

        when(existingProperty.propertyKeyId()).thenReturn( propertyKeyId );
        when(existingProperty.value()).thenReturn( Values.of( "Goodbye!" ) );

        RelationshipItem relationship = mock( RelationshipItem.class );
        when( storeStmt.acquireSingleRelationshipCursor( 1337 ) ).thenReturn( cursor( relationship ) );
        when( storeLayer.relationshipGetProperty( storeStmt, relationship, propertyKeyId, stmt ) )
                .thenReturn( cursor( existingProperty ) );

        // When
        context.relationshipSetProperty( stmt, 1337, propertyKeyId, value );

        // Then
        verify( relOps ).propertyChanged( eq( writeOps ), eq( 1337L ),
                eq( propertyKeyId ), any( Value.class ), eq( value ) );
    }

    @Test
    public void shouldSignalNodePropertyRemovedToAutoIndex() throws Exception
    {
        // Given
        PropertyItem existingProperty = mock( PropertyItem.class );
        when( existingProperty.propertyKeyId() ).thenReturn( 1 );
        when( existingProperty.value() ).thenReturn( Values.of( "Goodbye!" ) );
        int propertyKeyId = existingProperty.propertyKeyId();

        NodeItem node = mock( NodeItem.class );
        when( storeLayer.nodeGetProperty( eq( storeStmt ), any( NodeItem.class ), eq( propertyKeyId ),
                any( AssertOpen.class ) ) )
                .thenReturn( cursor( existingProperty ) );
        when( node.labels() ).thenReturn( PrimitiveIntCollections.emptySet() );
        when( storeStmt.acquireSingleNodeCursor( 1337 ) ).thenReturn( cursor( node ) );

        // When
        context.nodeRemoveProperty( stmt, 1337, propertyKeyId );

        // Then
        verify( nodeOps ).propertyRemoved( writeOps, 1337L, propertyKeyId );
    }

    @Test
    public void shouldSignalRelationshipPropertyRemovedToAutoIndex() throws Exception
    {
        // Given
        PropertyItem existingProperty = mock( PropertyItem.class );

        int propertyKeyId = 1;
        when( existingProperty.propertyKeyId() ).thenReturn( propertyKeyId );
        when( existingProperty.value() ).thenReturn( Values.of( "Goodbye!" ) );

        RelationshipItem relationship = mock( RelationshipItem.class );
        when( storeStmt.acquireSingleRelationshipCursor( 1337 ) ).thenReturn( cursor( relationship ) );
        when( storeLayer.relationshipGetProperty( storeStmt, relationship, propertyKeyId, stmt ) )
                .thenReturn( cursor( existingProperty ) );

        // When
        context.relationshipRemoveProperty( stmt, 1337, existingProperty.propertyKeyId() );

        // Then
        verify( relOps ).propertyRemoved( writeOps, 1337L, existingProperty.propertyKeyId() );
    }
}
