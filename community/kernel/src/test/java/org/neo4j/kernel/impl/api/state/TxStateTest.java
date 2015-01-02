/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Collections;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.impl.util.DiffSets;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class TxStateTest
{
    private PersistenceManager persistenceManager;

    @Test
    public void shouldGetAddedLabels() throws Exception
    {
        // GIVEN
        state.nodeDoAddLabel( 1, 0 );
        state.nodeDoAddLabel( 1, 1 );
        state.nodeDoAddLabel( 2, 1 );

        // WHEN
        Set<Integer> addedLabels = state.nodeStateLabelDiffSets( 1 ).getAdded();

        // THEN
        assertEquals( asSet( 1, 2 ), addedLabels );
    }

    @Test
    public void shouldGetRemovedLabels() throws Exception
    {
        // GIVEN
        state.nodeDoRemoveLabel( 1, 0 );
        state.nodeDoRemoveLabel( 1, 1 );
        state.nodeDoRemoveLabel( 2, 1 );

        // WHEN
        Set<Integer> removedLabels = state.nodeStateLabelDiffSets( 1 ).getRemoved();

        // THEN
        assertEquals( asSet( 1, 2 ), removedLabels );
    }

    @Test
    public void removeAddedLabelShouldRemoveFromAdded() throws Exception
    {
        // GIVEN
        state.nodeDoAddLabel( 1, 0 );
        state.nodeDoAddLabel( 1, 1 );
        state.nodeDoAddLabel( 2, 1 );

        // WHEN
        state.nodeDoRemoveLabel( 1, 1 );

        // THEN
        assertEquals( asSet( 2 ), state.nodeStateLabelDiffSets( 1 ).getAdded() );
    }

    @Test
    public void addRemovedLabelShouldRemoveFromRemoved() throws Exception
    {
        // GIVEN
        state.nodeDoRemoveLabel( 1, 0 );
        state.nodeDoRemoveLabel( 1, 1 );
        state.nodeDoRemoveLabel( 2, 1 );

        // WHEN
        state.nodeDoAddLabel( 1, 1 );

        // THEN
        assertEquals( asSet( 2 ), state.nodeStateLabelDiffSets( 1 ).getRemoved() );
    }

    @Test
    public void shouldMapFromAddedLabelToNodes() throws Exception
    {
        // GIVEN
        state.nodeDoAddLabel( 1, 0 );
        state.nodeDoAddLabel( 2, 0 );
        state.nodeDoAddLabel( 1, 1 );
        state.nodeDoAddLabel( 3, 1 );
        state.nodeDoAddLabel( 2, 2 );

        // WHEN
        Set<Long> nodes = state.nodesWithLabelAdded( 2 );

        // THEN
        assertEquals( asSet( 0L, 2L ), asSet( nodes ) );
    }

    @Test
    public void shouldMapFromRemovedLabelToNodes() throws Exception
    {
        // GIVEN
        state.nodeDoRemoveLabel( 1, 0 );
        state.nodeDoRemoveLabel( 2, 0 );
        state.nodeDoRemoveLabel( 1, 1 );
        state.nodeDoRemoveLabel( 3, 1 );
        state.nodeDoRemoveLabel( 2, 2 );

        // WHEN
        Set<Long> nodes = state.nodesWithLabelChanged( 2 ).getRemoved();

        // THEN
        assertEquals( asSet( 0L, 2L ), asSet( nodes ) );
    }

    @Test
    public void shouldAddAndGetByLabel() throws Exception
    {
        // GIVEN
        int labelId = 2, labelId2 = 5, propertyKey = 3;

        // WHEN
        IndexDescriptor rule = new IndexDescriptor( labelId, propertyKey );
        state.indexRuleDoAdd( rule );
        state.indexRuleDoAdd( new IndexDescriptor( labelId2, propertyKey ) );

        // THEN
        assertEquals( asSet( rule ), state.indexDiffSetsByLabel( labelId ).getAdded() );
    }

    @Test
    public void shouldAddAndGetByRuleId() throws Exception
    {
        // GIVEN
        int labelId = 2, propertyKey = 3;

        // WHEN
        IndexDescriptor rule = new IndexDescriptor( labelId, propertyKey );
        state.indexRuleDoAdd( rule );

        // THEN
        assertEquals( asSet( rule ), state.indexChanges().getAdded() );
    }

    @Test
    public void shouldIncludeAddedNodesWithCorrectProperty() throws Exception
    {
        // Given
        long nodeId = 1337l;
        int propertyKey = 2;
        int propValue = 42;

        DiffSets<Long> nodesWithChangedProp = new DiffSets<>( asSet( nodeId ), emptySet );
        when( legacyState.getNodesWithChangedProperty( propertyKey, propValue ) ).thenReturn( nodesWithChangedProp );

        // When
        DiffSets<Long> diff = state.nodesWithChangedProperty( propertyKey, propValue );

        // Then
        assertThat( diff.getAdded(), equalTo( asSet( nodeId ) ) );
        assertThat( diff.getRemoved(), equalTo( emptySet ) );
    }

    @Test
    public void shouldExcludeNodesWithCorrectPropertyRemoved() throws Exception
    {
        // Given
        long nodeId = 1337l;
        int propertyKey = 2;
        int propValue = 42;

        DiffSets<Long> nodesWithChangedProp = new DiffSets<>( emptySet, asSet( nodeId ) );
        when( legacyState.getNodesWithChangedProperty( propertyKey, propValue ) ).thenReturn( nodesWithChangedProp );

        // When
        DiffSets<Long> diff = state.nodesWithChangedProperty( propertyKey, propValue );

        // Then
        assertThat( diff.getAdded(), equalTo( emptySet ) );
        assertThat( diff.getRemoved(), equalTo( asSet( nodeId ) ) );
    }

    @Test
    public void shouldListNodeAsDeletedIfItIsDeleted() throws Exception
    {
        // Given

        // When
        long nodeId = 1337l;
        state.nodeDoDelete( nodeId );

        // Then
        verify( legacyState ).deleteNode( nodeId );
        verifyNoMoreInteractions( legacyState, persistenceManager );

        assertThat( asSet( state.nodesDeletedInTx().getRemoved() ), equalTo( asSet( nodeId ) ) );
    }

    @Test
    public void shouldAddUniquenessConstraint() throws Exception
    {
        // when
        UniquenessConstraint constraint = new UniquenessConstraint( 1, 17 );
        state.constraintDoAdd( constraint, 7 );

        // then
        DiffSets<UniquenessConstraint> diff = state.constraintsChangesForLabel( 1 );
        assertEquals( Collections.singleton( constraint ), diff.getAdded() );
        assertTrue( diff.getRemoved().isEmpty() );
    }

    @Test
    public void addingUniquenessConstraintShouldBeIdempotent() throws Exception
    {
        // given
        UniquenessConstraint constraint1 = new UniquenessConstraint( 1, 17 );
        state.constraintDoAdd( constraint1, 7 );

        // when
        UniquenessConstraint constraint2 = new UniquenessConstraint( 1, 17 );
        state.constraintDoAdd( constraint2, 19 );

        // then
        assertEquals( constraint1, constraint2 );
        assertEquals( Collections.singleton( constraint1 ), state.constraintsChangesForLabel( 1 ).getAdded() );
    }

    @Test
    public void shouldDifferentiateBetweenUniquenessConstraintsForDifferentLabels() throws Exception
    {
        // when
        UniquenessConstraint constraint1 = new UniquenessConstraint( 1, 17 );
        state.constraintDoAdd( constraint1, 7 );
        UniquenessConstraint constraint2 = new UniquenessConstraint( 2, 17 );
        state.constraintDoAdd( constraint2, 19 );

        // then
        assertEquals( Collections.singleton( constraint1 ), state.constraintsChangesForLabel( 1 ).getAdded() );
        assertEquals( Collections.singleton( constraint2 ), state.constraintsChangesForLabel( 2 ).getAdded() );
    }

    private TxState state;
    private OldTxStateBridge legacyState;
    private final Set<Long> emptySet = Collections.emptySet();

    @Before
    public void before() throws Exception
    {
        legacyState = mock( OldTxStateBridge.class );
        persistenceManager = mock( PersistenceManager.class );
        state = new TxStateImpl( legacyState,
                persistenceManager, mock( TxState.IdGeneration.class )
        );
    }
}
