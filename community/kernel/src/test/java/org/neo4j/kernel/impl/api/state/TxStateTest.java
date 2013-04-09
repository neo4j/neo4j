/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import java.util.Collections;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.xa.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

public class TxStateTest
{

    private PersistenceManager persistenceManager;
    private NodeManager nodeManager;

    @Test
    public void shouldGetAddedLabels() throws Exception
    {
        // GIVEN
        state.addLabelToNode( 1, 0 );
        state.addLabelToNode( 1, 1 );
        state.addLabelToNode( 2, 1 );

        // WHEN
        Set<Long> addedLabels = state.getNodeStateLabelDiffSets( 1 ).getAdded();

        // THEN
        assertEquals( asSet( 1L, 2L ), addedLabels );
    }

    @Test
    public void shouldGetRemovedLabels() throws Exception
    {
        // GIVEN
        state.removeLabelFromNode( 1, 0 );
        state.removeLabelFromNode( 1, 1 );
        state.removeLabelFromNode( 2, 1 );

        // WHEN
        Set<Long> removedLabels = state.getNodeStateLabelDiffSets( 1 ).getRemoved();

        // THEN
        assertEquals( asSet( 1L, 2L ), removedLabels );
    }

    @Test
    public void removeAddedLabelShouldRemoveFromAdded() throws Exception
    {
        // GIVEN
        state.addLabelToNode( 1, 0 );
        state.addLabelToNode( 1, 1 );
        state.addLabelToNode( 2, 1 );

        // WHEN
        state.removeLabelFromNode( 1, 1 );

        // THEN
        assertEquals( asSet( 2L ), state.getNodeStateLabelDiffSets( 1 ).getAdded() );
    }

    @Test
    public void addRemovedLabelShouldRemoveFromRemoved() throws Exception
    {
        // GIVEN
        state.removeLabelFromNode( 1, 0 );
        state.removeLabelFromNode( 1, 1 );
        state.removeLabelFromNode( 2, 1 );

        // WHEN
        state.addLabelToNode( 1, 1 );

        // THEN
        assertEquals( asSet( 2L ), state.getNodeStateLabelDiffSets( 1 ).getRemoved() );
    }

    @Test
    public void shouldMapFromAddedLabelToNodes() throws Exception
    {
        // GIVEN
        state.addLabelToNode( 1, 0 );
        state.addLabelToNode( 2, 0 );
        state.addLabelToNode( 1, 1 );
        state.addLabelToNode( 3, 1 );
        state.addLabelToNode( 2, 2 );

        // WHEN
        Set<Long> nodes = state.getNodesWithLabelAdded( 2 );

        // THEN
        assertEquals( asSet( 0L, 2L ), asSet( nodes ) );
    }

    @Test
    public void shouldMapFromRemovedLabelToNodes() throws Exception
    {
        // GIVEN
        state.removeLabelFromNode( 1, 0 );
        state.removeLabelFromNode( 2, 0 );
        state.removeLabelFromNode( 1, 1 );
        state.removeLabelFromNode( 3, 1 );
        state.removeLabelFromNode( 2, 2 );

        // WHEN
        Set<Long> nodes = state.getNodesWithLabelChanged( 2 ).getRemoved();

        // THEN
        assertEquals( asSet( 0L, 2L ), asSet( nodes ) );
    }

    @Test
    public void shouldAddAndGetByLabel() throws Exception
    {
        // GIVEN
        long labelId = 2, labelId2 = 5, propertyKey = 3;

        // WHEN
        IndexRule rule = state.addIndexRule( labelId, propertyKey );
        state.addIndexRule( labelId2, propertyKey );

        // THEN
        assertEquals( asSet( rule ), state.getIndexRuleDiffSetsByLabel( labelId ).getAdded() );
    }

    @Test
    public void shouldAddAndGetByRuleId() throws Exception
    {
        // GIVEN
        long labelId = 2, propertyKey = 3;

        // WHEN
        IndexRule rule = state.addIndexRule( labelId, propertyKey );

        // THEN
        assertEquals( asSet( rule ), state.getIndexRuleDiffSets().getAdded() );
    }

    @Test
    public void shouldIncludeAddedNodesWithCorrectProperty() throws Exception
    {
        // Given
        long nodeId = 1337l;
        int propertyKey = 2;
        int propValue = 42;

        DiffSets<Long> nodesWithChangedProp = new DiffSets<Long>( asSet( nodeId ), emptySet );
        when( legacyState.getNodesWithChangedProperty( propertyKey, propValue ) ).thenReturn( nodesWithChangedProp );

        // When
        DiffSets<Long> diff = state.getNodesWithChangedProperty( propertyKey, propValue );

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

        DiffSets<Long> nodesWithChangedProp = new DiffSets<Long>( emptySet, asSet( nodeId ) );
        when( legacyState.getNodesWithChangedProperty( propertyKey, propValue ) ).thenReturn( nodesWithChangedProp );

        // When
        DiffSets<Long> diff = state.getNodesWithChangedProperty( propertyKey, propValue );

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
        state.deleteNode( nodeId );

        // Then
        verify( legacyState ).deleteNode( nodeId );
        verifyNoMoreInteractions( legacyState, persistenceManager );

        assertThat( asSet( state.getDeletedNodes().getRemoved() ), equalTo( asSet( nodeId ) ) );
    }

    private TxState state;
    private OldTxStateBridge legacyState;
    private final Set<Long> emptySet = Collections.<Long>emptySet();

    @Before
    public void before() throws Exception
    {
        legacyState = mock( OldTxStateBridge.class );
        persistenceManager = mock( PersistenceManager.class );
        nodeManager = mock( NodeManager.class );
        state = new TxState( legacyState,
                persistenceManager, mock( TxState.IdGeneration.class ),
                new DefaultSchemaIndexProviderMap( SchemaIndexProvider.NO_INDEX_PROVIDER ) );
    }
}
