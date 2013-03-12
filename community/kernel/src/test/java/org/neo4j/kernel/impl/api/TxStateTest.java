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
package org.neo4j.kernel.impl.api;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import java.util.Collections;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.impl.api.state.OldTxStateBridge;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class TxStateTest
{

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
        Iterable<Long> nodes = state.getAddedNodesWithLabel( 2 );

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
        Iterable<Long> nodes = state.getRemovedNodesWithLabel( 2 );

        // THEN
        assertEquals( asSet( 0L, 2L ), asSet( nodes ) );
    }
    
    @Test
    public void shouldAddAndGetByLabel() throws Exception
    {
        // GIVEN
        long ruleId = 1, labelId = 2, labelId2 = 5, propertyKey = 3;
        IndexRule rule = newIndexRule( ruleId, labelId, propertyKey );
        IndexRule rule2 = newIndexRule( ruleId, labelId2, propertyKey );
        
        // WHEN
        state.addIndexRule( rule );
        state.addIndexRule( rule2 );
        
        // THEN
        assertEquals( asSet( rule ), state.getIndexRuleDiffSetsByLabel( labelId ).getAdded() );
    }

    @Test
    public void shouldAddAndGetByRuleId() throws Exception
    {
        // GIVEN
        long ruleId = 1, labelId = 2, propertyKey = 3;
        IndexRule rule = newIndexRule( ruleId, labelId, propertyKey );
        
        // WHEN
        state.addIndexRule( rule );
        
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

        DiffSets<Long> nodesWithChangedProp = new DiffSets<Long>( asSet(nodeId), emptySet );
        when( legacyState.getDeletedNodes() ).thenReturn( emptySet );
        when( legacyState.getNodesWithChangedProperty( propertyKey, propValue ) ).thenReturn( nodesWithChangedProp );

        // When
        DiffSets<Long> diff = state.getNodesWithChangedProperty( propertyKey, propValue );

        // Then
        assertThat( diff.getAdded(),   equalTo( asSet( nodeId ) ));
        assertThat( diff.getRemoved(), equalTo( emptySet ) );
    }
    
    @Test
    public void shouldExcludeNodesWithCorrectPropertyRemoved() throws Exception
    {
        // Given
        long nodeId = 1337l;
        int propertyKey = 2;
        int propValue = 42;

        DiffSets<Long> nodesWithChangedProp = new DiffSets<Long>( emptySet, asSet(nodeId) );
        when( legacyState.getNodesWithChangedProperty( propertyKey, propValue ) ).thenReturn( nodesWithChangedProp );

        // When
        DiffSets<Long> diff = state.getNodesWithChangedProperty( propertyKey, propValue );

        // Then
        assertThat( diff.getAdded(), equalTo( emptySet ) );
        assertThat( diff.getRemoved(),   equalTo( asSet( nodeId ) ));
    }

    private TxState state;
    private OldTxStateBridge legacyState;
    private final Set<Long> emptySet = Collections.<Long>emptySet();
    
    @Before
    public void before() throws Exception
    {
        legacyState = mock( OldTxStateBridge.class );
        state = new TxState(legacyState);
    }

    private IndexRule newIndexRule( long ruleId, long labelId, long propertyKey )
    {
        return new IndexRule( ruleId, labelId, propertyKey );
    }
}
