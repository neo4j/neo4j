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

import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.filter;
import static org.neo4j.kernel.api.schema.SchemaDescriptorPredicates.hasLabel;
import static org.neo4j.kernel.api.schema.SchemaDescriptorPredicates.hasProperty;
import static org.neo4j.kernel.impl.api.state.IndexTxStateUpdater.LabelChangeType.ADDED_LABEL;
import static org.neo4j.kernel.impl.api.state.IndexTxStateUpdater.LabelChangeType.REMOVED_LABEL;

public class IndexTxStateUpdaterTest
{
    private static final int labelId1 = 10;
    private static final int labelId2 = 11;
    private static final int unIndexedLabelId = 12;
    private static final int propId1 = 20;
    private static final int propId2 = 21;
    private static final int propId3 = 22;
    private static final int newPropId = 23;
    private static final int unIndexedPropId = 24;

    private KernelStatement state;
    private TransactionState txState;
    private IndexTxStateUpdater indexTxUpdater;
    private NodeItem node;

    private IndexDescriptor indexOn1_1 = IndexDescriptorFactory.forLabel( labelId1, propId1 );
    private IndexDescriptor indexOn2_new = IndexDescriptorFactory.forLabel( labelId2, newPropId );
    private IndexDescriptor uniqueOn1_2 = IndexDescriptorFactory.uniqueForLabel( labelId1, propId2 );
    private IndexDescriptor indexOn1_1_new = IndexDescriptorFactory.forLabel( labelId1, propId1, newPropId );
    private IndexDescriptor uniqueOn2_2_3 = IndexDescriptorFactory.uniqueForLabel( labelId2, propId2, propId3 );
    private List<IndexDescriptor> indexes =
            Arrays.asList( indexOn1_1, indexOn2_new, uniqueOn1_2, indexOn1_1_new, uniqueOn2_2_3 );

    @Before
    public void setup()
    {
        state = mock( KernelStatement.class );
        txState = mock( TransactionState.class );
        when( state.txState() ).thenReturn( txState );

        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        when( storeReadLayer.indexesGetAll() ).thenAnswer( x -> indexes.iterator() );
        when( storeReadLayer.indexesGetForLabel(anyInt() ) )
                .thenAnswer(
                    x -> filter( hasLabel( (Integer)x.getArguments()[0] ), indexes.iterator() ) );

        when( storeReadLayer.indexesGetRelatedToProperty( anyInt() ) )
                .thenAnswer(
                    x -> filter( hasProperty( (Integer)x.getArguments()[0] ), indexes.iterator() ) );

        PrimitiveIntSet labels = Primitive.intSet();
        labels.add( labelId1 );
        labels.add( labelId2 );

        Cursor<NodeItem> nodeItemCursor = StubCursors.asNodeCursor( 0, labels );
        nodeItemCursor.next();
        node = nodeItemCursor.get();

        PrimitiveIntSet defaultPropertyIds = PrimitiveIntCollections.asSet( new int[]{ propId1, propId2, propId3 } );
        EntityReadOperations readOps = mock( EntityReadOperations.class );
        when( readOps.nodeGetPropertyKeys( state, node ) ).thenReturn( defaultPropertyIds );
        when( readOps.nodeGetProperties( state, node ) ).thenAnswer( p -> StubCursors.asPropertyCursor(
                new PropertyKeyValue( propId1, Values.of( "hi1" ) ),
                new PropertyKeyValue( propId2, Values.of( "hi2" ) ),
                new PropertyKeyValue( propId3, Values.of( "hi3" ) ) ) );
        when( readOps.nodeGetProperty( state, node, propId1 ) ).thenReturn( Values.of( "hi1" ) );
        when( readOps.nodeGetProperty( state, node, propId2 ) ).thenReturn( Values.of( "hi2" ) );
        when( readOps.nodeGetProperty( state, node, propId3 ) ).thenReturn( Values.of( "hi3" ) );

        indexTxUpdater = new IndexTxStateUpdater( storeReadLayer, readOps );

    }

    // LABELS

    @Test
    public void shouldNotUpdateIndexesOnChangedIrrelevantLabel() throws EntityNotFoundException
    {
        // WHEN
        indexTxUpdater.onLabelChange( state, unIndexedLabelId, node, ADDED_LABEL );
        indexTxUpdater.onLabelChange( state, unIndexedLabelId, node, REMOVED_LABEL );

        // THEN
        verify( txState, times( 0 ) ).indexDoUpdateEntry( any(), anyInt(), any(), any() );
    }

    @Test
    public void shouldUpdateIndexesOnAddedLabel() throws EntityNotFoundException
    {
        // WHEN
        indexTxUpdater.onLabelChange( state, labelId1, node, ADDED_LABEL );

        // THEN
        verifyIndexUpdate( indexOn1_1.schema(), node.id(), null, values( "hi1" ) );
        verifyIndexUpdate( uniqueOn1_2.schema(), node.id(), null, values( "hi2" ) );
        verify( txState, times( 2 ) ).indexDoUpdateEntry( any(), anyInt(), any(), any() );
    }

    @Test
    public void shouldUpdateIndexesOnRemovedLabel() throws EntityNotFoundException
    {
        // WHEN
        indexTxUpdater.onLabelChange( state, labelId2, node, REMOVED_LABEL );

        // THEN
        verifyIndexUpdate( uniqueOn2_2_3.schema(), node.id(), values( "hi2", "hi3" ), null );
        verify( txState, times( 1 ) ).indexDoUpdateEntry( any(), anyInt(), any(), any() );
    }

    // PROPERTIES

    @Test
    public void shouldNotUpdateIndexesOnChangedIrrelevantProperty() throws EntityNotFoundException
    {
        // WHEN
        indexTxUpdater.onPropertyAdd( state, node, unIndexedPropId, Values.of( "whAt" ) );
        indexTxUpdater.onPropertyRemove( state, node, unIndexedPropId, Values.of( "whAt" ) );
        indexTxUpdater.onPropertyChange( state, node, unIndexedPropId, Values.of( "whAt" ), Values.of( "whAt2" ) );

        // THEN
        verify( txState, times( 0 ) ).indexDoUpdateEntry( any(), anyInt(), any(), any() );
    }

    @Test
    public void shouldUpdateIndexesOnAddedProperty() throws EntityNotFoundException
    {
        // WHEN
        indexTxUpdater.onPropertyAdd( state, node, newPropId, Values.of( "newHi" ) );

        // THEN
        verifyIndexUpdate( indexOn2_new.schema(), node.id(), null, values( "newHi" ) );
        verifyIndexUpdate( indexOn1_1_new.schema(), node.id(), null, values( "hi1", "newHi" ) );
        verify( txState, times( 2 ) ).indexDoUpdateEntry( any(), anyInt(), any(), any() );
    }

    @Test
    public void shouldUpdateIndexesOnRemovedProperty() throws EntityNotFoundException
    {
        // WHEN
        indexTxUpdater.onPropertyRemove( state, node, propId2, Values.of( "hi2" ) );

        // THEN
        verifyIndexUpdate( uniqueOn1_2.schema(), node.id(), values( "hi2" ), null );
        verifyIndexUpdate( uniqueOn2_2_3.schema(), node.id(), values( "hi2", "hi3" ), null );
        verify( txState, times( 2 ) ).indexDoUpdateEntry( any(), anyInt(), any(), any() );
    }

    @Test
    public void shouldUpdateIndexesOnChangesProperty() throws EntityNotFoundException
    {
        // WHEN
        indexTxUpdater.onPropertyChange( state, node, propId2, Values.of( "hi2" ), Values.of( "new2" ) );

        // THEN
        verifyIndexUpdate( uniqueOn1_2.schema(), node.id(), values( "hi2" ), values( "new2" ) );
        verifyIndexUpdate( uniqueOn2_2_3.schema(), node.id(), values( "hi2", "hi3" ), values( "new2", "hi3" ) );
        verify( txState, times( 2 ) ).indexDoUpdateEntry( any(), anyInt(), any(), any() );
    }

    private ValueTuple values( Object... values )
    {
        return ValueTuple.of( values );
    }

    private void verifyIndexUpdate(
            LabelSchemaDescriptor schema, long nodeId, ValueTuple before, ValueTuple after )
    {
        verify( txState ).indexDoUpdateEntry( eq( schema ), eq( nodeId), eq( before ), eq( after ) );
    }

}
