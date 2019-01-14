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

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.StubPropertyCursor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.filter;
import static org.neo4j.internal.kernel.api.schema.SchemaDescriptorPredicates.hasLabel;
import static org.neo4j.internal.kernel.api.schema.SchemaDescriptorPredicates.hasProperty;
import static org.neo4j.kernel.impl.newapi.IndexTxStateUpdater.LabelChangeType.ADDED_LABEL;
import static org.neo4j.kernel.impl.newapi.IndexTxStateUpdater.LabelChangeType.REMOVED_LABEL;

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

    private TransactionState txState;
    private IndexTxStateUpdater indexTxUpdater;

    private SchemaIndexDescriptor indexOn1_1 = SchemaIndexDescriptorFactory.forLabel( labelId1, propId1 );
    private SchemaIndexDescriptor indexOn2_new = SchemaIndexDescriptorFactory.forLabel( labelId2, newPropId );
    private SchemaIndexDescriptor uniqueOn1_2 = SchemaIndexDescriptorFactory.uniqueForLabel( labelId1, propId2 );
    private SchemaIndexDescriptor indexOn1_1_new = SchemaIndexDescriptorFactory.forLabel( labelId1, propId1, newPropId );
    private SchemaIndexDescriptor uniqueOn2_2_3 = SchemaIndexDescriptorFactory
            .uniqueForLabel( labelId2, propId2, propId3 );
    private List<SchemaIndexDescriptor> indexes =
            Arrays.asList( indexOn1_1, indexOn2_new, uniqueOn1_2, indexOn1_1_new, uniqueOn2_2_3 );
    private StubNodeCursor node;
    private StubPropertyCursor propertyCursor;

    @Before
    public void setup() throws IndexNotFoundKernelException
    {
        txState = mock( TransactionState.class );

        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        when( storeReadLayer.indexesGetAll() ).thenAnswer( x -> indexes.iterator() );
        when( storeReadLayer.indexesGetForLabel( anyInt() ) )
                .thenAnswer( x ->
                {
                    Integer argument = x.getArgument( 0 );
                    return filter( hasLabel( argument ), indexes.iterator() );
                } );

        when( storeReadLayer.indexesGetRelatedToProperty( anyInt() ) )
                .thenAnswer( x ->
                {
                    Integer argument = x.getArgument( 0 );
                    return filter( hasProperty( argument ), indexes.iterator() );
                } );

        HashMap<Integer,Value> map = new HashMap<>();
        map.put( propId1, Values.of( "hi1" ) );
        map.put( propId2, Values.of( "hi2" ) );
        map.put( propId3, Values.of( "hi3" ) );
        node = new StubNodeCursor().withNode( 0, new long[]{labelId1, labelId2}, map );
        node.next();

        propertyCursor = new StubPropertyCursor();

        Read readOps = mock( Read.class );
        when( readOps.txState() ).thenReturn( txState );

        IndexingService indexingService = mock( IndexingService.class );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( any( SchemaDescriptor.class ) ) ).thenReturn( indexProxy );
        indexTxUpdater = new IndexTxStateUpdater( storeReadLayer, readOps, indexingService );

    }

    // LABELS

    @Test
    public void shouldNotUpdateIndexesOnChangedIrrelevantLabel()
    {
        // WHEN
        indexTxUpdater.onLabelChange( unIndexedLabelId, node, propertyCursor, ADDED_LABEL );
        indexTxUpdater.onLabelChange( unIndexedLabelId, node, propertyCursor, REMOVED_LABEL );

        // THEN
        verify( txState, never() ).indexDoUpdateEntry( any(), anyInt(), any(), any() );
    }

    @Test
    public void shouldUpdateIndexesOnAddedLabel()
    {
        // WHEN
        indexTxUpdater.onLabelChange( labelId1, node, propertyCursor, ADDED_LABEL );

        // THEN
        verifyIndexUpdate( indexOn1_1.schema(), node.nodeReference(), null, values( "hi1" ) );
        verifyIndexUpdate( uniqueOn1_2.schema(), node.nodeReference(), null, values( "hi2" ) );
        verify( txState, times( 2 ) ).indexDoUpdateEntry( any(), anyLong(), isNull(), any() );
    }

    @Test
    public void shouldUpdateIndexesOnRemovedLabel()
    {
        // WHEN
        indexTxUpdater.onLabelChange( labelId2, node, propertyCursor, REMOVED_LABEL );

        // THEN
        verifyIndexUpdate( uniqueOn2_2_3.schema(), node.nodeReference(), values( "hi2", "hi3" ), null );
        verify( txState, times( 1 ) ).indexDoUpdateEntry( any(), anyLong(), any(), isNull() );
    }

    @Test
    public void shouldNotUpdateIndexesOnChangedIrrelevantProperty()
    {
        // WHEN
        indexTxUpdater.onPropertyAdd( node, propertyCursor, unIndexedPropId, Values.of( "whAt" ) );
        indexTxUpdater.onPropertyRemove( node, propertyCursor, unIndexedPropId, Values.of( "whAt" ) );
        indexTxUpdater.onPropertyChange( node, propertyCursor, unIndexedPropId, Values.of( "whAt" ), Values.of( "whAt2" ) );

        // THEN
        verify( txState, never() ).indexDoUpdateEntry( any(), anyInt(), any(), any() );
    }

    @Test
    public void shouldUpdateIndexesOnAddedProperty()
    {
        // WHEN
        indexTxUpdater.onPropertyAdd( node, propertyCursor, newPropId, Values.of( "newHi" ) );

        // THEN
        verifyIndexUpdate( indexOn2_new.schema(), node.nodeReference(), null, values( "newHi" ) );
        verifyIndexUpdate( indexOn1_1_new.schema(), node.nodeReference(), null, values( "hi1", "newHi" ) );
        verify( txState, times( 2 ) ).indexDoUpdateEntry( any(), anyLong(), isNull(), any() );
    }

    @Test
    public void shouldUpdateIndexesOnRemovedProperty()
    {
        // WHEN
        indexTxUpdater.onPropertyRemove( node, propertyCursor, propId2, Values.of( "hi2" ) );

        // THEN
        verifyIndexUpdate( uniqueOn1_2.schema(), node.nodeReference(), values( "hi2" ), null );
        verifyIndexUpdate( uniqueOn2_2_3.schema(), node.nodeReference(), values( "hi2", "hi3" ), null );
        verify( txState, times( 2 ) ).indexDoUpdateEntry( any(), anyLong(), any(), isNull() );
    }

    @Test
    public void shouldUpdateIndexesOnChangesProperty()
    {
        // WHEN
        indexTxUpdater.onPropertyChange( node, propertyCursor, propId2, Values.of( "hi2" ), Values.of( "new2" ) );

        // THEN
        verifyIndexUpdate( uniqueOn1_2.schema(), node.nodeReference(), values( "hi2" ), values( "new2" ) );
        verifyIndexUpdate( uniqueOn2_2_3.schema(), node.nodeReference(), values( "hi2", "hi3" ), values( "new2", "hi3" ) );
        verify( txState, times( 2 ) ).indexDoUpdateEntry( any(), anyLong(), any(), any() );
    }

    private ValueTuple values( Object... values )
    {
        return ValueTuple.of( values );
    }

    private void verifyIndexUpdate(
            SchemaDescriptor schema, long nodeId, ValueTuple before, ValueTuple after )
    {
        verify( txState ).indexDoUpdateEntry( eq( schema ), eq( nodeId ), eq( before ), eq( after ) );
    }
}
