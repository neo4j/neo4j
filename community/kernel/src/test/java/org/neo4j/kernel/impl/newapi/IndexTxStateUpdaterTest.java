/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.StubPropertyCursor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.store.DefaultIndexReference;
import org.neo4j.storageengine.api.StorageReader;
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
import static org.neo4j.helpers.collection.Iterators.map;
import static org.neo4j.internal.kernel.api.schema.SchemaDescriptorPredicates.hasLabel;
import static org.neo4j.internal.kernel.api.schema.SchemaDescriptorPredicates.hasProperty;
import static org.neo4j.kernel.impl.api.store.DefaultIndexReference.fromDescriptor;
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

    private SchemaIndexDescriptor indexDescriptorOn1_1 = SchemaIndexDescriptorFactory.forLabel( labelId1, propId1 );
    private IndexReference indexOn1_1 = fromDescriptor( indexDescriptorOn1_1 );
    private SchemaIndexDescriptor indexDescriptorOn2_new = SchemaIndexDescriptorFactory.forLabel( labelId2, newPropId );
    private IndexReference indexOn2_new = fromDescriptor( indexDescriptorOn2_new );
    private SchemaIndexDescriptor uniqueDescriptorOn1_2 = SchemaIndexDescriptorFactory.uniqueForLabel( labelId1, propId2 );
    private IndexReference uniqueOn1_2 = fromDescriptor( uniqueDescriptorOn1_2 );
    private SchemaIndexDescriptor indexDescriptorOn1_1_new = SchemaIndexDescriptorFactory.forLabel( labelId1, propId1, newPropId );
    private IndexReference indexOn1_1_new = fromDescriptor( indexDescriptorOn1_1_new );
    private SchemaIndexDescriptor uniqueDescriptorOn2_2_3 = SchemaIndexDescriptorFactory.uniqueForLabel( labelId2, propId2, propId3 );
    private IndexReference uniqueOn2_2_3 = fromDescriptor( uniqueDescriptorOn2_2_3 );
    private List<SchemaIndexDescriptor> indexes =
            Arrays.asList( indexDescriptorOn1_1, indexDescriptorOn2_new, uniqueDescriptorOn1_2, indexDescriptorOn1_1_new, uniqueDescriptorOn2_2_3 );
    private StubNodeCursor node;
    private StubPropertyCursor propertyCursor;

    @Before
    public void setup() throws IndexNotFoundKernelException
    {
        txState = mock( TransactionState.class );

        StorageReader storageReader = mock( StorageReader.class );
        when( storageReader.indexesGetAll() ).thenAnswer( x -> indexes.iterator() );
        when( storageReader.indexesGetForLabel( anyInt() ) )
                .thenAnswer( x ->
                {
                    Integer argument = x.getArgument( 0 );
                    return map( DefaultIndexReference::fromDescriptor, filter( hasLabel( argument ), indexes.iterator() ) );
                } );

        when( storageReader.indexesGetRelatedToProperty( anyInt() ) )
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

        IndexingService indexingService = mock( IndexingService.class );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( any( SchemaDescriptor.class ) ) ).thenReturn( indexProxy );
        TxStateHolder txStateHolder = mock( TxStateHolder.class );
        when( txStateHolder.txState() ).thenReturn( txState );
        indexTxUpdater = new IndexTxStateUpdater( storageReader, txStateHolder, indexingService );
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
        verifyIndexUpdate( indexDescriptorOn1_1.schema(), node.nodeReference(), null, values( "hi1" ) );
        verifyIndexUpdate( uniqueDescriptorOn1_2.schema(), node.nodeReference(), null, values( "hi2" ) );
        verify( txState, times( 2 ) ).indexDoUpdateEntry( any(), anyLong(), isNull(), any() );
    }

    @Test
    public void shouldUpdateIndexesOnRemovedLabel()
    {
        // WHEN
        indexTxUpdater.onLabelChange( labelId2, node, propertyCursor, REMOVED_LABEL );

        // THEN
        verifyIndexUpdate( uniqueDescriptorOn2_2_3.schema(), node.nodeReference(), values( "hi2", "hi3" ), null );
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
        verifyIndexUpdate( indexDescriptorOn2_new.schema(), node.nodeReference(), null, values( "newHi" ) );
        verifyIndexUpdate( indexDescriptorOn1_1_new.schema(), node.nodeReference(), null, values( "hi1", "newHi" ) );
        verify( txState, times( 2 ) ).indexDoUpdateEntry( any(), anyLong(), isNull(), any() );
    }

    @Test
    public void shouldUpdateIndexesOnRemovedProperty()
    {
        // WHEN
        indexTxUpdater.onPropertyRemove( node, propertyCursor, propId2, Values.of( "hi2" ) );

        // THEN
        verifyIndexUpdate( uniqueDescriptorOn1_2.schema(), node.nodeReference(), values( "hi2" ), null );
        verifyIndexUpdate( uniqueDescriptorOn2_2_3.schema(), node.nodeReference(), values( "hi2", "hi3" ), null );
        verify( txState, times( 2 ) ).indexDoUpdateEntry( any(), anyLong(), any(), isNull() );
    }

    @Test
    public void shouldUpdateIndexesOnChangesProperty()
    {
        // WHEN
        indexTxUpdater.onPropertyChange( node, propertyCursor, propId2, Values.of( "hi2" ), Values.of( "new2" ) );

        // THEN
        verifyIndexUpdate( uniqueDescriptorOn1_2.schema(), node.nodeReference(), values( "hi2" ), values( "new2" ) );
        verifyIndexUpdate( uniqueDescriptorOn2_2_3.schema(), node.nodeReference(), values( "hi2", "hi3" ), values( "new2", "hi3" ) );
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
