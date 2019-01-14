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
package org.neo4j.kernel.impl.transaction.command;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.concurrent.WorkSync;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingUpdateService;
import org.neo4j.kernel.impl.api.index.PropertyPhysicalToLogicalConverter;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory.uniqueForLabel;
import static org.neo4j.kernel.impl.store.record.IndexRule.constraintIndexRule;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

public class IndexBatchTransactionApplierTest
{
    @Test
    public void shouldProvideLabelScanStoreUpdatesSortedByNodeId() throws Exception
    {
        // GIVEN
        IndexingService indexing = mock( IndexingService.class );
        when( indexing.convertToIndexUpdates( any() ) ).thenAnswer( o -> Iterables.empty() );
        LabelScanWriter writer = new OrderVerifyingLabelScanWriter( 10, 15, 20 );
        WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanSync =
                spy( new WorkSync<>( singletonProvider( writer ) ) );
        WorkSync<IndexingUpdateService,IndexUpdatesWork> indexUpdatesSync = new WorkSync<>( indexing );
        TransactionToApply tx = mock( TransactionToApply.class );
        PropertyStore propertyStore = mock( PropertyStore.class );
        try ( IndexBatchTransactionApplier applier = new IndexBatchTransactionApplier( indexing, labelScanSync,
                indexUpdatesSync, mock( NodeStore.class ),
                new PropertyPhysicalToLogicalConverter( propertyStore ), new IndexActivator( indexing ) ) )
        {
            try ( TransactionApplier txApplier = applier.startTx( tx ) )
            {
                // WHEN
                txApplier.visitNodeCommand( node( 15 ) );
                txApplier.visitNodeCommand( node( 20 ) );
                txApplier.visitNodeCommand( node( 10 ) );
            }
        }
        // THEN all assertions happen inside the LabelScanWriter#write and #close
        verify( labelScanSync ).applyAsync( any() );
    }

    @Test
    public void shouldRegisterIndexesToActivateIntoTheActivator() throws Exception
    {
        // given
        IndexingService indexing = mock( IndexingService.class );
        LabelScanWriter writer = new OrderVerifyingLabelScanWriter( 10, 15, 20 );
        WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanSync =
                spy( new WorkSync<>( singletonProvider( writer ) ) );
        WorkSync<IndexingUpdateService,IndexUpdatesWork> indexUpdatesSync = new WorkSync<>( indexing );
        PropertyStore propertyStore = mock( PropertyStore.class );
        TransactionToApply tx = mock( TransactionToApply.class );
        IndexActivator indexActivator = new IndexActivator( indexing );
        long indexId1 = 1;
        long indexId2 = 2;
        long indexId3 = 3;
        long constraintId1 = 10;
        long constraintId2 = 11;
        long constraintId3 = 12;
        IndexProvider.Descriptor providerDescriptor = new IndexProvider.Descriptor( "index-key", "v1" );
        IndexRule rule1 = constraintIndexRule( indexId1, uniqueForLabel( 1, 1 ), providerDescriptor, constraintId1 );
        IndexRule rule2 = constraintIndexRule( indexId2, uniqueForLabel( 2, 1 ), providerDescriptor, constraintId2 );
        IndexRule rule3 = constraintIndexRule( indexId3, uniqueForLabel( 3, 1 ), providerDescriptor, constraintId3 );
        try ( IndexBatchTransactionApplier applier = new IndexBatchTransactionApplier( indexing, labelScanSync,
                indexUpdatesSync, mock( NodeStore.class ),
                new PropertyPhysicalToLogicalConverter( propertyStore ), indexActivator ) )
        {
            try ( TransactionApplier txApplier = applier.startTx( tx ) )
            {
                // WHEN
                // activate index 1
                txApplier.visitSchemaRuleCommand( new Command.SchemaRuleCommand( Collections.emptyList(), asRecords( rule1, true ), rule1 ) );

                // activate index 2
                txApplier.visitSchemaRuleCommand( new Command.SchemaRuleCommand( Collections.emptyList(), asRecords( rule2, true ), rule2 ) );

                // activate index 3
                txApplier.visitSchemaRuleCommand( new Command.SchemaRuleCommand( Collections.emptyList(), asRecords( rule3, true ), rule3 ) );

                // drop index 2
                txApplier.visitSchemaRuleCommand( new Command.SchemaRuleCommand( asRecords( rule2, true ), asRecords( rule2, false ), rule2 ) );
            }
        }

        verify( indexing ).dropIndex( rule2 );
        indexActivator.close();
        verify( indexing ).activateIndex( indexId1 );
        verify( indexing ).activateIndex( indexId3 );
        verifyNoMoreInteractions( indexing );
    }

    private Collection<DynamicRecord> asRecords( IndexRule rule, boolean inUse )
    {
        // Only used to transfer
        List<DynamicRecord> records = new ArrayList<>();
        DynamicRecord dynamicRecord = new DynamicRecord( rule.getId() );
        dynamicRecord.setInUse( inUse );
        records.add( dynamicRecord );
        return records;
    }

    private Supplier<LabelScanWriter> singletonProvider( final LabelScanWriter writer )
    {
        return () -> writer;
    }

    private NodeCommand node( long nodeId )
    {
        NodeRecord after = new NodeRecord( nodeId,
                true, false, NO_NEXT_RELATIONSHIP.intValue(),NO_NEXT_PROPERTY.intValue(), 0 );
        NodeLabelsField.parseLabelsField( after ).add( 1, null, null );

        return new NodeCommand( new NodeRecord( nodeId ), after );
    }

    private static class OrderVerifyingLabelScanWriter implements LabelScanWriter
    {
        private final long[] expectedNodeIds;
        private int cursor;

        OrderVerifyingLabelScanWriter( long... expectedNodeIds )
        {
            this.expectedNodeIds = expectedNodeIds;
        }

        @Override
        public void write( NodeLabelUpdate update )
        {
            assertEquals( expectedNodeIds[cursor], update.getNodeId() );
            cursor++;
        }

        @Override
        public void close()
        {
            assertEquals( cursor, expectedNodeIds.length );
        }
    }
}
