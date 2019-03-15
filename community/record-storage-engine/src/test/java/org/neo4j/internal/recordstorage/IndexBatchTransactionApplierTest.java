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
package org.neo4j.internal.recordstorage;

import org.junit.Test;

import java.util.Optional;

import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.storageengine.api.DefaultStorageIndexReference;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.NodeLabelUpdate;
import org.neo4j.storageengine.api.NodeLabelUpdateListener;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.util.concurrent.WorkSync;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.internal.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

public class IndexBatchTransactionApplierTest
{
    @Test
    public void shouldProvideLabelScanStoreUpdatesSortedByNodeId() throws Exception
    {
        // GIVEN
        IndexUpdateListener indexUpdateListener = mock( IndexUpdateListener.class );
        OrderVerifyingUpdateListener listener = new OrderVerifyingUpdateListener( 10, 15, 20 );
        WorkSync<NodeLabelUpdateListener,LabelUpdateWork> labelScanSync = spy( new WorkSync<>( listener ) );
        WorkSync<IndexUpdateListener,IndexUpdatesWork> indexUpdatesSync = new WorkSync<>( indexUpdateListener );
        PropertyStore propertyStore = mock( PropertyStore.class );
        try ( IndexBatchTransactionApplier applier = new IndexBatchTransactionApplier( indexUpdateListener, labelScanSync, indexUpdatesSync,
                mock( NodeStore.class ), mock( RelationshipStore.class ), propertyStore,
                mock( StorageEngine.class ), mock( SchemaCache.class ), new IndexActivator( indexUpdateListener ) ) )
        {
            try ( TransactionApplier txApplier = applier.startTx( new GroupOfCommands() ) )
            {
                // WHEN
                txApplier.visitNodeCommand( node( 15 ) );
                txApplier.visitNodeCommand( node( 20 ) );
                txApplier.visitNodeCommand( node( 10 ) );
            }
        }
        listener.done();
        // THEN all assertions happen inside the LabelScanWriter#write and #close
        verify( labelScanSync ).applyAsync( any() );
    }

    @Test
    public void shouldRegisterIndexesToActivateIntoTheActivator() throws Exception
    {
        // given
        IndexUpdateListener indexUpdateListener = mock( IndexUpdateListener.class );
        OrderVerifyingUpdateListener listener = new OrderVerifyingUpdateListener( 10, 15, 20 );
        WorkSync<NodeLabelUpdateListener,LabelUpdateWork> labelScanSync = spy( new WorkSync<>( listener ) );
        WorkSync<IndexUpdateListener,IndexUpdatesWork> indexUpdatesSync = new WorkSync<>( indexUpdateListener );
        PropertyStore propertyStore = mock( PropertyStore.class );
        IndexActivator indexActivator = new IndexActivator( indexUpdateListener );
        long indexId1 = 1;
        long indexId2 = 2;
        long indexId3 = 3;
        long constraintId1 = 10;
        long constraintId2 = 11;
        long constraintId3 = 12;
        String providerKey = "index-key";
        String providerVersion = "v1";
        StorageIndexReference rule1 = uniqueForSchema( forLabel( 1, 1 ), providerKey, providerVersion, indexId1, constraintId1 );
        StorageIndexReference rule2 = uniqueForSchema( forLabel( 2, 1 ), providerKey, providerVersion, indexId2, constraintId2 );
        StorageIndexReference rule3 = uniqueForSchema( forLabel( 3, 1 ), providerKey, providerVersion, indexId3, constraintId3 );
        try ( IndexBatchTransactionApplier applier = new IndexBatchTransactionApplier( indexUpdateListener, labelScanSync,
                indexUpdatesSync, mock( NodeStore.class ), mock( RelationshipStore.class ), propertyStore,
                mock( StorageEngine.class ), mock( SchemaCache.class ), indexActivator ) )
        {
            try ( TransactionApplier txApplier = applier.startTx( new GroupOfCommands() ) )
            {
                // WHEN
                // activate index 1
                txApplier.visitSchemaRuleCommand( new Command.SchemaRuleCommand( new SchemaRecord( rule1.getId() ), asSchemaRecord( rule1, true ), rule1 ) );

                // activate index 2
                txApplier.visitSchemaRuleCommand( new Command.SchemaRuleCommand( new SchemaRecord( rule2.getId() ), asSchemaRecord( rule2, true ), rule2 ) );

                // activate index 3
                txApplier.visitSchemaRuleCommand( new Command.SchemaRuleCommand( new SchemaRecord( rule3.getId() ), asSchemaRecord( rule3, true ), rule3 ) );

                // drop index 2
                txApplier.visitSchemaRuleCommand( new Command.SchemaRuleCommand( asSchemaRecord( rule2, true ), asSchemaRecord( rule2, false ), rule2 ) );
            }
        }

        verify( indexUpdateListener ).dropIndex( rule2 );
        indexActivator.close();
        verify( indexUpdateListener ).activateIndex( rule1 );
        verify( indexUpdateListener ).activateIndex( rule3 );
        verifyNoMoreInteractions( indexUpdateListener );
    }

    private StorageIndexReference uniqueForSchema( SchemaDescriptor schema, String providerKey, String providerVersion, long id, long owningConstraint )
    {
        return new DefaultStorageIndexReference( schema, providerKey, providerVersion, id, Optional.empty(), true, owningConstraint, false );
    }

    private SchemaRecord asSchemaRecord( SchemaRule rule, boolean inUse )
    {
        // Only used to transfer
        return new SchemaRecord( rule.getId() ).initialize( inUse, NO_NEXT_PROPERTY.longValue() );
    }

    private NodeCommand node( long nodeId )
    {
        NodeRecord after = new NodeRecord( nodeId,
                true, false, NO_NEXT_RELATIONSHIP.intValue(),NO_NEXT_PROPERTY.intValue(), 0 );
        NodeLabelsField.parseLabelsField( after ).add( 1, null, null );

        return new NodeCommand( new NodeRecord( nodeId ), after );
    }

    private static class OrderVerifyingUpdateListener implements NodeLabelUpdateListener
    {
        private final long[] expectedNodeIds;
        private int cursor;

        OrderVerifyingUpdateListener( long... expectedNodeIds )
        {
            this.expectedNodeIds = expectedNodeIds;
        }

        @Override
        public void applyUpdates( Iterable<NodeLabelUpdate> labelUpdates )
        {
            for ( NodeLabelUpdate update : labelUpdates )
            {
                assertEquals( expectedNodeIds[cursor], update.getNodeId() );
                cursor++;
            }
        }

        void done()
        {
            assertEquals( cursor, expectedNodeIds.length );
        }
    }
}
