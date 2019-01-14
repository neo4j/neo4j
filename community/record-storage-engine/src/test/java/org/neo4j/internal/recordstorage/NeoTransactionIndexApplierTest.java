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

import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

import org.neo4j.kernel.api.index.IndexProviderDescriptor;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.index.schema.IndexDescriptorFactory;
import org.neo4j.kernel.impl.index.schema.StoreIndexDescriptor;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.NodeLabelUpdateListener;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.util.concurrent.WorkSync;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.impl.store.record.DynamicRecord.dynamicRecord;

public class NeoTransactionIndexApplierTest
{
    private static final IndexProviderDescriptor INDEX_DESCRIPTOR = new IndexProviderDescriptor( "in-memory", "1.0" );

    private final IndexingService indexingService = mock( IndexingService.class );
    private final IndexUpdateListener indexUpdateListener = mock( IndexUpdateListener.class );
    private final SchemaCache schemaCache = mock( SchemaCache.class );
    @SuppressWarnings( "unchecked" )
    private final NodeLabelUpdateListener labelUpdateListener = mock( NodeLabelUpdateListener.class );
    private final Collection<DynamicRecord> emptyDynamicRecords = Collections.emptySet();
    private final WorkSync<NodeLabelUpdateListener,LabelUpdateWork> labelScanStoreSynchronizer = new WorkSync<>( labelUpdateListener );
    private final WorkSync<IndexUpdateListener,IndexUpdatesWork> indexUpdatesSync = new WorkSync<>( indexUpdateListener );
    private final TransactionToApply transactionToApply = mock( TransactionToApply.class );

    @Before
    public void setup()
    {
        when( transactionToApply.transactionId() ).thenReturn( 1L );
    }

    @Test
    public void shouldUpdateLabelStoreScanOnNodeCommands() throws Exception
    {
        // given
        final IndexBatchTransactionApplier applier = newIndexTransactionApplier();
        final NodeRecord before = new NodeRecord( 11 );
        before.setLabelField( 17, emptyDynamicRecords );
        final NodeRecord after = new NodeRecord( 12 );
        after.setLabelField( 18, emptyDynamicRecords );
        final Command.NodeCommand command = new Command.NodeCommand( before, after );

        // when
        boolean result;
        try ( TransactionApplier txApplier = applier.startTx( transactionToApply ) )
        {
            result = txApplier.visitNodeCommand( command );
        }
        // then
        assertFalse( result );
    }

    private IndexBatchTransactionApplier newIndexTransactionApplier()
    {
        PropertyStore propertyStore = mock( PropertyStore.class );
        return new IndexBatchTransactionApplier( indexingService, labelScanStoreSynchronizer, indexUpdatesSync, mock( NodeStore.class ),
                mock( RelationshipStore.class ), new PropertyPhysicalToLogicalConverter( propertyStore ), mock( StorageEngine.class ), schemaCache,
                new IndexActivator( indexingService ) );
    }

    @Test
    public void shouldCreateIndexGivenCreateSchemaRuleCommand() throws Exception
    {
        // Given
        final StoreIndexDescriptor indexRule = indexRule( 1, 42, 42, INDEX_DESCRIPTOR );

        final IndexBatchTransactionApplier applier = newIndexTransactionApplier();

        final Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand( emptyDynamicRecords, singleton( createdDynamicRecord( 1 ) ), indexRule );

        // When
        boolean result;
        try ( TransactionApplier txApplier = applier.startTx( transactionToApply ) )
        {
            result = txApplier.visitSchemaRuleCommand( command );
        }

        // Then
        assertFalse( result );
        verify( indexingService ).createIndexes( indexRule );
    }

    private StoreIndexDescriptor indexRule( long ruleId, int labelId, int propertyId, IndexProviderDescriptor descriptor )
    {
        return IndexDescriptorFactory.forSchema( forLabel( labelId, propertyId ), descriptor ).withId( ruleId );
    }

    @Test
    public void shouldDropIndexGivenDropSchemaRuleCommand() throws Exception
    {
        // Given
        final StoreIndexDescriptor indexRule = indexRule( 1, 42, 42, INDEX_DESCRIPTOR );

        final IndexBatchTransactionApplier applier = newIndexTransactionApplier();

        final Command.SchemaRuleCommand command = new Command.SchemaRuleCommand(
                singleton( createdDynamicRecord( 1 ) ), singleton( dynamicRecord( 1, false ) ), indexRule );

        // When
        boolean result;
        try ( TransactionApplier txApplier = applier.startTx( transactionToApply ) )
        {
            result = txApplier.visitSchemaRuleCommand( command );
        }

        // Then
        assertFalse( result );
        verify( indexingService ).dropIndex( indexRule );
    }

    private static DynamicRecord createdDynamicRecord( long id )
    {
        DynamicRecord record = dynamicRecord( id, true );
        record.setCreated();
        return record;
    }
}
