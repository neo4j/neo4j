/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;

import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.EntityTokenUpdateListener;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.util.concurrent.WorkSync;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.common.Subject.SYSTEM;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;

class NeoTransactionIndexApplierTest
{
    private final IndexUpdateListener indexingService = mock( IndexUpdateListener.class );
    private final IndexUpdateListener indexUpdateListener = mock( IndexUpdateListener.class );
    private final SchemaCache schemaCache = mock( SchemaCache.class );
    private final EntityTokenUpdateListener labelUpdateListener = mock( EntityTokenUpdateListener.class );
    private final Collection<DynamicRecord> emptyDynamicRecords = Collections.emptySet();
    private final WorkSync<EntityTokenUpdateListener,TokenUpdateWork> labelScanStoreSynchronizer = new WorkSync<>( labelUpdateListener );
    private final WorkSync<IndexUpdateListener,IndexUpdatesWork> indexUpdatesSync = new WorkSync<>( indexUpdateListener );
    private final CommandsToApply transactionToApply = new GroupOfCommands( 1L );
    private final BatchContext batchContext = mock( BatchContext.class, RETURNS_MOCKS );

    @Test
    void shouldUpdateLabelStoreScanOnNodeCommands() throws Exception
    {
        // given
        IndexTransactionApplierFactory applier = newIndexTransactionApplier();
        NodeRecord before = new NodeRecord( 11 );
        before.setLabelField( 17, emptyDynamicRecords );
        NodeRecord after = new NodeRecord( 12 );
        after.setLabelField( 18, emptyDynamicRecords );
        Command.NodeCommand command = new Command.NodeCommand( before, after );

        // when
        boolean result;
        try ( TransactionApplier txApplier = applier.startTx( transactionToApply, batchContext ) )
        {
            result = txApplier.visitNodeCommand( command );
        }
        // then
        assertFalse( result );
    }

    private IndexTransactionApplierFactory newIndexTransactionApplier()
    {
        return new IndexTransactionApplierFactory( indexingService );
    }

    @Test
    void shouldCreateIndexGivenCreateSchemaRuleCommand() throws Exception
    {
        // Given
        IndexDescriptor indexRule = indexRule( 1, 42, 42 );

        IndexTransactionApplierFactory applier = newIndexTransactionApplier();

        SchemaRecord before = new SchemaRecord( 1 );
        SchemaRecord after = before.copy().initialize( true, 39 );
        after.setCreated();
        Command.SchemaRuleCommand command = new Command.SchemaRuleCommand( before, after, indexRule );

        // When
        boolean result;
        try ( TransactionApplier txApplier = applier.startTx( transactionToApply, batchContext ) )
        {
            result = txApplier.visitSchemaRuleCommand( command );
        }

        // Then
        assertFalse( result );
        verify( indexingService ).createIndexes( SYSTEM , indexRule );
    }

    private IndexDescriptor indexRule( long ruleId, int labelId, int propertyId )
    {
        return IndexPrototype.forSchema( forLabel( labelId, propertyId ) ).withName( "index_" + ruleId ).materialise( ruleId );
    }

    @Test
    void shouldDropIndexGivenDropSchemaRuleCommand() throws Exception
    {
        // Given
        IndexDescriptor indexRule = indexRule( 1, 42, 42 );

        IndexTransactionApplierFactory applier = newIndexTransactionApplier();

        SchemaRecord before = new SchemaRecord( 1 ).initialize( true, 39 );
        SchemaRecord after = new SchemaRecord( 1 );
        Command.SchemaRuleCommand command = new Command.SchemaRuleCommand( before, after, indexRule );

        // When
        boolean result;
        try ( TransactionApplier txApplier = applier.startTx( transactionToApply, batchContext ) )
        {
            result = txApplier.visitSchemaRuleCommand( command );
        }

        // Then
        assertFalse( result );
        verify( indexingService ).dropIndex( indexRule );
    }
}
