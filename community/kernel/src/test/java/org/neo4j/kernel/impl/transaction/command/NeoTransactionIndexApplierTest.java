/*
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
package org.neo4j.kernel.impl.transaction.command;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

import org.neo4j.concurrent.WorkSync;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.SchemaIndexProvider.Descriptor;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static java.util.Collections.singleton;

import static org.neo4j.kernel.impl.store.record.DynamicRecord.dynamicRecord;
import static org.neo4j.kernel.impl.store.record.IndexRule.indexRule;

public class NeoTransactionIndexApplierTest
{
    private final static Descriptor INDEX_DESCRIPTOR = new Descriptor( "in-memory", "1.0" );

    private final IndexingService indexingService = mock( IndexingService.class );
    @SuppressWarnings( "unchecked" )
    private final Supplier<LabelScanWriter> labelScanStore = mock( Supplier.class );
    private final Collection<DynamicRecord> emptyDynamicRecords = Collections.emptySet();
    private final WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanStoreSynchronizer = new WorkSync<>(
            labelScanStore );
    private final TransactionToApply transactionToApply = mock( TransactionToApply.class );

    @Before
    public void setup()
    {
        when( transactionToApply.transactionId() ).thenReturn( 1L );
        when( transactionToApply.validatedIndexUpdates() ).thenReturn( mock( ValidatedIndexUpdates.class ) );
    }

    @Test
    public void shouldUpdateLabelStoreScanOnNodeCommands() throws Exception
    {
        // given
        final IndexBatchTransactionApplier applier = new IndexBatchTransactionApplier( indexingService,
                labelScanStoreSynchronizer );

        final NodeRecord before = new NodeRecord( 11 );
        before.setLabelField( 17, emptyDynamicRecords );
        final NodeRecord after = new NodeRecord( 12 );
        after.setLabelField( 18, emptyDynamicRecords );
        final Command.NodeCommand command = new Command.NodeCommand().init( before, after );

        when( labelScanStore.get() ).thenReturn( mock( LabelScanWriter.class ) );

        // when
        boolean result;
        try ( TransactionApplier txApplier = applier.startTx( transactionToApply ) )
        {
            result = txApplier.visitNodeCommand( command );
        }
        // then
        assertFalse( result );
    }

    @Test
    public void shouldCreateIndexGivenCreateSchemaRuleCommand() throws Exception
    {
        // Given
        final IndexRule indexRule = indexRule( 1, 42, 42, INDEX_DESCRIPTOR );

        final IndexBatchTransactionApplier applier = new IndexBatchTransactionApplier( indexingService,
                labelScanStoreSynchronizer );

        final Command.SchemaRuleCommand command = new Command.SchemaRuleCommand();
        command.init( emptyDynamicRecords, singleton( createdDynamicRecord( 1 ) ), indexRule );

        // When
        boolean result;
        try ( TransactionApplier txApplier = applier.startTx( transactionToApply ) )
        {
            result = txApplier.visitSchemaRuleCommand( command );
        }

        // Then
        assertFalse( result );
        verify( indexingService ).createIndex( indexRule );
    }

    @Test
    public void shouldDropIndexGivenDropSchemaRuleCommand() throws Exception
    {
        // Given
        final IndexRule indexRule = indexRule( 1, 42, 42, INDEX_DESCRIPTOR );

        final IndexBatchTransactionApplier applier = new IndexBatchTransactionApplier( indexingService,
                labelScanStoreSynchronizer );

        final Command.SchemaRuleCommand command = new Command.SchemaRuleCommand();
        command.init( singleton( createdDynamicRecord( 1 ) ), singleton( dynamicRecord( 1, false ) ), indexRule );

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
