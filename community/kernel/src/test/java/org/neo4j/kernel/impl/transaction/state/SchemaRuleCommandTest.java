/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Test;

import java.util.Arrays;
import java.util.function.Supplier;

import org.neo4j.concurrent.WorkSync;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.impl.api.BatchTransactionApplier;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.PropertyPhysicalToLogicalConverter;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.AbstractSchemaRule;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.RecordSerializer;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.transaction.command.CommandHandlerContract;
import org.neo4j.kernel.impl.transaction.command.IndexBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.command.IndexUpdatesWork;
import org.neo4j.kernel.impl.transaction.command.LabelUpdateWork;
import org.neo4j.kernel.impl.transaction.command.NeoStoreBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_2;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule.uniquenessConstraintRule;

public class SchemaRuleCommandTest
{

    private final int labelId = 2;
    private final int propertyKey = 8;
    private final long id = 0;
    private final long txId = 1337L;
    private final NeoStores neoStores = mock( NeoStores.class );
    private final MetaDataStore metaDataStore = mock( MetaDataStore.class );
    private final SchemaStore schemaStore = mock( SchemaStore.class );
    private final IndexingService indexes = mock( IndexingService.class );
    @SuppressWarnings( "unchecked" )
    private final Supplier<LabelScanWriter> labelScanStore = mock( Supplier.class );
    private final NeoStoreBatchTransactionApplier storeApplier = new NeoStoreBatchTransactionApplier( neoStores,
            mock( CacheAccessBackDoor.class ), LockService.NO_LOCK_SERVICE );
    private final WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanStoreSynchronizer =
            new WorkSync<>( labelScanStore );
    private final WorkSync<IndexingService,IndexUpdatesWork> indexUpdatesSync = new WorkSync<>( indexes );
    private final PropertyStore propertyStore = mock( PropertyStore.class );
    private final IndexBatchTransactionApplier indexApplier = new IndexBatchTransactionApplier( indexes,
            labelScanStoreSynchronizer, indexUpdatesSync, mock( NodeStore.class ),
            mock( PropertyLoader.class ), new PropertyPhysicalToLogicalConverter( propertyStore ),
            TransactionApplicationMode.INTERNAL );
    private final PhysicalLogCommandReaderV2_2 reader = new PhysicalLogCommandReaderV2_2();
    private final IndexRule rule = IndexRule.indexRule( id, labelId, propertyKey, PROVIDER_DESCRIPTOR );

    @Test
    public void shouldWriteCreatedSchemaRuleToStore() throws Exception
    {
        // GIVEN
        SchemaRecord beforeRecords = serialize( rule, id, false, false);
        SchemaRecord afterRecords = serialize( rule, id, true, true);

        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );

        // WHEN
        visitSchemaRuleCommand( storeApplier, new SchemaRuleCommand( beforeRecords, afterRecords, rule ) );

        // THEN
        verify( schemaStore ).updateRecord( Iterables.first( afterRecords ) );
    }

    @Test
    public void shouldCreateIndexForCreatedSchemaRule() throws Exception
    {
        // GIVEN
        SchemaRecord beforeRecords = serialize( rule, id, false, false);
        SchemaRecord afterRecords = serialize( rule, id, true, true);

        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );

        // WHEN
        visitSchemaRuleCommand( indexApplier, new SchemaRuleCommand( beforeRecords, afterRecords, rule ) );

        // THEN
        verify( indexes ).createIndexes( rule );
    }

    @Test
    public void shouldSetLatestConstraintRule() throws Exception
    {
        // Given
        SchemaRecord beforeRecords = serialize( rule, id, true, true);
        SchemaRecord afterRecords = serialize( rule, id, true, false);

        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );
        when( neoStores.getMetaDataStore() ).thenReturn( metaDataStore );

        UniquePropertyConstraintRule schemaRule = uniquenessConstraintRule( id, labelId, propertyKey, 0 );

        // WHEN
        visitSchemaRuleCommand( storeApplier, new SchemaRuleCommand( beforeRecords, afterRecords, schemaRule ) );

        // THEN
        verify( schemaStore ).updateRecord( Iterables.first( afterRecords ) );
        verify( metaDataStore ).setLatestConstraintIntroducingTx( txId );
    }

    @Test
    public void shouldDropSchemaRuleFromStore() throws Exception
    {
        // GIVEN
        SchemaRecord beforeRecords = serialize( rule, id, true, true);
        SchemaRecord afterRecords = serialize( rule, id, false, false);

        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );

        // WHEN
        visitSchemaRuleCommand( storeApplier, new SchemaRuleCommand( beforeRecords, afterRecords, rule ) );

        // THEN
        verify( schemaStore ).updateRecord( Iterables.first( afterRecords ) );
    }

    @Test
    public void shouldDropSchemaRuleFromIndex() throws Exception
    {
        // GIVEN
        SchemaRecord beforeRecords = serialize( rule, id, true, true);
        SchemaRecord afterRecords = serialize( rule, id, false, false);

        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );

        // WHEN
        visitSchemaRuleCommand( indexApplier, new SchemaRuleCommand( beforeRecords, afterRecords, rule ) );

        // THEN
        verify( indexes ).dropIndex( rule );
    }

    @Test
    public void shouldWriteSchemaRuleToLog() throws Exception
    {
        // GIVEN
        SchemaRecord beforeRecords = serialize( rule, id, false, false);
        SchemaRecord afterRecords = serialize( rule, id, true, true);

        SchemaRuleCommand command = new SchemaRuleCommand( beforeRecords, afterRecords, rule );
        InMemoryClosableChannel buffer = new InMemoryClosableChannel();

        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );

        // WHEN
        command.serialize( buffer );
        Command readCommand = reader.read( buffer );

        // THEN
        assertThat( readCommand, instanceOf( SchemaRuleCommand.class ) );

        assertSchemaRule( (SchemaRuleCommand)readCommand );
    }

    @Test
    public void shouldRecreateSchemaRuleWhenDeleteCommandReadFromDisk() throws Exception
    {
        // GIVEN
        SchemaRecord beforeRecords = serialize( rule, id, true, true);
        SchemaRecord afterRecords = serialize( rule, id, false, false);

        SchemaRuleCommand command = new SchemaRuleCommand( beforeRecords, afterRecords, rule );
        InMemoryClosableChannel buffer = new InMemoryClosableChannel();
        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );

        // WHEN
        command.serialize( buffer );
        Command readCommand = reader.read( buffer );

        // THEN
        assertThat( readCommand, instanceOf( SchemaRuleCommand.class ) );

        assertSchemaRule( (SchemaRuleCommand)readCommand );
    }

    private SchemaRecord serialize( AbstractSchemaRule rule, long id, boolean inUse, boolean created )
    {
        RecordSerializer serializer = new RecordSerializer();
        serializer = serializer.append( rule );
        DynamicRecord record = new DynamicRecord( id );
        record.setData( serializer.serialize() );
        if ( created )
        {
            record.setCreated();
        }
        if ( inUse )
        {
            record.setInUse( true );
        }
        return new SchemaRecord( Arrays.asList( record ) );
    }

    private void assertSchemaRule( SchemaRuleCommand readSchemaCommand )
    {
        assertEquals( id, readSchemaCommand.getKey() );
        assertEquals( labelId, readSchemaCommand.getSchemaRule().getLabel() );
        assertEquals( propertyKey, ((IndexRule)readSchemaCommand.getSchemaRule()).getPropertyKey() );
    }

    private void visitSchemaRuleCommand( BatchTransactionApplier applier, SchemaRuleCommand command ) throws Exception
    {
        TransactionToApply tx = new TransactionToApply(
                new PhysicalTransactionRepresentation( Arrays.<StorageCommand>asList( command ) ), txId );
        CommandHandlerContract.apply( applier, tx );
    }
}
