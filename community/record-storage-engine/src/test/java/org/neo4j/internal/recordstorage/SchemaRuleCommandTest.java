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

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorPredicates;
import org.neo4j.internal.recordstorage.Command.SchemaRuleCommand;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.index.schema.StoreIndexDescriptor;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.store.record.SchemaRuleSerialization;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.NodeLabelUpdateListener;
import org.neo4j.storageengine.api.SchemaRule;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.util.concurrent.WorkSync;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    private final IndexUpdateListener indexUpdateListener = mock( IndexUpdateListener.class );
    private final SchemaCache schemaCache = mock( SchemaCache.class );
    private final StorageEngine storageEngine = mock( StorageEngine.class );
    @SuppressWarnings( "unchecked" )
    private final NodeLabelUpdateListener labelUpdateListener = mock( NodeLabelUpdateListener.class );
    private final NeoStoreBatchTransactionApplier storeApplier = new NeoStoreBatchTransactionApplier( neoStores,
            mock( CacheAccessBackDoor.class ), LockService.NO_LOCK_SERVICE );
    private final WorkSync<NodeLabelUpdateListener,LabelUpdateWork> labelScanStoreSynchronizer = new WorkSync<>( labelUpdateListener );
    private final WorkSync<IndexUpdateListener,IndexUpdatesWork> indexUpdatesSync = new WorkSync<>( indexUpdateListener );
    private final PropertyStore propertyStore = mock( PropertyStore.class );
    private final IndexBatchTransactionApplier indexApplier =
            new IndexBatchTransactionApplier( indexUpdateListener, labelScanStoreSynchronizer, indexUpdatesSync, mock( NodeStore.class ),
                    neoStores.getRelationshipStore(), new PropertyPhysicalToLogicalConverter( propertyStore ), storageEngine, schemaCache,
                    new IndexActivator( indexes ) );
    private final BaseCommandReader reader = new PhysicalLogCommandReaderV3_0_2();
    private final StoreIndexDescriptor rule = TestIndexDescriptorFactory.forLabel( labelId, propertyKey ).withId( id );

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
        verify( indexUpdateListener ).createIndexes( rule );
    }

    @Test
    public void shouldSetLatestConstraintRule() throws Exception
    {
        // Given
        SchemaRecord beforeRecords = serialize( rule, id, true, true);
        SchemaRecord afterRecords = serialize( rule, id, true, false);

        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );
        when( neoStores.getMetaDataStore() ).thenReturn( metaDataStore );

        ConstraintRule schemaRule = ConstraintRule.constraintRule( id,
                ConstraintDescriptorFactory.uniqueForLabel( labelId, propertyKey ), 0 );

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
        verify( indexUpdateListener ).dropIndex( rule );
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

    private SchemaRecord serialize( SchemaRule rule, long id, boolean inUse, boolean created )
    {
        DynamicRecord record = new DynamicRecord( id );
        record.setData( SchemaRuleSerialization.serialize( rule ) );
        if ( created )
        {
            record.setCreated();
        }
        if ( inUse )
        {
            record.setInUse( true );
        }
        return new SchemaRecord( singletonList( record ) );
    }

    private void assertSchemaRule( SchemaRuleCommand readSchemaCommand )
    {
        assertEquals( id, readSchemaCommand.getKey() );
        assertTrue( SchemaDescriptorPredicates.hasLabel( readSchemaCommand.getSchemaRule(), labelId ) );
        assertTrue( SchemaDescriptorPredicates.hasProperty( readSchemaCommand.getSchemaRule(), propertyKey ) );
    }

    private void visitSchemaRuleCommand( BatchTransactionApplier applier, SchemaRuleCommand command ) throws Exception
    {
        TransactionToApply tx = new TransactionToApply(
                new PhysicalTransactionRepresentation( singletonList( command ) ), txId );
        CommandHandlerContract.apply( applier, tx );
    }
}
