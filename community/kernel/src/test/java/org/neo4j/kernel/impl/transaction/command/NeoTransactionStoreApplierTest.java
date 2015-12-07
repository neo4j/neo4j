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
import org.mockito.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

import org.neo4j.concurrent.WorkSync;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.CommandApplierFacade;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;
import org.neo4j.kernel.impl.transaction.command.Command.LabelTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyKeyTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipTypeTokenCommand;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.transaction.command.CommandHandlerContract.apply;

public class NeoTransactionStoreApplierTest
{
    private final NeoStores neoStores = mock( NeoStores.class );
    private final IndexingService indexingService = mock( IndexingService.class );
    @SuppressWarnings( "unchecked" )
    private final Supplier<LabelScanWriter> labelScanStore = mock( Supplier.class );
    private final CacheAccessBackDoor cacheAccess = mock( CacheAccessBackDoor.class );
    private final LockService lockService = mock( LockService.class );

    private final MetaDataStore metaDataStore = mock( MetaDataStore.class );
    private final NodeStore nodeStore = mock( NodeStore.class );
    private final RelationshipStore relationshipStore = mock( RelationshipStore.class );
    private final PropertyStore propertyStore = mock( PropertyStore.class );
    private final RelationshipGroupStore relationshipGroupStore = mock( RelationshipGroupStore.class );
    private final RelationshipTypeTokenStore relationshipTypeTokenStore = mock( RelationshipTypeTokenStore.class );
    private final LabelTokenStore labelTokenStore = mock( LabelTokenStore.class );
    private final PropertyKeyTokenStore propertyKeyTokenStore = mock( PropertyKeyTokenStore.class );
    private final SchemaStore schemaStore = mock( SchemaStore.class );
    private final DynamicArrayStore dynamicLabelStore = mock( DynamicArrayStore.class );

    private final long transactionId = 55555;
    private final DynamicRecord one = DynamicRecord.dynamicRecord( 1, true );
    private final DynamicRecord two = DynamicRecord.dynamicRecord( 2, true );
    private final DynamicRecord three = DynamicRecord.dynamicRecord( 3, true );
    private final WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork>
            labelScanStoreSynchronizer = new WorkSync<>( labelScanStore );
    private final TransactionToApply transactionToApply = mock( TransactionToApply.class );

    @Before
    public void setup()
    {
        when( neoStores.getMetaDataStore() ).thenReturn( metaDataStore );
        when( neoStores.getNodeStore() ).thenReturn( nodeStore );
        when( neoStores.getRelationshipStore() ).thenReturn( relationshipStore );
        when( neoStores.getPropertyStore() ).thenReturn( propertyStore );
        when( neoStores.getRelationshipGroupStore() ).thenReturn( relationshipGroupStore );
        when( neoStores.getRelationshipTypeTokenStore() ).thenReturn( relationshipTypeTokenStore );
        when( neoStores.getLabelTokenStore() ).thenReturn( labelTokenStore );
        when( neoStores.getPropertyKeyTokenStore() ).thenReturn( propertyKeyTokenStore );
        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );
        when( nodeStore.getDynamicLabelStore() ).thenReturn( dynamicLabelStore );
        when( lockService.acquireNodeLock( anyLong(), Matchers.<LockService.LockType>any() ) )
                .thenReturn( LockService.NO_LOCK );
        when( transactionToApply.transactionId() ).thenReturn( transactionId );
        when( transactionToApply.validatedIndexUpdates() ).thenReturn( mock( ValidatedIndexUpdates.class ) );
    }

    // NODE COMMAND

    @Test
    public void shouldApplyNodeCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final NodeRecord before = new NodeRecord( 11 );
        before.setLabelField( 42, Arrays.asList( one, two ) );
        final NodeRecord after = new NodeRecord( 12 );
        after.setInUse( true );
        after.setLabelField( 42, Arrays.asList( one, two, three ) );
        final Command.NodeCommand command = new Command.NodeCommand().init( before, after );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitNodeCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( lockService, times( 1 ) ).acquireNodeLock( command.getKey(), LockService.LockType.WRITE_LOCK );
        verify( nodeStore, times( 1 ) ).updateRecord( after );
    }

    @Test
    public void shouldApplyNodeCommandToTheStoreAndInvalidateTheCache() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final NodeRecord before = new NodeRecord( 11 );
        before.setLabelField( 42, Arrays.asList( one, two ) );
        final NodeRecord after = new NodeRecord( 12 );
        after.setInUse( false );
        after.setLabelField( 42, Arrays.asList( one, two, three ) );
        final Command.NodeCommand command = new Command.NodeCommand().init( before, after );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitNodeCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( lockService, times( 1 ) ).acquireNodeLock( command.getKey(), LockService.LockType.WRITE_LOCK );
        verify( nodeStore, times( 1 ) ).updateRecord( after );
    }

    @Test
    public void shouldApplyNodeCommandToTheStoreInRecoveryMode() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        final NodeRecord before = new NodeRecord( 11 );
        before.setLabelField( 42, Arrays.asList( one, two ) );
        final NodeRecord after = new NodeRecord( 12 );
        after.setInUse( true );
        after.setLabelField( 42, Arrays.asList( one, two, three ) );
        final Command.NodeCommand command = new Command.NodeCommand().init( before, after );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitNodeCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( lockService, times( 1 ) ).acquireNodeLock( command.getKey(), LockService.LockType.WRITE_LOCK );
        verify( nodeStore, times( 1 ) ).setHighestPossibleIdInUse( after.getId() );
        verify( nodeStore, times( 1 ) ).updateRecord( after );
        verify( dynamicLabelStore, times( 1 ) ).setHighestPossibleIdInUse( three.getId() );
    }

    @Test
    public void shouldInvalidateTheCacheWhenTheNodeBecomesDense() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final NodeRecord before = new NodeRecord( 11 );
        before.setLabelField( 42, Arrays.asList( one ) );
        before.setInUse( true );
        before.setDense( false );
        final NodeRecord after = new NodeRecord( 12 );
        after.setInUse( true );
        after.setDense( true );
        after.setLabelField( 42, Arrays.asList( one, two, three ) );
        final Command.NodeCommand command = new Command.NodeCommand().init( before, after );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitNodeCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( lockService, times( 1 ) ).acquireNodeLock( command.getKey(), LockService.LockType.WRITE_LOCK );
        verify( nodeStore, times( 1 ) ).updateRecord( after );
    }

    // RELATIONSHIP COMMAND

    @Test
    public void shouldApplyRelationshipCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final RelationshipRecord before = new RelationshipRecord( 12 );
        final RelationshipRecord record = new RelationshipRecord( 12, 3, 4, 5 );
        record.setInUse( true );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitRelationshipCommand( new Command.RelationshipCommand().init( before, record ) );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( relationshipStore, times( 1 ) ).updateRecord( record );
    }

    @Test
    public void shouldApplyRelationshipCommandToTheStoreAndInvalidateTheCache() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final RelationshipRecord before = new RelationshipRecord( 12 );
        final RelationshipRecord record = new RelationshipRecord( 12, 3, 4, 5 );
        record.setInUse( false );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitRelationshipCommand( new Command.RelationshipCommand().init( before, record ) );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( relationshipStore, times( 1 ) ).updateRecord( record );
    }

    @Test
    public void shouldApplyRelationshipCommandToTheStoreInRecovery() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        final RelationshipRecord before = new RelationshipRecord( 12 );
        final RelationshipRecord record = new RelationshipRecord( 12, 3, 4, 5 );
        record.setInUse( true );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitRelationshipCommand( new Command.RelationshipCommand().init( before, record ) );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( relationshipStore, times( 1 ) ).setHighestPossibleIdInUse( record.getId() );
        verify( relationshipStore, times( 1 ) ).updateRecord( record );
    }

    // PROPERTY COMMAND

    @Test
    public void shouldApplyNodePropertyCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final PropertyRecord before = new PropertyRecord( 11 );
        final PropertyRecord after = new PropertyRecord( 12 );
        after.setNodeId( 42 );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitPropertyCommand( new Command.PropertyCommand().init( before, after ) );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( lockService, times( 1 ) ).acquireNodeLock( 42, LockService.LockType.WRITE_LOCK );
        verify( propertyStore, times( 1 ) ).updateRecord( after );
    }

    @Test
    public void shouldApplyNodePropertyCommandToTheStoreInRecovery() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        final PropertyRecord before = new PropertyRecord( 11 );
        final PropertyRecord after = new PropertyRecord( 12 );
        after.setNodeId( 42 );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitPropertyCommand( new Command.PropertyCommand().init( before, after ) );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( lockService, times( 1 ) ).acquireNodeLock( 42, LockService.LockType.WRITE_LOCK );
        verify( propertyStore, times( 1 ) ).setHighestPossibleIdInUse( after.getId() );
        verify( propertyStore, times( 1 ) ).updateRecord( after );
    }

    @Test
    public void shouldApplyRelPropertyCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final PropertyRecord before = new PropertyRecord( 11 );
        final PropertyRecord after = new PropertyRecord( 12 );
        after.setRelId( 42 );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitPropertyCommand( new Command.PropertyCommand().init( before, after ) );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( propertyStore, times( 1 ) ).updateRecord( after );
    }

    @Test
    public void shouldApplyRelPropertyCommandToTheStoreInRecovery() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        final PropertyRecord before = new PropertyRecord( 11 );
        final PropertyRecord after = new PropertyRecord( 12 );
        after.setRelId( 42 );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitPropertyCommand( new Command.PropertyCommand().init( before, after ) );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( propertyStore, times( 1 ) ).setHighestPossibleIdInUse( 12 );
        verify( propertyStore, times( 1 ) ).updateRecord( after );
    }

    // RELATIONSHIP GROUP COMMAND

    @Test
    public void shouldApplyRelationshipGroupCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        // when
        final RelationshipGroupRecord before = new RelationshipGroupRecord( 42, 1 );
        final RelationshipGroupRecord after = new RelationshipGroupRecord( 42, 1, 2, 3, 4, 5, 6, true );
        final boolean result = apply( applier, (handler,tx) -> {
            return handler.visitRelationshipGroupCommand(
                    new Command.RelationshipGroupCommand().init( before, after ) );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( relationshipGroupStore, times( 1 ) ).updateRecord( after );
    }

    @Test
    public void shouldApplyRelationshipGroupCommandToTheStoreInRecovery() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        // when
        final RelationshipGroupRecord before = new RelationshipGroupRecord( 42, 1 );
        final RelationshipGroupRecord after = new RelationshipGroupRecord( 42, 1, 2, 3, 4, 5, 6, true );

        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitRelationshipGroupCommand(
                    new Command.RelationshipGroupCommand().init( before, after ) );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( relationshipGroupStore, times( 1 ) ).setHighestPossibleIdInUse( after.getId() );
        verify( relationshipGroupStore, times( 1 ) ).updateRecord( after );
    }

    // RELATIONSHIP TYPE TOKEN COMMAND

    @Test
    public void shouldApplyRelationshipTypeTokenCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final RelationshipTypeTokenRecord record = new RelationshipTypeTokenRecord( 42 );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitRelationshipTypeTokenCommand(
                    (RelationshipTypeTokenCommand) new Command.RelationshipTypeTokenCommand().init( record ) );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( relationshipTypeTokenStore, times( 1 ) ).updateRecord( record );
    }

    @Test
    public void shouldApplyRelationshipTypeTokenCommandToTheStoreInRecovery() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        final RelationshipTypeTokenRecord record = new RelationshipTypeTokenRecord( 42 );
        final Command.RelationshipTypeTokenCommand command =
                (RelationshipTypeTokenCommand) new Command.RelationshipTypeTokenCommand().init( record );
        final RelationshipTypeToken token = new RelationshipTypeToken( "token", 21 );
        when( relationshipTypeTokenStore.getToken( (int) command.getKey() ) ).thenReturn( token );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitRelationshipTypeTokenCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( relationshipTypeTokenStore, times( 1 ) ).setHighestPossibleIdInUse( record.getId() );
        verify( relationshipTypeTokenStore, times( 1 ) ).updateRecord( record );
        verify( cacheAccess, times( 1 ) ).addRelationshipTypeToken( token );
    }

    // LABEL TOKEN COMMAND

    @Test
    public void shouldApplyLabelTokenCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final LabelTokenRecord record = new LabelTokenRecord( 42 );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitLabelTokenCommand(
                    (LabelTokenCommand) new Command.LabelTokenCommand().init( record ) );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( labelTokenStore, times( 1 ) ).updateRecord( record );
    }

    @Test
    public void shouldApplyLabelTokenCommandToTheStoreInRecovery() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        final LabelTokenRecord record = new LabelTokenRecord( 42 );
        final Command.LabelTokenCommand command = (LabelTokenCommand) new Command.LabelTokenCommand().init( record );
        final Token token = new Token( "token", 21 );
        when( labelTokenStore.getToken( (int) command.getKey() ) ).thenReturn( token );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitLabelTokenCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( labelTokenStore, times( 1 ) ).setHighestPossibleIdInUse( record.getId() );
        verify( labelTokenStore, times( 1 ) ).updateRecord( record );
        verify( cacheAccess, times( 1 ) ).addLabelToken( token );
    }

    // PROPERTY KEY TOKEN COMMAND

    @Test
    public void shouldApplyPropertyKeyTokenCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( 42 );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitPropertyKeyTokenCommand(
                    (PropertyKeyTokenCommand) new Command.PropertyKeyTokenCommand().init( record ) );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( propertyKeyTokenStore, times( 1 ) ).updateRecord( record );
    }

    @Test
    public void shouldApplyPropertyKeyTokenCommandToTheStoreInRecovery() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        final PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( 42 );
        final Command.PropertyKeyTokenCommand command =
                (PropertyKeyTokenCommand) new Command.PropertyKeyTokenCommand().init( record );
        final Token token = new Token( "token", 21 );
        when( propertyKeyTokenStore.getToken( (int) command.getKey() ) ).thenReturn( token );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitPropertyKeyTokenCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( propertyKeyTokenStore, times( 1 ) ).setHighestPossibleIdInUse( record.getId() );
        verify( propertyKeyTokenStore, times( 1 ) ).updateRecord( record );
        verify( cacheAccess, times( 1 ) ).addPropertyKeyToken( token );
    }

    // SCHEMA RULE COMMAND

    @Test
    public void shouldApplyCreateIndexRuleSchemaRuleCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final CommandHandler indexApplier = new IndexTransactionApplier( indexingService,
                labelScanStoreSynchronizer );
        final DynamicRecord record = DynamicRecord.dynamicRecord( 21, true );
        record.setCreated();
        final Collection<DynamicRecord> recordsAfter = Arrays.asList( record );
        final IndexRule rule = IndexRule.indexRule( 0, 1, 2, new SchemaIndexProvider.Descriptor( "K", "X.Y" ) );
        final Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand().init( Collections.<DynamicRecord>emptyList(), recordsAfter, rule );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitSchemaRuleCommand( command ) & indexApplier.visitSchemaRuleCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( schemaStore, times( 1 ) ).updateRecord( record );
        verify( indexingService, times( 1 ) ).createIndex( rule );
        verify( cacheAccess, times( 1 ) ).addSchemaRule( rule );
    }

    @Test
    public void shouldApplyCreateIndexRuleSchemaRuleCommandToTheStoreInRecovery() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        final CommandHandler indexApplier = newIndexApplier( TransactionApplicationMode.EXTERNAL );
        final DynamicRecord record = DynamicRecord.dynamicRecord( 21, true );
        record.setCreated();
        final Collection<DynamicRecord> recordsAfter = Arrays.asList( record );
        final IndexRule rule = IndexRule.indexRule( 0, 1, 2, new SchemaIndexProvider.Descriptor( "K", "X.Y" ) );
        final Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand().init( Collections.<DynamicRecord>emptyList(), recordsAfter, rule );

        // when
        CommandHandler all = new CommandApplierFacade( applier, indexApplier );
        boolean result = apply( all, (handler,tx) -> {
            return all.visitSchemaRuleCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( schemaStore, times( 1 ) ).setHighestPossibleIdInUse( record.getId() );
        verify( schemaStore, times( 1 ) ).updateRecord( record );
        verify( indexingService, times( 1 ) ).createIndex( rule );
        verify( cacheAccess, times( 1 ) ).addSchemaRule( rule );
    }

    @Test
    public void shouldApplyUpdateIndexRuleSchemaRuleCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final CommandHandler indexApplier = newIndexApplier( TransactionApplicationMode.INTERNAL );
        final DynamicRecord record = DynamicRecord.dynamicRecord( 21, true );
        final Collection<DynamicRecord> recordsAfter = Arrays.asList( record );
        final IndexRule rule =
                IndexRule.constraintIndexRule( 0, 1, 2, new SchemaIndexProvider.Descriptor( "K", "X.Y" ), 42l );
        final Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand().init( Collections.<DynamicRecord>emptyList(), recordsAfter, rule );

        // when
        CommandHandler all = new CommandApplierFacade( applier, indexApplier );
        boolean result = apply( all, (handler,tx) -> {
            return handler.visitSchemaRuleCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( schemaStore, times( 1 ) ).updateRecord( record );
        verify( indexingService, times( 1 ) ).activateIndex( rule.getId() );
        verify( cacheAccess, times( 1 ) ).addSchemaRule( rule );
    }

    @Test
    public void shouldApplyUpdateIndexRuleSchemaRuleCommandToTheStoreInRecovery() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        final CommandHandler indexApplier = newIndexApplier( TransactionApplicationMode.EXTERNAL );
        final DynamicRecord record = DynamicRecord.dynamicRecord( 21, true );
        final Collection<DynamicRecord> recordsAfter = Arrays.asList( record );
        final IndexRule rule =
                IndexRule.constraintIndexRule( 0, 1, 2, new SchemaIndexProvider.Descriptor( "K", "X.Y" ), 42l );
        final Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand().init( Collections.<DynamicRecord>emptyList(), recordsAfter, rule );

        // when
        CommandHandler all = new CommandApplierFacade( applier, indexApplier );
        boolean result = apply( all, (handler,tx) -> {
            return handler.visitSchemaRuleCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( schemaStore, times( 1 ) ).setHighestPossibleIdInUse( record.getId() );
        verify( schemaStore, times( 1 ) ).updateRecord( record );
        verify( indexingService, times( 1 ) ).activateIndex( rule.getId() );
        verify( cacheAccess, times( 1 ) ).addSchemaRule( rule );
    }

    @Test
    public void shouldApplyUpdateIndexRuleSchemaRuleCommandToTheStoreThrowingIndexProblem()
            throws IOException, IndexNotFoundKernelException,
            IndexPopulationFailedKernelException, IndexActivationFailedKernelException
    {
        // given
        final CommandHandler applier = newIndexApplier( TransactionApplicationMode.INTERNAL );
        doThrow( new IndexNotFoundKernelException( "" ) ).when( indexingService ).activateIndex( anyLong() );

        final DynamicRecord record = DynamicRecord.dynamicRecord( 21, true );
        final Collection<DynamicRecord> recordsAfter = Arrays.asList( record );
        final IndexRule rule =
                IndexRule.constraintIndexRule( 0, 1, 2, new SchemaIndexProvider.Descriptor( "K", "X.Y" ), 42l );
        final Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand().init( Collections.<DynamicRecord>emptyList(), recordsAfter, rule );

        // when
        try
        {
            apply( applier, (handler,tx) -> {
                return handler.visitSchemaRuleCommand( command );
            }, transactionToApply );
            fail( "should have thrown" );
        }
        catch ( Exception e )
        {
            // then
            assertTrue( e.getCause() instanceof IndexNotFoundKernelException );
        }
    }

    @Test
    public void shouldApplyDeleteIndexRuleSchemaRuleCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final CommandHandler indexApplier = newIndexApplier( TransactionApplicationMode.INTERNAL );
        final DynamicRecord record = DynamicRecord.dynamicRecord( 21, true );
        record.setInUse( false );
        final Collection<DynamicRecord> recordsAfter = Arrays.asList( record );
        final IndexRule rule = IndexRule.indexRule( 0, 1, 2, new SchemaIndexProvider.Descriptor( "K", "X.Y" ) );
        final Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand().init( Collections.<DynamicRecord>emptyList(), recordsAfter, rule );

        // when
        CommandHandler all = new CommandApplierFacade( applier, indexApplier );
        boolean result = apply( all, (handler,tx) -> {
            return handler.visitSchemaRuleCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( schemaStore, times( 1 ) ).updateRecord( record );
        verify( indexingService, times( 1 ) ).dropIndex( rule );
        verify( cacheAccess, times( 1 ) ).removeSchemaRuleFromCache( command.getKey() );
    }

    @Test
    public void shouldApplyDeleteIndexRuleSchemaRuleCommandToTheStoreInRecovery() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        final CommandHandler indexApplier = newIndexApplier( TransactionApplicationMode.RECOVERY );
        final DynamicRecord record = DynamicRecord.dynamicRecord( 21, true );
        record.setInUse( false );
        final Collection<DynamicRecord> recordsAfter = Arrays.asList( record );
        final IndexRule rule = IndexRule.indexRule( 0, 1, 2, new SchemaIndexProvider.Descriptor( "K", "X.Y" ) );
        final Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand().init( Collections.<DynamicRecord>emptyList(), recordsAfter, rule );

        // when
        CommandHandler all = new CommandApplierFacade( applier, indexApplier );
        boolean result = apply( all, (handler,tx) -> {
            return handler.visitSchemaRuleCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( schemaStore, times( 1 ) ).setHighestPossibleIdInUse( record.getId() );
        verify( schemaStore, times( 1 ) ).updateRecord( record );
        verify( indexingService, times( 1 ) ).dropIndex( rule );
        verify( cacheAccess, times( 1 ) ).removeSchemaRuleFromCache( command.getKey() );
    }

    @Test
    public void shouldApplyCreateUniquenessConstraintRuleSchemaRuleCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final DynamicRecord record = DynamicRecord.dynamicRecord( 21, true );
        record.setCreated();
        final Collection<DynamicRecord> recordsAfter = Arrays.asList( record );
        final UniquePropertyConstraintRule
                rule = UniquePropertyConstraintRule.uniquenessConstraintRule( 0l, 1, 2, 3l );
        final Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand().init( Collections.<DynamicRecord>emptyList(), recordsAfter, rule );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitSchemaRuleCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( schemaStore, times( 1 ) ).updateRecord( record );
        verify( metaDataStore, times( 1 ) ).setLatestConstraintIntroducingTx( transactionId );
        verify( cacheAccess, times( 1 ) ).addSchemaRule( rule );
    }

    @Test
    public void shouldApplyCreateUniquenessConstraintRuleSchemaRuleCommandToTheStoreInRecovery() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        final DynamicRecord record = DynamicRecord.dynamicRecord( 21, true );
        record.setCreated();
        final Collection<DynamicRecord> recordsAfter = Arrays.asList( record );
        final UniquePropertyConstraintRule
                rule = UniquePropertyConstraintRule.uniquenessConstraintRule( 0l, 1, 2, 3l );
        final Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand().init( Collections.<DynamicRecord>emptyList(), recordsAfter, rule );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitSchemaRuleCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( schemaStore, times( 1 ) ).setHighestPossibleIdInUse( record.getId() );
        verify( schemaStore, times( 1 ) ).updateRecord( record );
        verify( metaDataStore, times( 1 ) ).setLatestConstraintIntroducingTx( transactionId );
        verify( cacheAccess, times( 1 ) ).addSchemaRule( rule );
    }

    @Test
    public void shouldApplyUpdateUniquenessConstraintRuleSchemaRuleCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final DynamicRecord record = DynamicRecord.dynamicRecord( 21, true );
        final Collection<DynamicRecord> recordsAfter = Arrays.asList( record );
        final UniquePropertyConstraintRule
                rule = UniquePropertyConstraintRule.uniquenessConstraintRule( 0l, 1, 2, 3l );
        final Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand().init( Collections.<DynamicRecord>emptyList(), recordsAfter, rule );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitSchemaRuleCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( schemaStore, times( 1 ) ).updateRecord( record );
        verify( metaDataStore, times( 1 ) ).setLatestConstraintIntroducingTx( transactionId );
        verify( cacheAccess, times( 1 ) ).addSchemaRule( rule );
    }

    @Test
    public void shouldApplyUpdateUniquenessConstraintRuleSchemaRuleCommandToTheStoreInRecovery() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        final DynamicRecord record = DynamicRecord.dynamicRecord( 21, true );
        final Collection<DynamicRecord> recordsAfter = Arrays.asList( record );
        final UniquePropertyConstraintRule
                rule = UniquePropertyConstraintRule.uniquenessConstraintRule( 0l, 1, 2, 3l );
        final Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand().init( Collections.<DynamicRecord>emptyList(), recordsAfter, rule );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitSchemaRuleCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( schemaStore, times( 1 ) ).setHighestPossibleIdInUse( record.getId() );
        verify( schemaStore, times( 1 ) ).updateRecord( record );
        verify( metaDataStore, times( 1 ) ).setLatestConstraintIntroducingTx( transactionId );
        verify( cacheAccess, times( 1 ) ).addSchemaRule( rule );
    }

    @Test
    public void shouldApplyDeleteUniquenessConstraintRuleSchemaRuleCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final DynamicRecord record = DynamicRecord.dynamicRecord( 21, true );
        record.setInUse( false );
        final Collection<DynamicRecord> recordsAfter = Arrays.asList( record );
        final UniquePropertyConstraintRule
                rule = UniquePropertyConstraintRule.uniquenessConstraintRule( 0l, 1, 2, 3l );
        final Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand().init( Collections.<DynamicRecord>emptyList(), recordsAfter, rule );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitSchemaRuleCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( schemaStore, times( 1 ) ).updateRecord( record );
        verify( metaDataStore, never() ).setLatestConstraintIntroducingTx( transactionId );
        verify( cacheAccess, times( 1 ) ).removeSchemaRuleFromCache( command.getKey() );
    }

    @Test
    public void shouldApplyDeleteUniquenessConstraintRuleSchemaRuleCommandToTheStoreInRecovery() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        final DynamicRecord record = DynamicRecord.dynamicRecord( 21, true );
        record.setInUse( false );
        final Collection<DynamicRecord> recordsAfter = Arrays.asList( record );
        final UniquePropertyConstraintRule
                rule = UniquePropertyConstraintRule.uniquenessConstraintRule( 0l, 1, 2, 3l );
        final Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand().init( Collections.<DynamicRecord>emptyList(), recordsAfter, rule );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitSchemaRuleCommand( command );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( schemaStore, times( 1 ) ).setHighestPossibleIdInUse( record.getId() );
        verify( schemaStore, times( 1 ) ).updateRecord( record );
        verify( metaDataStore, never() ).setLatestConstraintIntroducingTx( transactionId );
        verify( cacheAccess, times( 1 ) ).removeSchemaRuleFromCache( command.getKey() );
    }

    // NEO STORE COMMAND

    @Test
    public void shouldApplyNeoStoreCommandToTheStore() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( false );
        final NeoStoreRecord record = new NeoStoreRecord();
        record.setNextProp( 42 );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitNeoStoreCommand( new Command.NeoStoreCommand().init( record ) );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( metaDataStore, times( 1 ) ).setGraphNextProp( record.getNextProp() );
    }

    @Test
    public void shouldApplyNeoStoreCommandToTheStoreInRecovery() throws Exception
    {
        // given
        final CommandHandler applier = newApplier( true );
        final NeoStoreRecord record = new NeoStoreRecord();
        record.setNextProp( 42 );

        // when
        boolean result = apply( applier, (handler,tx) -> {
            return handler.visitNeoStoreCommand( new Command.NeoStoreCommand().init( record ) );
        }, transactionToApply );

        // then
        assertFalse( result );

        verify( metaDataStore, times( 1 ) ).setGraphNextProp( record.getNextProp() );
    }

    private CommandHandler newApplier( boolean recovery )
    {
        CommandHandler applier = new NeoStoreTransactionApplier( neoStores, cacheAccess, lockService );
        if ( recovery )
        {
            applier = new HighIdTransactionApplier( applier, neoStores );
            applier = new CacheInvalidationTransactionApplier( applier, neoStores, cacheAccess );
        }
        // TODO begin?
        return applier;
    }

    private CommandHandler newIndexApplier( TransactionApplicationMode mode )
    {
        return new IndexTransactionApplier( indexingService, labelScanStoreSynchronizer );
    }
}
