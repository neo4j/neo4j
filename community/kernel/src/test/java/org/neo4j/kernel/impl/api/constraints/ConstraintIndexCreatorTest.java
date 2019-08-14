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
package org.neo4j.kernel.impl.api.constraints;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;

class ConstraintIndexCreatorTest
{
    private static final int PROPERTY_KEY_ID = 456;
    private static final int LABEL_ID = 123;
    private static final long INDEX_ID = 0L;

    private final LabelSchemaDescriptor descriptor = forLabel( LABEL_ID, PROPERTY_KEY_ID );
    private final UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema( descriptor );
    private final IndexDescriptor index = IndexPrototype.uniqueForSchema( forLabel( LABEL_ID, PROPERTY_KEY_ID ) ).withName( "index" ).materialise( INDEX_ID );
    private final IndexDescriptor indexReference = TestIndexDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_KEY_ID );
    private final SchemaRead schemaRead = schemaRead();
    private final SchemaWrite schemaWrite = mock( SchemaWrite.class );
    private final TokenRead tokenRead = mock( TokenRead.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    void shouldCreateIndexInAnotherTransaction() throws Exception
    {
        // given
        StubKernel kernel = new StubKernel();
        IndexProxy indexProxy = mock( IndexProxy.class );
        IndexingService indexingService = mock( IndexingService.class );
        when( indexingService.getIndexProxy( INDEX_ID ) ).thenReturn( indexProxy );
        when( indexingService.getIndexProxy( descriptor ) ).thenReturn( indexProxy );
        when( indexProxy.getDescriptor() ).thenReturn( index );
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, logProvider );

        // when
        IndexDescriptor constraintIndex = creator.createUniquenessConstraintIndex( createTransaction(), constraint, getDefaultProvider() );

        // then
        assertEquals( INDEX_ID, constraintIndex.getId() );
        verify( schemaRead ).index( descriptor );
        verifyNoMoreInteractions( schemaRead );
        verify( indexProxy ).awaitStoreScanCompleted( anyLong(), any() );
    }

    @Test
    void shouldDropIndexIfPopulationFails() throws Exception
    {
        // given

        StubKernel kernel = new StubKernel();

        IndexingService indexingService = mock( IndexingService.class );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( INDEX_ID ) ).thenReturn( indexProxy );
        when( indexingService.getIndexProxy( descriptor ) ).thenReturn( indexProxy );
        when( indexProxy.getDescriptor() ).thenReturn( index );

        IndexEntryConflictException cause = new IndexEntryConflictException( 2, 1, Values.of( "a" ) );
        doThrow( new IndexPopulationFailedKernelException( "some index", cause ) )
                .when( indexProxy ).awaitStoreScanCompleted( anyLong(), any() );
        when( schemaRead.index( any( SchemaDescriptor.class ) ) )
                .thenReturn( IndexDescriptor.NO_INDEX )   // first claim it doesn't exist, because it doesn't... so
                // that it gets created
                .thenReturn( indexReference ); // then after it failed claim it does exist
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, logProvider );

        // when
        KernelTransactionImplementation transaction = createTransaction();
        UniquePropertyValueValidationException exception = assertThrows( UniquePropertyValueValidationException.class,
                () -> creator.createUniquenessConstraintIndex( transaction, constraint, getDefaultProvider() ) );
        assertEquals( "Existing data does not satisfy CONSTRAINT ON ( label[123]:label[123] ) " +
                "ASSERT (label[123].property[456]) IS UNIQUE: Both node 2 and node 1 share the property value ( String(\"a\") )",
                exception.getMessage() );
        assertEquals( 2, kernel.transactions.size() );
        KernelTransactionImplementation tx1 = kernel.transactions.get( 0 );
        verify( tx1 ).indexUniqueCreate( eq( constraint ), eq( getDefaultProvider() ) );
        verify( schemaRead, times( 2 ) ).index( descriptor );
        verifyNoMoreInteractions( schemaRead );
        KernelTransactionImplementation kti2 = kernel.transactions.get( 1 );
        verify( kti2 ).addIndexDoDropToTxState( index );
    }

    @Test
    void shouldDropIndexInAnotherTransaction() throws Exception
    {
        // given
        StubKernel kernel = new StubKernel();
        IndexingService indexingService = mock( IndexingService.class );

        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, logProvider );

        // when
        creator.dropUniquenessConstraintIndex( index );

        // then
        assertEquals( 1, kernel.transactions.size() );
        verify( kernel.transactions.get( 0 ) ).addIndexDoDropToTxState( index );
        verifyZeroInteractions( indexingService );
    }

    @Test
    void shouldReleaseLabelLockWhileAwaitingIndexPopulation() throws Exception
    {
        // given
        StubKernel kernel = new StubKernel();
        IndexingService indexingService = mock( IndexingService.class );

        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( anyLong() ) ).thenReturn( indexProxy );
        when( indexingService.getIndexProxy( descriptor ) ).thenReturn( indexProxy );

        when( schemaRead.index( LABEL_ID, PROPERTY_KEY_ID ) ).thenReturn( IndexDescriptor.NO_INDEX );

        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, logProvider );

        // when
        KernelTransactionImplementation transaction = createTransaction();
        creator.createUniquenessConstraintIndex( transaction, constraint, getDefaultProvider() );

        // then
        verify( transaction.statementLocks().pessimistic() )
                .releaseExclusive( ResourceTypes.LABEL, descriptor.getLabelId() );

        verify( transaction.statementLocks().pessimistic() )
                .acquireExclusive( transaction.lockTracer(), ResourceTypes.LABEL, descriptor.getLabelId() );
    }

    @Test
    void shouldReuseExistingOrphanedConstraintIndex() throws Exception
    {
        // given
        IndexingService indexingService = mock( IndexingService.class );
        StubKernel kernel = new StubKernel();

        long orphanedConstraintIndexId = 111;
        IndexDescriptor indexReference = IndexPrototype.uniqueForSchema( descriptor ).withName( "constraint" ).materialise( orphanedConstraintIndexId );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( orphanedConstraintIndexId ) ).thenReturn( indexProxy );
        when( schemaRead.index( descriptor ) ).thenReturn( indexReference );
        when( schemaRead.indexGetOwningUniquenessConstraintId( indexReference ) )
                .thenReturn( null ); // which means it has no owner
        ConstraintIndexCreator creator =
                new ConstraintIndexCreator( () -> kernel, indexingService, logProvider );

        // when
        KernelTransactionImplementation transaction = createTransaction();
        IndexDescriptor constraintIndex = creator.createUniquenessConstraintIndex( transaction, constraint, getDefaultProvider() );

        // then
        assertEquals( orphanedConstraintIndexId, constraintIndex.getId() );
        assertEquals( 0, kernel.transactions.size(), "There should have been no need to acquire a statement to create the constraint index" );
        verify( schemaRead ).index( descriptor );
        verify( schemaRead ).indexGetOwningUniquenessConstraintId( indexReference );
        verifyNoMoreInteractions( schemaRead );
        verify( indexProxy ).awaitStoreScanCompleted( anyLong(), any() );
    }

    @Test
    void shouldFailOnExistingOwnedConstraintIndex() throws Exception
    {
        // given
        IndexingService indexingService = mock( IndexingService.class );
        StubKernel kernel = new StubKernel();

        long constraintIndexId = 111;
        long constraintIndexOwnerId = 222;
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( constraintIndexId ) ).thenReturn( indexProxy );
        when( schemaRead.index( descriptor ) ).thenReturn( indexReference );
        when( schemaRead.indexGetOwningUniquenessConstraintId( indexReference ) )
                .thenReturn( constraintIndexOwnerId ); // which means there's an owner
        when( tokenRead.nodeLabelName( LABEL_ID ) ).thenReturn( "MyLabel" );
        when( tokenRead.propertyKeyName( PROPERTY_KEY_ID ) ).thenReturn( "MyKey" );
        ConstraintIndexCreator creator =
                new ConstraintIndexCreator( () -> kernel, indexingService, logProvider );

        // when
        assertThrows( AlreadyConstrainedException.class, () ->
        {
            KernelTransactionImplementation transaction = createTransaction();
            creator.createUniquenessConstraintIndex( transaction, constraint, getDefaultProvider() );
        } );

        // then
        assertEquals( 0,
                kernel.transactions.size(), "There should have been no need to acquire a statement to create the constraint index" );
        verify( schemaRead ).index( descriptor );
        verify( schemaRead ).indexGetOwningUniquenessConstraintId( indexReference );
        verifyNoMoreInteractions( schemaRead );
    }

    @Test
    void shouldCreateConstraintIndexForSpecifiedProvider() throws Exception
    {
        // given
        IndexingService indexingService = mock( IndexingService.class );
        StubKernel kernel = new StubKernel();

        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( INDEX_ID ) ).thenReturn( indexProxy );
        when( indexingService.getIndexProxy( descriptor ) ).thenReturn( indexProxy );
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, logProvider );
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( "Groovy", "1.2" );

        // when
        KernelTransactionImplementation transaction = createTransaction();
        creator.createUniquenessConstraintIndex( transaction, constraint, providerDescriptor.name() );

        // then
        assertEquals( 1, kernel.transactions.size() );
        KernelTransactionImplementation transactionInstance = kernel.transactions.get( 0 );
        verify( transactionInstance ).indexUniqueCreate( eq( constraint ), eq( providerDescriptor.name() ) );
        verify( schemaRead ).index( descriptor );
        verifyNoMoreInteractions( schemaRead );
    }

    @Test
    void logMessagesAboutConstraintCreation()
            throws SchemaKernelException, UniquePropertyValueValidationException, TransactionFailureException, IndexNotFoundKernelException
    {
        StubKernel kernel = new StubKernel();
        IndexProxy indexProxy = mock( IndexProxy.class );
        IndexingService indexingService = mock( IndexingService.class );
        when( indexingService.getIndexProxy( INDEX_ID ) ).thenReturn( indexProxy );
        when( indexingService.getIndexProxy( descriptor ) ).thenReturn( indexProxy );
        when( indexProxy.getDescriptor() ).thenReturn( index );
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, logProvider );
        KernelTransactionImplementation transaction = createTransaction();

        creator.createUniquenessConstraintIndex( transaction, constraint, "indexProviderByName-1.0" );

        logProvider.rawMessageMatcher().assertContains( "Starting constraint creation: %s." );
        logProvider.rawMessageMatcher().assertContains( "Constraint %s populated, starting verification." );
        logProvider.rawMessageMatcher().assertContains( "Constraint %s verified." );
    }

    private class StubKernel implements Kernel
    {
        private final List<KernelTransactionImplementation> transactions = new ArrayList<>();

        private KernelTransaction remember( KernelTransactionImplementation kernelTransaction )
        {
            transactions.add( kernelTransaction );
            return kernelTransaction;
        }

        @Override
        public Transaction beginTransaction( Transaction.Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo )
        {
            return remember( createTransaction() );
        }

        @Override
        public Transaction beginTransaction( Transaction.Type type, LoginContext loginContext )
        {
            return remember( createTransaction() );
        }

        @Override
        public CursorFactory cursors()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }

    private SchemaRead schemaRead()
    {
        SchemaRead schemaRead = mock( SchemaRead.class );
        when( schemaRead.index( descriptor ) ).thenReturn( IndexDescriptor.NO_INDEX );
        return schemaRead;
    }

    private KernelTransactionImplementation createTransaction()
    {
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        try
        {
            TransactionHeaderInformation headerInformation = new TransactionHeaderInformation( -1, -1, new byte[0] );
            TransactionHeaderInformationFactory headerInformationFactory = mock( TransactionHeaderInformationFactory.class );
            when( headerInformationFactory.create() ).thenReturn( headerInformation );
            StorageEngine storageEngine = mock( StorageEngine.class );
            StorageReader storageReader = mock( StorageReader.class );
            when( storageEngine.newReader() ).thenReturn( storageReader );

            SimpleStatementLocks locks = new SimpleStatementLocks( mock( org.neo4j.kernel.impl.locking.Locks.Client.class ) );
            when( transaction.statementLocks() ).thenReturn( locks );
            when( transaction.tokenRead() ).thenReturn( tokenRead );
            when( transaction.schemaRead() ).thenReturn( schemaRead );
            when( transaction.schemaWrite() ).thenReturn( schemaWrite );
            TransactionState transactionState = mock( TransactionState.class );
            when( transaction.txState() ).thenReturn( transactionState );
            when( transaction.indexUniqueCreate( any( IndexBackedConstraintDescriptor.class ), any( String.class ) ) ).thenAnswer(
                    i -> IndexPrototype.uniqueForSchema( i.<IndexBackedConstraintDescriptor>getArgument( 0 ).schema() )
                            .withName( "constraint" ).materialise( INDEX_ID ) );
            when( transaction.newStorageReader() ).thenReturn( mock( StorageReader.class ) );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            fail( "Expected write transaction" );
        }
        catch ( KernelException e )
        {
            throw new RuntimeException( e );
        }
        return transaction;
    }

    private static String getDefaultProvider()
    {
        return Config.defaults().get( GraphDatabaseSettings.default_schema_provider );
    }
}
