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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexDescriptorFactory;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
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

public class ConstraintIndexCreatorTest
{
    private static final int PROPERTY_KEY_ID = 456;
    private static final int LABEL_ID = 123;
    private static final long INDEX_ID = 0L;

    private final LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( LABEL_ID, PROPERTY_KEY_ID );
    private final IndexDescriptor index = TestIndexDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_KEY_ID );
    private final IndexReference indexReference = TestIndexDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_KEY_ID );
    private final SchemaRead schemaRead = schemaRead();
    private final SchemaWrite schemaWrite = mock( SchemaWrite.class );
    private final TokenRead tokenRead = mock( TokenRead.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    public void shouldCreateIndexInAnotherTransaction() throws Exception
    {
        // given
        StubKernel kernel = new StubKernel();
        IndexProxy indexProxy = mock( IndexProxy.class );
        IndexingService indexingService = mock( IndexingService.class );
        when( indexingService.getIndexProxy( INDEX_ID ) ).thenReturn( indexProxy );
        when( indexingService.getIndexProxy( descriptor ) ).thenReturn( indexProxy );
        when( indexProxy.getDescriptor() ).thenReturn( index.withId( INDEX_ID ).withoutCapabilities() );
        NodePropertyAccessor nodePropertyAccessor = mock( NodePropertyAccessor.class );
        ConstraintIndexCreator creator =
                new ConstraintIndexCreator( () -> kernel, indexingService, nodePropertyAccessor, logProvider );

        // when
        long indexId = creator.createUniquenessConstraintIndex( createTransaction(), descriptor, getDefaultProvider() );

        // then
        assertEquals( INDEX_ID, indexId );
        verify( schemaRead ).indexGetCommittedId( indexReference );
        verify( schemaRead ).index( descriptor );
        verifyNoMoreInteractions( schemaRead );
        verify( indexProxy ).awaitStoreScanCompleted( anyLong(), any() );
    }

    @Test
    public void shouldDropIndexIfPopulationFails() throws Exception
    {
        // given

        StubKernel kernel = new StubKernel();

        IndexingService indexingService = mock( IndexingService.class );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( INDEX_ID ) ).thenReturn( indexProxy );
        when( indexingService.getIndexProxy( descriptor ) ).thenReturn( indexProxy );
        when( indexProxy.getDescriptor() ).thenReturn( index.withId( INDEX_ID ).withoutCapabilities() );

        IndexEntryConflictException cause = new IndexEntryConflictException( 2, 1, Values.of( "a" ) );
        doThrow( new IndexPopulationFailedKernelException( "some index", cause ) )
                .when( indexProxy ).awaitStoreScanCompleted( anyLong(), any() );
        NodePropertyAccessor nodePropertyAccessor = mock( NodePropertyAccessor.class );
        when( schemaRead.index( any( SchemaDescriptor.class ) ) )
                .thenReturn(
                        IndexReference.NO_INDEX )   // first claim it doesn't exist, because it doesn't... so
                // that it gets created
                .thenReturn( indexReference ); // then after it failed claim it does exist
        ConstraintIndexCreator creator =
                new ConstraintIndexCreator( () -> kernel, indexingService, nodePropertyAccessor, logProvider );

        // when
        KernelTransactionImplementation transaction = createTransaction();
        try
        {
            creator.createUniquenessConstraintIndex( transaction, descriptor, getDefaultProvider() );

            fail( "expected exception" );
        }
        // then
        catch ( UniquePropertyValueValidationException e )
        {
            assertEquals( "Existing data does not satisfy CONSTRAINT ON ( label[123]:label[123] ) " +
                    "ASSERT label[123].property[456] IS UNIQUE: Both node 2 and node 1 share the property value ( String(\"a\") )", e.getMessage() );
        }
        assertEquals( 2, kernel.transactions.size() );
        KernelTransactionImplementation tx1 = kernel.transactions.get( 0 );
        SchemaDescriptor newIndex = index.schema();
        verify( tx1 ).indexUniqueCreate( eq( newIndex ), eq( getDefaultProvider() ) );
        verify( schemaRead ).indexGetCommittedId( indexReference );
        verify( schemaRead, times( 2 ) ).index( descriptor );
        verifyNoMoreInteractions( schemaRead );
        TransactionState tx2 = kernel.transactions.get( 1 ).txState();
        verify( tx2 ).indexDoDrop( index );
        verifyNoMoreInteractions( tx2 );
    }

    @Test
    public void shouldDropIndexInAnotherTransaction() throws Exception
    {
        // given
        StubKernel kernel = new StubKernel();
        IndexingService indexingService = mock( IndexingService.class );

        NodePropertyAccessor nodePropertyAccessor = mock( NodePropertyAccessor.class );
        ConstraintIndexCreator creator =
                new ConstraintIndexCreator( () -> kernel, indexingService, nodePropertyAccessor, logProvider );

        // when
        creator.dropUniquenessConstraintIndex( index );

        // then
        assertEquals( 1, kernel.transactions.size() );
        verify( kernel.transactions.get( 0 ).txState() ).indexDoDrop( index );
        verifyZeroInteractions( indexingService );
    }

    @Test
    public void shouldReleaseLabelLockWhileAwaitingIndexPopulation() throws Exception
    {
        // given
        StubKernel kernel = new StubKernel();
        IndexingService indexingService = mock( IndexingService.class );

        NodePropertyAccessor nodePropertyAccessor = mock( NodePropertyAccessor.class );

        when( schemaRead.indexGetCommittedId( indexReference ) ).thenReturn( INDEX_ID );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( anyLong() ) ).thenReturn( indexProxy );
        when( indexingService.getIndexProxy( descriptor ) ).thenReturn( indexProxy );

        when( schemaRead.index( LABEL_ID, PROPERTY_KEY_ID ) ).thenReturn( IndexReference.NO_INDEX );

        ConstraintIndexCreator creator =
                new ConstraintIndexCreator( () -> kernel, indexingService, nodePropertyAccessor, logProvider );

        // when
        KernelTransactionImplementation transaction = createTransaction();
        creator.createUniquenessConstraintIndex( transaction, descriptor, getDefaultProvider() );

        // then
        verify( transaction.statementLocks().pessimistic() )
                .releaseExclusive( ResourceTypes.LABEL, descriptor.getLabelId() );

        verify( transaction.statementLocks().pessimistic() )
                .acquireExclusive( transaction.lockTracer(), ResourceTypes.LABEL, descriptor.getLabelId() );
    }

    @Test
    public void shouldReuseExistingOrphanedConstraintIndex() throws Exception
    {
        // given
        IndexingService indexingService = mock( IndexingService.class );
        StubKernel kernel = new StubKernel();

        long orphanedConstraintIndexId = 111;
        when( schemaRead.indexGetCommittedId( indexReference ) ).thenReturn( orphanedConstraintIndexId );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( orphanedConstraintIndexId ) ).thenReturn( indexProxy );
        NodePropertyAccessor nodePropertyAccessor = mock( NodePropertyAccessor.class );
        when( schemaRead.index( descriptor ) ).thenReturn( indexReference );
        when( schemaRead.indexGetOwningUniquenessConstraintId( indexReference ) )
                .thenReturn( null ); // which means it has no owner
        ConstraintIndexCreator creator =
                new ConstraintIndexCreator( () -> kernel, indexingService, nodePropertyAccessor, logProvider );

        // when
        KernelTransactionImplementation transaction = createTransaction();
        long indexId = creator.createUniquenessConstraintIndex( transaction, descriptor, getDefaultProvider() );

        // then
        assertEquals( orphanedConstraintIndexId, indexId );
        assertEquals( "There should have been no need to acquire a statement to create the constraint index", 0,
                kernel.transactions.size() );
        verify( schemaRead ).indexGetCommittedId( indexReference );
        verify( schemaRead ).index( descriptor );
        verify( schemaRead ).indexGetOwningUniquenessConstraintId( indexReference );
        verifyNoMoreInteractions( schemaRead );
        verify( indexProxy ).awaitStoreScanCompleted( anyLong(), any() );
    }

    @Test
    public void shouldFailOnExistingOwnedConstraintIndex() throws Exception
    {
        // given
        IndexingService indexingService = mock( IndexingService.class );
        StubKernel kernel = new StubKernel();

        long constraintIndexId = 111;
        long constraintIndexOwnerId = 222;
        when( schemaRead.indexGetCommittedId( indexReference ) ).thenReturn( constraintIndexId );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( constraintIndexId ) ).thenReturn( indexProxy );
        NodePropertyAccessor nodePropertyAccessor = mock( NodePropertyAccessor.class );
        when( schemaRead.index( descriptor ) ).thenReturn( indexReference );
        when( schemaRead.indexGetOwningUniquenessConstraintId( indexReference ) )
                .thenReturn( constraintIndexOwnerId ); // which means there's an owner
        when( tokenRead.nodeLabelName( LABEL_ID ) ).thenReturn( "MyLabel" );
        when( tokenRead.propertyKeyName( PROPERTY_KEY_ID ) ).thenReturn( "MyKey" );
        ConstraintIndexCreator creator =
                new ConstraintIndexCreator( () -> kernel, indexingService, nodePropertyAccessor, logProvider );

        // when
        try
        {
            KernelTransactionImplementation transaction = createTransaction();
            creator.createUniquenessConstraintIndex( transaction, descriptor, getDefaultProvider() );
            fail( "Should've failed" );
        }
        catch ( AlreadyConstrainedException e )
        {
            // THEN good
        }

        // then
        assertEquals( "There should have been no need to acquire a statement to create the constraint index", 0,
                kernel.transactions.size() );
        verify( schemaRead ).index( descriptor );
        verify( schemaRead ).indexGetOwningUniquenessConstraintId( indexReference );
        verifyNoMoreInteractions( schemaRead );
    }

    @Test
    public void shouldCreateConstraintIndexForSpecifiedProvider() throws Exception
    {
        // given
        IndexingService indexingService = mock( IndexingService.class );
        StubKernel kernel = new StubKernel();

        when( schemaRead.indexGetCommittedId( indexReference ) ).thenReturn( INDEX_ID );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( INDEX_ID ) ).thenReturn( indexProxy );
        when( indexingService.getIndexProxy( descriptor ) ).thenReturn( indexProxy );
        NodePropertyAccessor nodePropertyAccessor = mock( NodePropertyAccessor.class );
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, nodePropertyAccessor, logProvider );
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( "Groovy", "1.2" );

        // when
        KernelTransactionImplementation transaction = createTransaction();
        creator.createUniquenessConstraintIndex( transaction, descriptor, providerDescriptor.name() );

        // then
        assertEquals( 1, kernel.transactions.size() );
        KernelTransactionImplementation transactionInstance = kernel.transactions.get( 0 );
        verify( transactionInstance ).indexUniqueCreate( eq( descriptor ), eq( providerDescriptor.name() ) );
        verify( schemaRead ).index( descriptor );
        verify( schemaRead ).indexGetCommittedId( any() );
        verifyNoMoreInteractions( schemaRead );
    }

    @Test
    public void logMessagesAboutConstraintCreation()
            throws SchemaKernelException, UniquePropertyValueValidationException, TransactionFailureException, IndexNotFoundKernelException
    {
        StubKernel kernel = new StubKernel();
        IndexProxy indexProxy = mock( IndexProxy.class );
        IndexingService indexingService = mock( IndexingService.class );
        when( indexingService.getIndexProxy( INDEX_ID ) ).thenReturn( indexProxy );
        when( indexingService.getIndexProxy( descriptor ) ).thenReturn( indexProxy );
        when( indexProxy.getDescriptor() ).thenReturn( index.withId( INDEX_ID ).withoutCapabilities() );
        NodePropertyAccessor propertyAccessor = mock( NodePropertyAccessor.class );
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, propertyAccessor, logProvider );
        KernelTransactionImplementation transaction = createTransaction();

        creator.createUniquenessConstraintIndex( transaction, descriptor, "indexProviderByName-1.0" );

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
        public Transaction beginTransaction( Transaction.Type type, LoginContext loginContext )
        {
            return remember( createTransaction() );
        }
    }

    private SchemaRead schemaRead()
    {
        SchemaRead schemaRead = mock( SchemaRead.class );
        when( schemaRead.index( descriptor ) ).thenReturn( IndexReference.NO_INDEX );
        try
        {
            when( schemaRead.indexGetCommittedId( indexReference ) ).thenReturn( INDEX_ID );
        }
        catch ( SchemaKernelException e )
        {
            throw new AssertionError( e );
        }
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
            when( transaction.indexUniqueCreate( any( SchemaDescriptor.class ), any( String.class ) ) ).thenAnswer(
                    i -> IndexDescriptorFactory.uniqueForSchema( i.getArgument( 0 ) ) );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            fail( "Expected write transaction" );
        }
        catch ( SchemaKernelException e )
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
