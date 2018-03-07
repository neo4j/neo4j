/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.constraints;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.ExplicitIndexRead;
import org.neo4j.internal.kernel.api.ExplicitIndexWrite;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.Modes;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TransactionHook;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.transaction.state.DefaultSchemaIndexProviderMap;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.StatementOperationsTestHelper.mockedParts;
import static org.neo4j.kernel.impl.api.StatementOperationsTestHelper.mockedState;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;

public class ConstraintIndexCreatorTest
{
    private static final int PROPERTY_KEY_ID = 456;
    private static final int LABEL_ID = 123;

    private final LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( LABEL_ID, PROPERTY_KEY_ID );
    private final IndexDescriptor index = IndexDescriptorFactory.uniqueForLabel( 123, 456 );
    private final SchemaStorage storage = mock( SchemaStorage.class );
    private SchemaIndexProviderMap schemaIndexProviderMap;
    private IndexRule rule;

    @Before
    public void setUp() throws Exception
    {
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        when( provider.getOnlineAccessor( anyLong(), any( IndexDescriptor.class ), any( IndexSamplingConfig.class ) ) )
                .thenReturn( mock( IndexAccessor.class ) );
        schemaIndexProviderMap = new DefaultSchemaIndexProviderMap( provider );
        rule = IndexRule.indexRule( 2468L, index, schemaIndexProviderMap.getDefaultProvider().getProviderDescriptor() );
    }

    @Test
    public void createIndexForConstraint() throws Exception
    {
        // given
        StatementOperationParts constraintCreationContext = mockedParts();
        StatementOperationParts indexCreationContext = mockedParts();

        KernelStatement state = mockedState();

        IndexingService indexingService = mock( IndexingService.class );
        StubKernel kernel = new StubKernel();

        when( constraintCreationContext.schemaReadOperations().indexGetExistingRule( state, index ) )
                .thenReturn( rule );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( storage.newRuleId() ).thenReturn( 2468L );
        when( indexingService.getIndexProxy( 2468L ) ).thenReturn( indexProxy );
        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
        when( constraintCreationContext.schemaReadOperations().indexGetForSchema( state, descriptor ) )
                .thenReturn( null );
        ConstraintIndexCreator creator = getConstraintCreator( indexingService, kernel, propertyAccessor );

        // when
        IndexRule indexRule = creator.createUniquenessConstraintIndex( state, constraintCreationContext.schemaReadOperations(), descriptor );

        // then
        assertEquals( rule, indexRule );
        verifyNoMoreInteractions( indexCreationContext.schemaWriteOperations() );
        verify( constraintCreationContext.schemaReadOperations() ).indexGetForSchema( state, descriptor );
        verifyNoMoreInteractions( constraintCreationContext.schemaReadOperations() );
        verify( indexProxy ).awaitStoreScanCompleted();
    }

    private ConstraintIndexCreator getConstraintCreator( IndexingService indexingService, StubKernel kernel,
            PropertyAccessor propertyAccessor )
    {
        return new ConstraintIndexCreator( () -> kernel, indexingService, propertyAccessor, storage, schemaIndexProviderMap );
    }

    @Test
    @Ignore
    public void shouldDropIndexIfPopulationFails() throws Exception
    {
        // given
        StatementOperationParts constraintCreationContext = mockedParts();
        KernelStatement state = mockedState();

        IndexingService indexingService = mock( IndexingService.class );
        StubKernel kernel = new StubKernel();

        SchemaReadOperations schemaOps = constraintCreationContext.schemaReadOperations();
        when( schemaOps.indexGetExistingRule( state, index ) ).thenReturn( rule );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( storage.newRuleId() ).thenReturn( 2468L );
        when( indexingService.getIndexProxy( 2468L ) ).thenReturn( indexProxy );
        IndexEntryConflictException cause = new IndexEntryConflictException( 2, 1, Values.of( "a" ) );
        doThrow( new IndexPopulationFailedKernelException( descriptor, "some index", cause ) )
                .when( indexProxy ).awaitStoreScanCompleted();
        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
        when( schemaOps.indexGetForSchema( any( KernelStatement.class ), any( LabelSchemaDescriptor.class ) ) )
                .thenReturn( null )   // first claim it doesn't exist, because it doesn't... so that it gets created
                .thenReturn( index ); // then after it failed claim it does exist
        ConstraintIndexCreator creator = getConstraintCreator( indexingService, kernel, propertyAccessor );

        // when
        try
        {
            creator.createUniquenessConstraintIndex( state, schemaOps, descriptor );

            fail( "expected exception" );
        }
        // then
        catch ( UniquePropertyValueValidationException e )
        {
            assertEquals( "Existing data does not satisfy CONSTRAINT ON ( label[123]:label[123] ) " +
                            "ASSERT label[123].property[456] IS UNIQUE.", e.getMessage() );
        }
        TransactionState tx1 = kernel.statements.get( 0 ).txState();
        IndexDescriptor newIndex = IndexDescriptorFactory.uniqueForLabel( 123, 456 );
        verify( tx1 ).indexRuleDoAdd( newIndex );
        verifyNoMoreInteractions( tx1 );
        verify( schemaOps ).indexGetExistingRule( state, index );
        verify( schemaOps, times( 2 ) ).indexGetForSchema( state, descriptor );
        verifyNoMoreInteractions( schemaOps );
        TransactionState tx2 = kernel.statements.get( 1 ).txState();
        verify( tx2 ).indexDoDrop( newIndex );
        verifyNoMoreInteractions( tx2 );
    }

    @Test
    public void shouldDropIndexInAnotherTransaction() throws Exception
    {
        // given
        StubKernel kernel = new StubKernel();
        IndexingService indexingService = mock( IndexingService.class );

        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );

        ConstraintIndexCreator creator = getConstraintCreator( indexingService, kernel, propertyAccessor );

        // when
        creator.dropUniquenessConstraintIndex( index );

        // then
        assertEquals( 1, kernel.statements.size() );
        verify( kernel.statements.get( 0 ).txState() ).indexDoDrop( index );
        verifyZeroInteractions( indexingService );
    }

    @Test
    public void shouldReleaseLabelLockWhileAwaitingIndexPopulation() throws Exception
    {
        // given
        StubKernel kernel = new StubKernel();
        IndexingService indexingService = mock( IndexingService.class );
        StatementOperationParts constraintCreationContext = mockedParts();

        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );

        KernelStatement state = mockedState();

        when( constraintCreationContext.schemaReadOperations().indexGetExistingRule( state, index ) )
                .thenReturn( rule );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( anyLong() ) ).thenReturn( indexProxy );
        when( constraintCreationContext.schemaReadOperations().indexGetForSchema( state, descriptor ) )
                .thenReturn( null );

        ConstraintIndexCreator creator = getConstraintCreator( indexingService, kernel, propertyAccessor );

        // when
        creator.createUniquenessConstraintIndex( state, constraintCreationContext.schemaReadOperations(), descriptor );

        // then
        verify( state.locks().pessimistic() )
                .releaseExclusive( ResourceTypes.LABEL, descriptor.getLabelId() );

        verify( state.locks().pessimistic() )
                .acquireExclusive( state.lockTracer(), ResourceTypes.LABEL, descriptor.getLabelId() );
    }

    @Test
    public void shouldReuseExistingOrphanedConstraintIndex() throws Exception
    {
        // given
        StatementOperationParts constraintCreationContext = mockedParts();
        StatementOperationParts indexCreationContext = mockedParts();

        KernelStatement state = mockedState();

        IndexingService indexingService = mock( IndexingService.class );
        StubKernel kernel = new StubKernel();

        when( constraintCreationContext.schemaReadOperations().indexGetExistingRule( state, index ) )
                .thenReturn( rule );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( 2468L ) ).thenReturn( indexProxy );
        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
        when( constraintCreationContext.schemaReadOperations().indexGetForSchema( state, descriptor ) )
                .thenReturn( index );
        when( constraintCreationContext.schemaReadOperations().indexGetOwningUniquenessConstraintId(
                state, index ) ).thenReturn( null ); // which means it has no owner
        ConstraintIndexCreator creator = getConstraintCreator( indexingService, kernel, propertyAccessor );

        IndexRule indexRule = creator.createUniquenessConstraintIndex( state,
                constraintCreationContext.schemaReadOperations(), descriptor );

        // then
        assertEquals( 2468L, indexRule.getId() );
        verifyNoMoreInteractions( indexCreationContext.schemaWriteOperations() );
        verify( constraintCreationContext.schemaReadOperations() ).indexGetExistingRule( state, index );
        verify( constraintCreationContext.schemaReadOperations() ).indexGetForSchema( state, descriptor );
        verify( constraintCreationContext.schemaReadOperations() )
                .indexGetOwningUniquenessConstraintId( state, index );
        verifyNoMoreInteractions( constraintCreationContext.schemaReadOperations() );
        verify( indexProxy ).awaitStoreScanCompleted();
    }

    @Test
    public void shouldFailOnExistingOwnedConstraintIndex() throws Exception
    {
        // given
        StatementOperationParts constraintCreationContext = mockedParts();
        StatementOperationParts indexCreationContext = mockedParts();

        KernelStatement state = mockedState();

        IndexingService indexingService = mock( IndexingService.class );
        StubKernel kernel = new StubKernel();

        long constraintIndexId = 111;
        long constraintIndexOwnerId = 222;
        when( constraintCreationContext.schemaReadOperations().indexGetExistingRule( state, index ) )
                .thenReturn( rule );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( constraintIndexId ) ).thenReturn( indexProxy );
        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
        when( constraintCreationContext.schemaReadOperations().indexGetForSchema( state, descriptor ) )
                .thenReturn( index );
        when( constraintCreationContext.schemaReadOperations().indexGetOwningUniquenessConstraintId(
                state, index ) ).thenReturn( constraintIndexOwnerId ); // which means there's an owner
        when( state.readOperations().labelGetName( LABEL_ID ) ).thenReturn( "MyLabel" );
        when( state.readOperations().propertyKeyGetName( PROPERTY_KEY_ID ) ).thenReturn( "MyKey" );
        ConstraintIndexCreator creator = getConstraintCreator( indexingService, kernel, propertyAccessor );

        // when
        try
        {
            creator.createUniquenessConstraintIndex(
                    state, constraintCreationContext.schemaReadOperations(), descriptor );
            fail( "Should've failed" );
        }
        catch ( AlreadyConstrainedException e )
        {
            // THEN good
        }

        // then
        assertEquals( "There should have been no need to acquire a statement to create the constraint index", 0,
                kernel.statements.size() );
        verifyNoMoreInteractions( indexCreationContext.schemaWriteOperations() );
        verify( constraintCreationContext.schemaReadOperations() ).indexGetForSchema( state, descriptor );
        verify( constraintCreationContext.schemaReadOperations() )
                .indexGetOwningUniquenessConstraintId( state, index );
        verifyNoMoreInteractions( constraintCreationContext.schemaReadOperations() );
    }

    private class StubKernel implements InwardKernel
    {
        private final List<KernelStatement> statements = new ArrayList<>();

        @Override
        public KernelTransaction newTransaction( KernelTransaction.Type type, LoginContext loginContext )
        {
            return new StubKernelTransaction();
        }

        @Override
        public KernelTransaction newTransaction( KernelTransaction.Type type, LoginContext loginContext, long timeout )
        {
            return new StubKernelTransaction( timeout );
        }

        @Override
        public void registerTransactionHook( TransactionHook hook )
        {
            throw new UnsupportedOperationException( "Please implement" );
        }

        @Override
        public void registerProcedure( CallableProcedure procedure )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void registerUserFunction( CallableUserFunction function )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void registerUserAggregationFunction( CallableUserAggregationFunction function )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CursorFactory cursors()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Session beginSession( LoginContext loginContext )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Modes modes()
        {
            return null;
        }

        private class StubKernelTransaction implements KernelTransaction
        {
            private long timeout;

            StubKernelTransaction()
            {
            }

            StubKernelTransaction( long timeout )
            {
                this.timeout = timeout;
            }

            @Override
            public void success()
            {
            }

            @Override
            public void failure()
            {
            }

            @Override
            public Read dataRead()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public Read stableDataRead()
            {
                return null;
            }

            @Override
            public void markAsStable()
            {

            }

            @Override
            public Write dataWrite()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public ExplicitIndexRead indexRead()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public ExplicitIndexWrite indexWrite()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public TokenRead tokenRead()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public TokenWrite tokenWrite()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public Token token()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public SchemaRead schemaRead()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public SchemaWrite schemaWrite()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public Locks locks()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public Procedures procedures()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public CursorFactory cursors()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public long closeTransaction()
            {
                return ROLLBACK;
            }

            @Override
            public Statement acquireStatement()
            {
                return remember( mockedState() );
            }

            private Statement remember( KernelStatement mockedState )
            {
                statements.add( mockedState );
                return mockedState;
            }

            @Override
            public boolean isOpen()
            {
                return true;
            }

            @Override
            public SecurityContext securityContext()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Status> getReasonIfTerminated()
            {
                return Optional.empty();
            }

            @Override
            public boolean isTerminated()
            {
                return false;
            }

            @Override
            public void markForTermination( Status reason )
            {
            }

            @Override
            public long lastTransactionTimestampWhenStarted()
            {
                return 0;
            }

            @Override
            public void registerCloseListener( CloseListener listener )
            {
            }

            @Override
            public Type transactionType()
            {
                return null;
            }

            @Override
            public long getTransactionId()
            {
                return -1;
            }

            @Override
            public long getCommitTime()
            {
                return -1;
            }

            @Override
            public Revertable overrideWith( SecurityContext context )
            {
                return null;
            }

            @Override
            public long lastTransactionIdWhenStarted()
            {
                return 0;
            }

            @Override
            public long startTime()
            {
                return 0;
            }

            @Override
            public long timeout()
            {
                return timeout;
            }

            @Override
            public NodeCursor nodeCursor()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public RelationshipScanCursor relationshipCursor()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public PropertyCursor propertyCursor()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public ClockContext clocks()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }
        }
    }
}
