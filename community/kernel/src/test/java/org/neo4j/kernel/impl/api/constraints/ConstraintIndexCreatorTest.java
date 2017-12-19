/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.ExplicitIndexRead;
import org.neo4j.internal.kernel.api.ExplicitIndexWrite;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TransactionHook;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.StatementOperationsTestHelper.mockedParts;
import static org.neo4j.kernel.impl.api.StatementOperationsTestHelper.mockedState;

public class ConstraintIndexCreatorTest
{
    private static final int PROPERTY_KEY_ID = 456;
    private static final int LABEL_ID = 123;

    private final LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( LABEL_ID, PROPERTY_KEY_ID );
    private final IndexDescriptor index = IndexDescriptorFactory.uniqueForLabel( 123, 456 );

    @Test
    public void shouldCreateIndexInAnotherTransaction() throws Exception
    {
        // given
        StatementOperationParts constraintCreationContext = mockedParts();
        StatementOperationParts indexCreationContext = mockedParts();

        KernelStatement state = mockedState();

        IndexingService indexingService = mock( IndexingService.class );
        StubKernel kernel = new StubKernel();

        when( constraintCreationContext.schemaReadOperations().indexGetCommittedId( state, index ) )
                .thenReturn( 2468L );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( 2468L ) ).thenReturn( indexProxy );
        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
        when( constraintCreationContext.schemaReadOperations().indexGetForSchema( state, descriptor ) )
                .thenReturn( null );
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, propertyAccessor );

        // when
        long indexId = creator.createUniquenessConstraintIndex( state, constraintCreationContext.schemaReadOperations(), descriptor );

        // then
        assertEquals( 2468L, indexId );
        assertEquals( 1, kernel.statements.size() );
        verify( kernel.statements.get( 0 ).txState() ).indexRuleDoAdd( eq( index ) );
        verifyNoMoreInteractions( indexCreationContext.schemaWriteOperations() );
        verify( constraintCreationContext.schemaReadOperations() ).indexGetCommittedId( state, index );
        verify( constraintCreationContext.schemaReadOperations() ).indexGetForSchema( state, descriptor );
        verifyNoMoreInteractions( constraintCreationContext.schemaReadOperations() );
        verify( indexProxy ).awaitStoreScanCompleted();
    }

    @Test
    public void shouldDropIndexIfPopulationFails() throws Exception
    {
        // given
        StatementOperationParts constraintCreationContext = mockedParts();
        KernelStatement state = mockedState();

        IndexingService indexingService = mock( IndexingService.class );
        StubKernel kernel = new StubKernel();

        when( constraintCreationContext.schemaReadOperations().indexGetCommittedId( state, index ) )
                .thenReturn( 2468L );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( 2468L ) ).thenReturn( indexProxy );
        IndexEntryConflictException cause = new IndexEntryConflictException( 2, 1, Values.of( "a" ) );
        doThrow( new IndexPopulationFailedKernelException( descriptor, "some index", cause ) )
                .when( indexProxy ).awaitStoreScanCompleted();
        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
        when( constraintCreationContext.schemaReadOperations().indexGetForSchema( state, descriptor ) )
                .thenReturn( null );
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, propertyAccessor );

        // when
        try
        {
            creator.createUniquenessConstraintIndex( state, constraintCreationContext.schemaReadOperations(), descriptor );

            fail( "expected exception" );
        }
        // then
        catch ( UniquePropertyValueValidationException e )
        {
            assertEquals( "Existing data does not satisfy CONSTRAINT ON ( label[123]:label[123] ) " +
                            "ASSERT label[123].property[456] IS UNIQUE.", e.getMessage() );
        }
        assertEquals( 2, kernel.statements.size() );
        TransactionState tx1 = kernel.statements.get( 0 ).txState();
        IndexDescriptor newIndex = IndexDescriptorFactory.uniqueForLabel( 123, 456 );
        verify( tx1 ).indexRuleDoAdd( newIndex );
        verifyNoMoreInteractions( tx1 );
        verify( constraintCreationContext.schemaReadOperations() ).indexGetCommittedId( state, index );
        verify( constraintCreationContext.schemaReadOperations() ).indexGetForSchema( state, descriptor );
        verifyNoMoreInteractions( constraintCreationContext.schemaReadOperations() );
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

        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, propertyAccessor );

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

        when( constraintCreationContext.schemaReadOperations().indexGetCommittedId( state, index ) )
                .thenReturn( 2468L );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( anyLong() ) ).thenReturn( indexProxy );
        when( constraintCreationContext.schemaReadOperations().indexGetForSchema( state, descriptor ) )
                .thenReturn( null );

        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, propertyAccessor );

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

        long orphanedConstraintIndexId = 111;
        when( constraintCreationContext.schemaReadOperations().indexGetCommittedId( state, index ) )
                .thenReturn( orphanedConstraintIndexId );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( orphanedConstraintIndexId ) ).thenReturn( indexProxy );
        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
        when( constraintCreationContext.schemaReadOperations().indexGetForSchema( state, descriptor ) )
                .thenReturn( index );
        when( constraintCreationContext.schemaReadOperations().indexGetOwningUniquenessConstraintId(
                state, index ) ).thenReturn( null ); // which means it has no owner
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, propertyAccessor );

        // when
        long indexId = creator.createUniquenessConstraintIndex( state,
                constraintCreationContext.schemaReadOperations(), descriptor );

        // then
        assertEquals( orphanedConstraintIndexId, indexId );
        assertEquals( "There should have been no need to acquire a statement to create the constraint index", 0,
                kernel.statements.size() );
        verifyNoMoreInteractions( indexCreationContext.schemaWriteOperations() );
        verify( constraintCreationContext.schemaReadOperations() ).indexGetCommittedId( state, index );
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
        when( constraintCreationContext.schemaReadOperations().indexGetCommittedId( state, index ) )
                .thenReturn( constraintIndexId );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getIndexProxy( constraintIndexId ) ).thenReturn( indexProxy );
        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
        when( constraintCreationContext.schemaReadOperations().indexGetForSchema( state, descriptor ) )
                .thenReturn( index );
        when( constraintCreationContext.schemaReadOperations().indexGetOwningUniquenessConstraintId(
                state, index ) ).thenReturn( constraintIndexOwnerId ); // which means there's an owner
        when( state.readOperations().labelGetName( LABEL_ID ) ).thenReturn( "MyLabel" );
        when( state.readOperations().propertyKeyGetName( PROPERTY_KEY_ID ) ).thenReturn( "MyKey" );
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, propertyAccessor );

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
        public KernelTransaction newTransaction( KernelTransaction.Type type, SecurityContext securityContext )
        {
            return new StubKernelTransaction();
        }

        @Override
        public KernelTransaction newTransaction( KernelTransaction.Type type, SecurityContext securityContext, long timeout )
                throws TransactionFailureException
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
        public void registerUserFunction( CallableUserFunction function ) throws ProcedureException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void registerUserAggregationFunction( CallableUserAggregationFunction function )
                throws ProcedureException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CursorFactory cursors()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Session beginSession( SecurityContext securityContext )
        {
            throw new UnsupportedOperationException();
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
            public CursorFactory cursors()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public long closeTransaction() throws TransactionFailureException
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
                return null;
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
            public PropertyCursor propertyCursor()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }
        }
    }
}
