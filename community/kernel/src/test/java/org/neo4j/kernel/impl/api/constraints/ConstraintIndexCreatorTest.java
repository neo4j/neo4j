/**
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
package org.neo4j.kernel.impl.api.constraints;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.core.Transactor;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.TxState;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.api.StatementOperationsTestHelper.mockedParts;
import static org.neo4j.kernel.impl.api.StatementOperationsTestHelper.mockedState;
import static org.neo4j.kernel.impl.nioneo.store.SchemaStorage.IndexRuleKind.CONSTRAINT;

public class ConstraintIndexCreatorTest
{
    @Test
    public void shouldCreateIndexInAnotherTransaction() throws Exception
    {
        // given
        StatementOperationParts constraintCreationContext = mockedParts();
        StatementOperationParts indexCreationContext = mockedParts();

        IndexDescriptor descriptor = new IndexDescriptor( 123, 456 );
        KernelStatement state = mockedState();

        IndexingService indexingService = mock( IndexingService.class );
        StubTransactor transactor = new StubTransactor();

        when( constraintCreationContext.schemaReadOperations().indexGetCommittedId( state, descriptor, CONSTRAINT ) )
                .thenReturn( 2468l );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getProxyForRule( 2468l ) ).thenReturn( indexProxy );

        ConstraintIndexCreator creator = new ConstraintIndexCreator( transactor, indexingService );

        // when
        long indexId = creator.createUniquenessConstraintIndex( state, constraintCreationContext.schemaReadOperations(), 123, 456 );

        // then
        assertEquals( 2468l, indexId );
        assertEquals( 1, transactor.transactions.size() );
        verify( transactor.transactions.get( 0 ).txState() ).constraintIndexRuleDoAdd( descriptor );
        verifyNoMoreInteractions( indexCreationContext.schemaWriteOperations() );
        verify( constraintCreationContext.schemaReadOperations() ).indexGetCommittedId( state, descriptor, CONSTRAINT );
        verifyNoMoreInteractions( constraintCreationContext.schemaReadOperations() );
        verify( indexProxy ).awaitStoreScanCompleted();
    }

    @Test
    public void shouldDropIndexIfPopulationFails() throws Exception
    {
        // given
        StatementOperationParts constraintCreationContext = mockedParts();
        KernelStatement state = mockedState();

        IndexDescriptor descriptor = new IndexDescriptor( 123, 456 );

        IndexingService indexingService = mock( IndexingService.class );
        StubTransactor transactor = new StubTransactor();

        when( constraintCreationContext.schemaReadOperations().indexGetCommittedId( state, descriptor, CONSTRAINT ) )
                .thenReturn( 2468l );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getProxyForRule( 2468l ) ).thenReturn( indexProxy );
        PreexistingIndexEntryConflictException cause = new PreexistingIndexEntryConflictException("a", 2, 1);
        doThrow( new IndexPopulationFailedKernelException( descriptor, "some index", cause) )
                .when(indexProxy).awaitStoreScanCompleted();

        ConstraintIndexCreator creator = new ConstraintIndexCreator( transactor, indexingService );

        // when
        try
        {
            creator.createUniquenessConstraintIndex( state, constraintCreationContext.schemaReadOperations(), 123, 456 );

            fail( "expected exception" );
        }
        // then
        catch ( ConstraintVerificationFailedKernelException e )
        {
            assertEquals( "Existing data does not satisfy CONSTRAINT ON ( n:label[123] ) ASSERT n.property[456] IS UNIQUE.",
                          e.getMessage() );
        }
        assertEquals( 2, transactor.transactions.size() );
        TxState tx1 = transactor.transactions.get( 0 ).txState();
        verify( tx1 ).constraintIndexRuleDoAdd( new IndexDescriptor( 123, 456 ) );
        verifyNoMoreInteractions( tx1 );
        verify( constraintCreationContext.schemaReadOperations() ).indexGetCommittedId( state, descriptor, CONSTRAINT );
        verifyNoMoreInteractions( constraintCreationContext.schemaReadOperations() );
        TxState tx2 = transactor.transactions.get( 1 ).txState();
        verify( tx2 ).constraintIndexDoDrop( new IndexDescriptor( 123, 456 ) );
        verifyNoMoreInteractions( tx2 );
    }

    @Test
    public void shouldDropIndexInAnotherTransaction() throws Exception
    {
        // given
        StubTransactor transactor = new StubTransactor();
        IndexingService indexingService = mock( IndexingService.class );

        IndexDescriptor descriptor = new IndexDescriptor( 123, 456 );

        ConstraintIndexCreator creator = new ConstraintIndexCreator( transactor, indexingService );

        // when
        creator.dropUniquenessConstraintIndex( descriptor );

        // then
        assertEquals( 1, transactor.transactions.size() );
        verify( transactor.transactions.get( 0 ).txState() ).constraintIndexDoDrop( descriptor );
        verifyZeroInteractions( indexingService );
    }

    private static class StubTransactor extends Transactor
    {
        final List<KernelStatement> transactions = new ArrayList<>();

        StubTransactor()
        {
            super( null, null );
        }

        @Override
        public <RESULT, FAILURE extends KernelException> RESULT execute(
                Work<RESULT, FAILURE> work ) throws FAILURE
        {
            KernelStatement state = mockedState();
            transactions.add( state );
            return work.perform( state );
        }
    }
}
