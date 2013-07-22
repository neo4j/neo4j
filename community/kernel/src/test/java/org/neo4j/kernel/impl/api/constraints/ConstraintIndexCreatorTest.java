/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.Iterator;

import org.junit.Test;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.impl.api.Transactor;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;

import static java.util.Arrays.asList;

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

public class ConstraintIndexCreatorTest
{
    @Test
    public void shouldCreateIndexInAnotherTransaction() throws Exception
    {
        // given
        StatementOperationParts constraintCreationContext = mockedParts();
        StatementOperationParts indexCreationContext = mockedParts();

        IndexDescriptor descriptor = new IndexDescriptor( 123, 456 );
        StatementState state = mockedState();
        when( indexCreationContext.schemaWriteOperations().uniqueIndexCreate( state, 123, 456 ) ).thenReturn( descriptor );

        IndexingService indexingService = mock( IndexingService.class );
        StubTransactor transactor = new StubTransactor( state, indexCreationContext );

        when( constraintCreationContext.schemaReadOperations().indexGetCommittedId( state, descriptor ) ).thenReturn( 2468l );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getProxyForRule( 2468l ) ).thenReturn( indexProxy );

        ConstraintIndexCreator creator = new ConstraintIndexCreator( transactor, indexingService );

        // when
        long indexId = creator.createUniquenessConstraintIndex( state, constraintCreationContext.schemaReadOperations(), 123, 456 );

        // then
        assertEquals( 2468l, indexId );
        verify( indexCreationContext.schemaWriteOperations() ).uniqueIndexCreate( state, 123, 456 );
        verifyNoMoreInteractions( indexCreationContext.schemaWriteOperations() );
        verify( constraintCreationContext.schemaReadOperations() ).indexGetCommittedId( state, descriptor );
        verifyNoMoreInteractions( constraintCreationContext.schemaReadOperations() );
        verify( indexProxy ).awaitStoreScanCompleted();
    }

    @Test
    public void shouldDropIndexIfPopulationFails() throws Exception
    {
        // given
        StatementOperationParts constraintCreationContext = mockedParts();
        StatementOperationParts indexCreationContext = mockedParts();
        StatementOperationParts indexDestructionContext = mockedParts();
        StatementState state = mockedState();

        IndexDescriptor descriptor = new IndexDescriptor( 123, 456 );
        when( indexCreationContext.schemaWriteOperations().uniqueIndexCreate( state, 123, 456 ) ).thenReturn( descriptor );

        IndexingService indexingService = mock( IndexingService.class );
        StubTransactor transactor = new StubTransactor( state, indexCreationContext, indexDestructionContext );

        when( constraintCreationContext.schemaReadOperations().indexGetCommittedId( state, descriptor ) ).thenReturn( 2468l );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getProxyForRule( 2468l ) ).thenReturn( indexProxy );
        doThrow( new IndexPopulationFailedKernelException( descriptor, new PreexistingIndexEntryConflictException( "a", 2, 1 ) ) )
                .when( indexProxy ).awaitStoreScanCompleted();

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
        verify( indexCreationContext.schemaWriteOperations() ).uniqueIndexCreate( state, 123, 456 );
        verifyNoMoreInteractions( indexCreationContext.schemaWriteOperations() );
        verify( constraintCreationContext.schemaReadOperations() ).indexGetCommittedId( state, descriptor );
        verifyNoMoreInteractions( constraintCreationContext.schemaReadOperations() );
        verify( indexDestructionContext.schemaWriteOperations() ).uniqueIndexDrop( state, descriptor );
        verifyNoMoreInteractions( indexDestructionContext.schemaWriteOperations() );
    }

    @Test
    public void shouldDropIndexInAnotherTransaction() throws Exception
    {
        // given
        StatementOperationParts indexDestructionTransaction = mockedParts();
        StatementState state = mockedState();
        StubTransactor transactor = new StubTransactor( state, indexDestructionTransaction );
        IndexingService indexingService = mock( IndexingService.class );

        IndexDescriptor descriptor = new IndexDescriptor( 123, 456 );

        ConstraintIndexCreator creator = new ConstraintIndexCreator( transactor, indexingService );

        // when
        creator.dropUniquenessConstraintIndex( descriptor );

        // then
        verifyZeroInteractions( indexingService );
        verify( indexDestructionTransaction.schemaWriteOperations() ).uniqueIndexDrop( state, descriptor );
        verifyNoMoreInteractions( indexDestructionTransaction.schemaWriteOperations() );
    }

    private static class StubTransactor extends Transactor
    {
        private final Iterator<StatementOperationParts> mockContexts;
        private final StatementState state;

        StubTransactor( StatementState state, StatementOperationParts... mockContexts )
        {
            super( null );
            this.state = state;
            this.mockContexts = asList( mockContexts ).iterator();
        }

        @Override
        public <RESULT, FAILURE extends KernelException> RESULT execute(
                Statement<RESULT, FAILURE> statement ) throws FAILURE
        {
            return statement.perform( mockContexts.next(), state );
        }
    }
}
