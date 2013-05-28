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

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
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

public class ConstraintIndexCreatorTest
{
    @Test
    public void shouldCreateIndexInAnotherTransaction() throws Exception
    {
        // given
        StatementContext constraintCreationContext = mock( StatementContext.class );
        StatementContext indexCreationContext = mock( StatementContext.class );

        IndexDescriptor descriptor = new IndexDescriptor( 123, 456 );
        when( indexCreationContext.uniqueIndexCreate( 123, 456 ) ).thenReturn( descriptor );

        IndexingService indexingService = mock( IndexingService.class );
        StubTransactor transactor = new StubTransactor( indexCreationContext );

        when( constraintCreationContext.indexGetCommittedId( descriptor ) ).thenReturn( 2468l );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getProxyForRule( 2468l ) ).thenReturn( indexProxy );

        ConstraintIndexCreator creator = new ConstraintIndexCreator( transactor, indexingService );

        // when
        long indexId = creator.createUniquenessConstraintIndex( constraintCreationContext, 123, 456 );

        // then
        assertEquals( 2468l, indexId );
        verify( indexCreationContext ).uniqueIndexCreate( 123, 456 );
        verifyNoMoreInteractions( indexCreationContext );
        verify( constraintCreationContext ).indexGetCommittedId( descriptor );
        verifyNoMoreInteractions( constraintCreationContext );
        verify( indexProxy ).awaitStoreScanCompleted();
    }

    @Test
    public void shouldDropIndexIfPopulationFails() throws Exception
    {
        // given
        StatementContext constraintCreationContext = mock( StatementContext.class );
        StatementContext indexCreationContext = mock( StatementContext.class );
        StatementContext indexDestructionContext = mock( StatementContext.class );

        IndexDescriptor descriptor = new IndexDescriptor( 123, 456 );
        when( indexCreationContext.uniqueIndexCreate( 123, 456 ) ).thenReturn( descriptor );

        IndexingService indexingService = mock( IndexingService.class );
        StubTransactor transactor = new StubTransactor( indexCreationContext, indexDestructionContext );

        when( constraintCreationContext.indexGetCommittedId( descriptor ) ).thenReturn( 2468l );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexingService.getProxyForRule( 2468l ) ).thenReturn( indexProxy );
        doThrow( new IndexPopulationFailedKernelException( descriptor, new PreexistingIndexEntryConflictException( "a", 2, 1 ) ) )
                .when( indexProxy ).awaitStoreScanCompleted();

        ConstraintIndexCreator creator = new ConstraintIndexCreator( transactor, indexingService );

        // when
        try
        {
            creator.createUniquenessConstraintIndex( constraintCreationContext, 123, 456 );

            fail( "expected exception" );
        }
        // then
        catch ( ConstraintVerificationFailedKernelException e )
        {
            assertEquals( "Existing data does not satisfy CONSTRAINT ON ( n:label[123] ) ASSERT n.property[456] IS UNIQUE.",
                          e.getMessage() );
        }
        verify( indexCreationContext ).uniqueIndexCreate( 123, 456 );
        verifyNoMoreInteractions( indexCreationContext );
        verify( constraintCreationContext ).indexGetCommittedId( descriptor );
        verifyNoMoreInteractions( constraintCreationContext );
        verify( indexDestructionContext ).uniqueIndexDrop( descriptor );
        verifyNoMoreInteractions( indexDestructionContext );
    }

    @Test
    public void shouldDropIndexInAnotherTransaction() throws Exception
    {
        // given
        StatementContext indexDestructionTransaction = mock( StatementContext.class );
        StubTransactor transactor = new StubTransactor( indexDestructionTransaction );
        IndexingService indexingService = mock( IndexingService.class );

        IndexDescriptor descriptor = new IndexDescriptor( 123, 456 );

        ConstraintIndexCreator creator = new ConstraintIndexCreator( transactor, indexingService );

        // when
        creator.dropUniquenessConstraintIndex( descriptor );

        // then
        verifyZeroInteractions( indexingService );
        verify( indexDestructionTransaction ).uniqueIndexDrop( descriptor );
        verifyNoMoreInteractions( indexDestructionTransaction );
    }

    private static class StubTransactor extends Transactor
    {
        private final Iterator<StatementContext> mockContexts;

        StubTransactor( StatementContext... mockContexts )
        {
            super( null );
            this.mockContexts = asList( mockContexts ).iterator();
        }

        @Override
        public <RESULT, FAILURE extends KernelException> RESULT execute( Statement<RESULT, FAILURE> statement )
                throws FAILURE
        {
            return statement.perform( mockContexts.next() );
        }
    }
}
