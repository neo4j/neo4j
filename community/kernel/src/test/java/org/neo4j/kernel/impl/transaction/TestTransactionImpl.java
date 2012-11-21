/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;

import org.junit.Test;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.util.MultipleCauseException;

public class TestTransactionImpl
{
    @Test
    public void shouldBeAbleToAccessAllExceptionsOccurringInSynchronizationsBeforeCompletion()
            throws IllegalStateException, RollbackException
    {
        TxManager mockedTxManager = mock( TxManager.class );
        TransactionImpl tx = new TransactionImpl( mockedTxManager, ForceMode.forced, TransactionState.NO_STATE );

        // Evil synchronizations
        final RuntimeException firstException = new RuntimeException( "Ex1" );
        Synchronization meanSync1 = new Synchronization()
        {

            @Override
            public void beforeCompletion()
            {
                throw firstException;
            }

            @Override
            public void afterCompletion( int status )
            {
            }
        };
        final RuntimeException secondException = new RuntimeException( "Ex1" );
        Synchronization meanSync2 = new Synchronization()
        {

            @Override
            public void beforeCompletion()
            {
                throw secondException;
            }

            @Override
            public void afterCompletion( int status )
            {
            }
        };

        tx.registerSynchronization( meanSync1 );
        tx.registerSynchronization( meanSync2 );
        tx.doBeforeCompletion();

        assertThat( tx.getRollbackCause(), is( MultipleCauseException.class ) );

        MultipleCauseException error = (MultipleCauseException) tx.getRollbackCause();
        assertThat( error.getCause(), is( (Throwable) firstException ) );
        assertThat( error.getCauses().size(), is( 2 ) );
        assertThat( error.getCauses().get( 0 ), is( (Throwable) firstException ) );
        assertThat( error.getCauses().get( 1 ), is( (Throwable) secondException ) );

    }

    @Test
    public void shouldNotThrowMultipleCauseIfOnlyOneErrorOccursInBeforeCompletion() throws IllegalStateException,
            RollbackException
    {
        TxManager mockedTxManager = mock( TxManager.class );
        TransactionImpl tx = new TransactionImpl( mockedTxManager, ForceMode.forced, TransactionState.NO_STATE );

        // Evil synchronizations
        final RuntimeException firstException = new RuntimeException( "Ex1" );
        Synchronization meanSync1 = new Synchronization()
        {

            @Override
            public void beforeCompletion()
            {
                throw firstException;
            }

            @Override
            public void afterCompletion( int status )
            {
            }
        };

        tx.registerSynchronization( meanSync1 );
        tx.doBeforeCompletion();

        assertThat( tx.getRollbackCause(), is( (Throwable) firstException ) );
    }
}
