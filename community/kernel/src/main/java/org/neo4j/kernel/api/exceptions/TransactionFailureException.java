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
package org.neo4j.kernel.api.exceptions;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;

import static org.neo4j.helpers.Exceptions.withCause;

/**
 * This class (in its current form - 2013-05-07) is a vector for exceptions thrown by a transaction manager, for
 * carrying the exception through the Kernel API stack to be rethrown on a higher level.
 *
 * The intention is that when the dependency on a transaction manager is gone, this class will either disappear, or
 * change into something completely different. Most likely this different thing will emerge alongside this exception
 * type while the transaction system is being refactored, and thus this class will disappear.
 */
public class TransactionFailureException extends TransactionalException
{
    private final Rethrow rethrow;

    public TransactionFailureException( HeuristicMixedException cause )
    {
        super( cause );
        rethrow = Rethrow.HEURISTIC_MIXED;
    }

    public TransactionFailureException( HeuristicRollbackException cause )
    {
        super( cause );
        rethrow = Rethrow.HEURISTIC_ROLLBACK;
    }

    public TransactionFailureException( RollbackException cause )
    {
        super( cause );
        rethrow = Rethrow.ROLLBACK;
    }

    public TransactionFailureException( SystemException cause )
    {
        super( cause );
        rethrow = Rethrow.SYSTEM;
    }

    public TransactionFailureException( Exception e )
    {
        super(e);
        if( e instanceof HeuristicMixedException )
        {
            rethrow = Rethrow.HEURISTIC_MIXED;
        }
        else if( e instanceof HeuristicRollbackException )
        {
            rethrow = Rethrow.HEURISTIC_ROLLBACK;
        }
        else if( e instanceof  RollbackException )
        {
            rethrow = Rethrow.ROLLBACK;
        }
        else if( e instanceof  SystemException )
        {
            rethrow = Rethrow.SYSTEM;
        }
        else if( e instanceof RuntimeException )
        {
            rethrow = Rethrow.RUNTIME;
        }
        else
        {
            rethrow = Rethrow.RUNTIME_WRAPPED;
        }
    }

    public TransactionFailureException( RuntimeException cause )
    {
        super( cause );
        rethrow = Rethrow.RUNTIME;
    }

    public RuntimeException unBoxedForCommit() throws XAException
    {
        return rethrow.exception( this.getCause() );
    }

    public RuntimeException unBoxedForRollback() throws XAException
    {
        return rethrow.exceptionForRollback( this.getCause() );
    }

    private enum Rethrow
    {
        HEURISTIC_MIXED( XAException.XA_HEURMIX ),
        HEURISTIC_ROLLBACK( XAException.XA_HEURRB ),
        ROLLBACK( XAException.XA_RBROLLBACK ),
        SYSTEM( 0 ),
        RUNTIME( 0 )
        {
            @Override
            RuntimeException exception( Throwable exception )
            {
                return (RuntimeException) exception;
            }

            @Override
            RuntimeException exceptionForRollback( Throwable exception )
            {
                return (RuntimeException) exception;
            }
        },
        RUNTIME_WRAPPED( 0 )
        {
            @Override
            RuntimeException exception( Throwable exception )
            {
                return new RuntimeException(exception);
            }

            @Override
            RuntimeException exceptionForRollback( Throwable exception )
            {
                return new RuntimeException(exception);
            }
        };

        int errorCodeOnRollback;

        private Rethrow( int errorCodeOnRollback )
        {
            this.errorCodeOnRollback = errorCodeOnRollback;
        }

        RuntimeException exception( Throwable exception ) throws XAException
        {
            throw withCause( new XAException( errorCodeOnRollback ), exception );
        }

        RuntimeException exceptionForRollback( Throwable exception ) throws XAException
        {
            throw withCause( new XAException( errorCodeOnRollback ), exception );
        }
    }
}
