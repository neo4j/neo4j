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
package org.neo4j.kernel.api;

import static org.neo4j.helpers.Exceptions.withCause;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;

public class TransactionFailureException extends Exception
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

    public TransactionFailureException( RuntimeException cause )
    {
        super( cause );
        rethrow = Rethrow.RUNTIME;
    }

    public RuntimeException unBoxedForCommit()
            throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SystemException
    {
        return rethrow.exception( this.getCause() );
    }
    
    public RuntimeException unBoxedForRollback() throws SystemException
    {
        try
        {
            return rethrow.exception( this.getCause() );
        }
        catch ( HeuristicMixedException e )
        {
            throw withCause( new SystemException( rethrow.xaCode ), e );
        }
        catch ( HeuristicRollbackException e )
        {
            throw withCause( new SystemException( rethrow.xaCode ), e );
        }
        catch ( RollbackException e )
        {
            throw withCause( new SystemException( rethrow.xaCode ), e );
        }
    }
    
    private enum Rethrow
    {
        HEURISTIC_MIXED( XAException.XA_HEURMIX )
        {
            @Override
            RuntimeException exception( Throwable exception ) throws HeuristicMixedException
            {
                throw (HeuristicMixedException) exception;
            }
        },
        HEURISTIC_ROLLBACK( XAException.XA_HEURRB )
        {
            @Override
            RuntimeException exception( Throwable exception ) throws HeuristicRollbackException
            {
                throw (HeuristicRollbackException) exception;
            }
        },
        ROLLBACK( XAException.XA_RBROLLBACK )
        {
            @Override
            RuntimeException exception( Throwable exception ) throws RollbackException
            {
                throw (RollbackException) exception;
            }
        },
        SYSTEM( XAException.XAER_RMERR )
        {
            @Override
            RuntimeException exception( Throwable exception ) throws SystemException
            {
                throw (SystemException) exception;
            }
        },
        RUNTIME( XAException.XAER_RMERR )
        {
            @Override
            RuntimeException exception( Throwable exception )
            {
                return (RuntimeException) exception;
            }
        };
        
        int xaCode;
        
        private Rethrow( int xaCode )
        {
            this.xaCode = xaCode;
        }

        abstract RuntimeException exception( Throwable exception )
                throws HeuristicMixedException, HeuristicRollbackException, RollbackException, SystemException;
    }
}
