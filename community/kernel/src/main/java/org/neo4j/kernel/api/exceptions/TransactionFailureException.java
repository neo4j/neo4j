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
    private static final int NO_CODE = 0;
    private final int errorCode;

    public TransactionFailureException( HeuristicMixedException cause )
    {
        super( cause );
        errorCode = XAException.XA_HEURMIX;
    }

    public TransactionFailureException( HeuristicRollbackException cause )
    {
        super( cause );
        errorCode = XAException.XA_HEURRB;
    }

    public TransactionFailureException( RollbackException cause )
    {
        super( cause );
        errorCode = XAException.XA_RBROLLBACK;
    }

    public TransactionFailureException( SystemException cause )
    {
        super( cause );
        errorCode = XAException.XAER_RMERR;
    }

    public TransactionFailureException( Exception e )
    {
        super(e);
        errorCode = NO_CODE;
    }

    public RuntimeException unBoxedForCommit() throws XAException
    {
        Throwable cause = getCause();
        if ( errorCode == NO_CODE)
        {
            return (cause instanceof RuntimeException)? (RuntimeException) cause : new RuntimeException( cause );
        }
        throw withCause( new XAException( errorCode ), cause );
    }
}
