/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.v44.runtime;

import org.neo4j.bolt.runtime.statemachine.BoltStateMachineState;
import org.neo4j.bolt.runtime.statemachine.StateMachineContext;
import org.neo4j.bolt.transaction.TransactionNotFoundException;
import org.neo4j.exceptions.KernelException;

public class InTransactionState extends org.neo4j.bolt.v4.runtime.InTransactionState
{

    @Override
    protected BoltStateMachineState processCommitMessage( StateMachineContext context ) throws KernelException, TransactionNotFoundException
    {
        try
        {
            return super.processCommitMessage( context );
        }
        finally
        {
            context.impersonateUser( null );
        }
    }

    @Override
    protected BoltStateMachineState processRollbackMessage( StateMachineContext context ) throws KernelException, TransactionNotFoundException
    {
        try
        {
            return super.processRollbackMessage( context );
        }
        finally
        {
            context.impersonateUser( null );
        }
    }
}
