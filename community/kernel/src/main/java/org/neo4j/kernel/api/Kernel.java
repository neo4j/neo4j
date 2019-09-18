/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;

/**
 * The main API through which access to the Neo4j kernel is made, both read
 * and write operations are supported as well as creating transactions.
 *
 * Changes to the graph (i.e. write operations) are performed via a
 * {@link #beginTransaction(KernelTransaction.Type, LoginContext)}  transaction context} where changes done
 * inside the transaction are visible in read operations for {@link Statement statements}
 * executed within that transaction context.
 */
public interface Kernel
{
    /**
     * Creates and returns a new {@link KernelTransaction} capable of modifying the
     * underlying graph with custom timeout in milliseconds.
     *
     * @param type the type of the new transaction: implicit (internally created) or explicit (created by the user)
     * @param loginContext transaction login context
     * @param clientInfo transaction client info
     * @param timeout transaction timeout in milliseconds
     */
    KernelTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo, long timeout )
            throws TransactionFailureException;

    /**
     * Begin new transaction.
     *
     * @param type type of transaction (implicit/explicit)
     * @param loginContext the {@link LoginContext} of the user which is beginning this transaction
     * @param clientInfo {@link ClientConnectionInfo} of the user which is beginning this transaction
     * @return the transaction
     */
    KernelTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo )
            throws TransactionFailureException;

    /**
     * Begin new transaction.
     *
     * @param type type of transaction (implicit/explicit)
     * @param loginContext the {@link LoginContext} of the user which is beginning this transaction
     * @return the transaction
     */
    KernelTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext )
            throws TransactionFailureException;

    /**
     * Register a procedure that should be available from this kernel. This is not a transactional method, the procedure is not
     * durably stored, and is not propagated in a cluster.
     *
     * @param procedure procedure to register
     */
    void registerProcedure( CallableProcedure procedure ) throws ProcedureException;

    /**
     * Register a function that should be available from this kernel. This is not a transactional method, the function is not
     * durably stored, and is not propagated in a cluster.
     *
     * @param function function to register
     */
    void registerUserFunction( CallableUserFunction function ) throws ProcedureException;

    /**
     * Register an aggregation function that should be available from this kernel. This is not a transactional method, the function is not
     * durably stored, and is not propagated in a cluster.
     *
     * @param function function to register
     */
    void registerUserAggregationFunction( CallableUserAggregationFunction function ) throws ProcedureException;

    /**
     * Cursor factory which produces cursors that are not bound to any particular transaction.
     */
    CursorFactory cursors();
}
