/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.procedure;

import java.time.Clock;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.ValueMapper;

/**
 * The context in which a procedure is invoked. This is a read-only structure.
 * For instance, a read-only transactional procedure might have access to the current statement it is being invoked
 * in through this.
 *
 * The context is entirely defined by the caller of the procedure,
 * so what is available in the context depends on the context of the call.
 */
public interface Context {
    /**
     * Returns the value mapper of this context.
     * <p>
     * This method is always safe to call, there should always be a value mapper associated with the context.
     *
     * @return the value mapper of this context
     */
    ValueMapper<Object> valueMapper();

    /**
     * Returns the security context of this context.
     * <p>
     * This method is always safe to call, there should always be a security context associated with the context.
     *
     * @return the security context of this context
     */
    SecurityContext securityContext();

    /**
     * Returns the dependency resolver of this context.
     * <p>
     * This method is always safe to call, there should always be a dependency resolver associated with the context.
     *
     * @return the dependency resolver of this context
     */
    DependencyResolver dependencyResolver();

    /**
     * Returns the graphdatabase API of this context.
     *
     * @return the graphdatabase API of this context
     */
    GraphDatabaseAPI graphDatabaseAPI();

    /**
     * Returns the thread of this context.
     * <p>
     * This method is always safe to call, there should always be a thread associated with the context.
     *
     * @return the thread of this context
     */
    Thread thread();

    /**
     * Returns the public API transaction that should be used by the procedure and function authors.
     * Such transaction has typically some functionality like commit or rollback disabled, as they are not supported
     * from procedures and functions.
     * In this way it differs from {@link #internalTransaction()} and {@link #internalTransactionOrNull()}
     * that apart from returning internal API transaction implementing {@link InternalTransaction} also return
     * a transaction representation that does not have such operations disabled.
     *
     * @throws ProcedureException if no transaction has been associated with the context
     */
    Transaction transaction() throws ProcedureException;

    /**
     * Returns the internal transaction of this context
     * <p>
     * If no transaction has been associated with this context this method will throw a {@link ProcedureException}
     *
     * @return the internal transaction of this context
     * @throws ProcedureException if no transaction has been associated with the context
     */
    InternalTransaction internalTransaction() throws ProcedureException;

    /**
     * @return The internal transaction of this context or <code>null</code> if no transaction has been associated with
     * the context.
     */
    InternalTransaction internalTransactionOrNull();

    /**
     * Returns the kernel transaction of this context
     * <p>
     * If no transaction has been associated with this context this method will throw a {@link ProcedureException}
     *
     * @return the internal transaction of this context
     * @throws ProcedureException if no transaction has been associated with the context
     */
    KernelTransaction kernelTransaction() throws ProcedureException;

    /**
     * Returns the system clock of this context
     * <p>
     * If no clock has been associated with this context this method will throw a {@link ProcedureException}
     *
     * @return the system clock of this context
     * @throws ProcedureException if no clock has been associated with the context
     */
    Clock systemClock() throws ProcedureException;

    /**
     * Returns the statement clock of this context
     * <p>
     * If no clock has been associated with this context this method will throw a {@link ProcedureException}
     *
     * @return the statement clock of this context
     * @throws ProcedureException if no clock has been associated with the context
     */
    Clock statementClock() throws ProcedureException;

    /**
     * Returns the transaction clock of this context
     * <p>
     * If no clock has been associated with this context this method will throw a {@link ProcedureException}
     *
     * @return the transaction clock of this context
     * @throws ProcedureException if no clock has been associated with the context
     */
    Clock transactionClock() throws ProcedureException;

    /**
     * Returns a url access checker to validate URLs against the current security rules
     * <p>
     * If no such checker has been associated with this context this method will throw a {@link ProcedureException}
     *
     * @return the url access checker of this context
     * @throws ProcedureException if no clock has been associated with the context
     */
    URLAccessChecker urlAccessChecker() throws ProcedureException;

    /**
     * Returns the procedure call context of this context.
     * <p>
     * This method is always safe to call, there should always be a procedure call context associated with the context.
     *
     * @return the procedure call context of this context
     */
    ProcedureCallContext procedureCallContext();
}
