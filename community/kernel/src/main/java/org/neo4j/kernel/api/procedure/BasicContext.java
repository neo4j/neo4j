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

import static java.util.Objects.requireNonNull;

import java.time.Clock;
import java.util.function.Function;
import java.util.function.Supplier;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.newapi.ProcedureTransactionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.ValueMapper;

public class BasicContext implements Context {
    private final DependencyResolver resolver;
    private final KernelTransaction kernelTransaction;
    private final SecurityContext securityContext;
    private final ClockContext clockContext;
    private final ValueMapper<Object> valueMapper;
    private final Thread thread;
    private final ProcedureCallContext procedureCallContext;

    private final URLAccessChecker urlAccessChecker;

    private final Supplier<GraphDatabaseAPI> graphDatabaseAPISupplier;

    private BasicContext(
            DependencyResolver resolver,
            KernelTransaction kernelTransaction,
            SecurityContext securityContext,
            ClockContext clockContext,
            ValueMapper<Object> valueMapper,
            Thread thread,
            ProcedureCallContext procedureCallContext,
            URLAccessChecker urlAccessChecker,
            Supplier<GraphDatabaseAPI> graphDatabaseAPISupplier) {
        this.resolver = resolver;
        this.kernelTransaction = kernelTransaction;
        this.securityContext = securityContext;
        this.clockContext = clockContext;
        this.valueMapper = valueMapper;
        this.thread = thread;
        this.procedureCallContext = procedureCallContext;
        this.urlAccessChecker = urlAccessChecker;
        this.graphDatabaseAPISupplier = graphDatabaseAPISupplier;
    }

    @Override
    public ValueMapper<Object> valueMapper() {
        return valueMapper;
    }

    @Override
    public SecurityContext securityContext() {
        return securityContext;
    }

    @Override
    public DependencyResolver dependencyResolver() {
        return resolver;
    }

    @Override
    public GraphDatabaseAPI graphDatabaseAPI() {
        return graphDatabaseAPISupplier.get();
    }

    @Override
    public Thread thread() {
        return thread;
    }

    @Override
    public Transaction transaction() throws ProcedureException {
        return new ProcedureTransactionImpl(internalTransaction());
    }

    @Override
    public InternalTransaction internalTransaction() throws ProcedureException {
        return throwIfNull("Transaction", kernelTransaction, KernelTransaction::internalTransaction);
    }

    @Override
    public InternalTransaction internalTransactionOrNull() {
        return kernelTransaction == null ? null : kernelTransaction.internalTransaction();
    }

    @Override
    public KernelTransaction kernelTransaction() throws ProcedureException {
        return throwIfNull("KernelTransaction", kernelTransaction);
    }

    @Override
    public Clock systemClock() throws ProcedureException {
        return throwIfNull("SystemClock", clockContext, ClockContext::systemClock);
    }

    @Override
    public Clock statementClock() throws ProcedureException {
        return throwIfNull("StatementClock", clockContext, ClockContext::statementClock);
    }

    @Override
    public Clock transactionClock() throws ProcedureException {
        return throwIfNull("TransactionClock", clockContext, ClockContext::transactionClock);
    }

    @Override
    public URLAccessChecker urlAccessChecker() throws ProcedureException {
        return throwIfNull("URLAccessChecker", urlAccessChecker);
    }

    @Override
    public ProcedureCallContext procedureCallContext() {
        return procedureCallContext;
    }

    public static ContextBuilder buildContext(DependencyResolver dependencyResolver, ValueMapper<Object> valueMapper) {
        return new ContextBuilder(dependencyResolver, valueMapper);
    }

    @SuppressWarnings("unchecked")
    private static <T, U> T throwIfNull(String name, U value) throws ProcedureException {
        return throwIfNull(name, value, v -> (T) v);
    }

    private static <T, U> T throwIfNull(String name, U value, Function<U, T> producer) throws ProcedureException {
        if (value == null) {
            throw new ProcedureException(
                    Status.Procedure.ProcedureCallFailed,
                    "There is no `%s` in the current procedure call context.",
                    name);
        }
        return producer.apply(value);
    }

    public static class ContextBuilder {
        private final DependencyResolver resolver;
        private final Thread thread = Thread.currentThread();
        private final ValueMapper<Object> valueMapper;
        private KernelTransaction procedureTransaction;
        private SecurityContext securityContext = SecurityContext.AUTH_DISABLED;
        private ClockContext clockContext;
        private ProcedureCallContext procedureCallContext;
        private URLAccessChecker urlAccessChecker;
        private Supplier<GraphDatabaseAPI> graphDatabaseAPISupplier;

        private ContextBuilder(DependencyResolver resolver, ValueMapper<Object> valueMapper) {
            this.resolver = resolver;
            this.valueMapper = valueMapper;
        }

        public ContextBuilder withKernelTransaction(KernelTransaction procedureTransaction) {
            this.procedureTransaction = procedureTransaction;
            return this;
        }

        public ContextBuilder withGraphDatabaseSupplier(Supplier<GraphDatabaseAPI> graphDatabaseAPISupplier) {
            this.graphDatabaseAPISupplier = graphDatabaseAPISupplier;
            return this;
        }

        public ContextBuilder withSecurityContext(SecurityContext securityContext) {
            this.securityContext = securityContext;
            return this;
        }

        public ContextBuilder withClock(ClockContext clockContext) {
            this.clockContext = clockContext;
            return this;
        }

        public ContextBuilder withProcedureCallContext(ProcedureCallContext procedureContext) {
            this.procedureCallContext = procedureContext;
            return this;
        }

        public ContextBuilder withUrlAccessChecker(URLAccessChecker urlAccessChecker) {
            this.urlAccessChecker = urlAccessChecker;
            return this;
        }

        public Context context() {
            requireNonNull(resolver);
            requireNonNull(securityContext);
            requireNonNull(valueMapper);
            requireNonNull(thread);
            return new BasicContext(
                    resolver,
                    procedureTransaction,
                    securityContext,
                    clockContext,
                    valueMapper,
                    thread,
                    procedureCallContext,
                    urlAccessChecker,
                    graphDatabaseAPISupplier);
        }
    }
}
