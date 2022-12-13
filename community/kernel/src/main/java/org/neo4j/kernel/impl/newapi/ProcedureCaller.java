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
package org.neo4j.kernel.impl.newapi;

import static java.lang.String.format;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;

import java.time.Clock;
import java.util.function.Supplier;
import org.neo4j.collection.RawIterator;
import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserAggregationUpdater;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AdminAccessMode;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.api.OverridableSecurityContext;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextValueMapper;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;

public abstract class ProcedureCaller {

    final GlobalProcedures globalProcedures;
    private final DependencyResolver databaseDependencies;

    private ProcedureCaller(GlobalProcedures globalProcedures, DependencyResolver databaseDependencies) {
        this.globalProcedures = globalProcedures;
        this.databaseDependencies = databaseDependencies;
    }

    public AnyValue callFunction(int id, AnyValue[] input) throws ProcedureException {
        performCheckBeforeOperation();

        AccessMode mode = securityContext().mode();
        if (!mode.allowsExecuteFunction(id).allowsAccess()) {
            String message = format(
                    "Executing a user defined function is not allowed for %s.",
                    securityContext().description());
            throw securityAuthorizationHandler().logAndGetAuthorizationException(securityContext(), message);
        }

        final SecurityContext securityContext = mode.shouldBoostFunction(id).allowsAccess()
                ? securityContext().withMode(new OverriddenAccessMode(mode, AccessMode.Static.READ))
                : securityContext().withMode(new RestrictedAccessMode(mode, AccessMode.Static.READ));

        try (var ignore = overrideSecurityContext(securityContext)) {
            return globalProcedures.callFunction(
                    prepareContext(securityContext, ProcedureCallContext.EMPTY), id, input);
        }
    }

    public AnyValue callBuiltInFunction(int id, AnyValue[] input) throws ProcedureException {
        performCheckBeforeOperation();
        return globalProcedures.callFunction(prepareContext(securityContext(), ProcedureCallContext.EMPTY), id, input);
    }

    AccessMode checkAggregationFunctionAccessMode(int functionId) {
        AccessMode mode = securityContext().mode();
        if (!mode.allowsExecuteAggregatingFunction(functionId).allowsAccess()) {
            String message = format(
                    "Executing a user defined aggregating function is not allowed for %s.",
                    securityContext().description());
            throw securityAuthorizationHandler().logAndGetAuthorizationException(securityContext(), message);
        }
        return mode;
    }

    UserAggregationReducer createGenericAggregator(boolean overrideAccessMode, AccessMode mode, int functionId)
            throws ProcedureException {
        final SecurityContext securityContext = overrideAccessMode
                ? securityContext().withMode(new OverriddenAccessMode(mode, AccessMode.Static.READ))
                : securityContext().withMode(new RestrictedAccessMode(mode, AccessMode.Static.READ));

        try (var ignore = overrideSecurityContext(securityContext)) {
            UserAggregationReducer aggregator = globalProcedures.createAggregationFunction(
                    prepareContext(securityContext, ProcedureCallContext.EMPTY), functionId);
            return new UserAggregationReducer() {
                @Override
                public UserAggregationUpdater newUpdater() throws ProcedureException {
                    try (var ignore = overrideSecurityContext(securityContext)) {
                        UserAggregationUpdater updater = aggregator.newUpdater();
                        return new UserAggregationUpdater() {
                            @Override
                            public void update(AnyValue[] input) throws ProcedureException {
                                try (var ignore = overrideSecurityContext(securityContext)) {
                                    updater.update(input);
                                }
                            }

                            @Override
                            public void applyUpdates() throws ProcedureException {
                                try (var ignore = overrideSecurityContext(securityContext)) {
                                    updater.applyUpdates();
                                }
                            }
                        };
                    }
                }

                @Override
                public AnyValue result() throws ProcedureException {
                    try (var ignore = overrideSecurityContext(securityContext)) {
                        return aggregator.result();
                    }
                }
            };
        }
    }

    public UserAggregationReducer createBuiltInAggregationFunction(int id) throws ProcedureException {
        performCheckBeforeOperation();

        return globalProcedures.createAggregationFunction(
                prepareContext(securityContext(), ProcedureCallContext.EMPTY), id);
    }

    Context prepareContext(SecurityContext securityContext, ProcedureCallContext procedureContext) {
        return buildContext(databaseDependencies, createValueMapper())
                .withTransaction(maybeInternalTransaction())
                .withSecurityContext(securityContext)
                .withProcedureCallContext(procedureContext)
                .withClock(clockContext())
                .context();
    }

    abstract SecurityContext securityContext();

    abstract OverridableSecurityContext.Revertable overrideSecurityContext(SecurityContext context);

    abstract InternalTransaction maybeInternalTransaction();

    abstract void performCheckBeforeOperation();

    abstract SecurityAuthorizationHandler securityAuthorizationHandler();

    abstract ClockContext clockContext();

    abstract ValueMapper<Object> createValueMapper();

    public abstract UserAggregationReducer createAggregationFunction(int id) throws ProcedureException;

    public abstract RawIterator<AnyValue[], ProcedureException> callProcedure(
            int id, AnyValue[] input, final AccessMode.Static procedureMode, ProcedureCallContext procedureCallContext)
            throws ProcedureException;

    public static class ForTransactionScope extends ProcedureCaller {

        private final KernelTransaction ktx;

        public ForTransactionScope(
                KernelTransaction ktx, GlobalProcedures globalProcedures, DependencyResolver databaseDependencies) {
            super(globalProcedures, databaseDependencies);

            this.ktx = ktx;
        }

        @Override
        void performCheckBeforeOperation() {
            ktx.assertOpen();
        }

        @Override
        SecurityAuthorizationHandler securityAuthorizationHandler() {
            return ktx.securityAuthorizationHandler();
        }

        @Override
        ClockContext clockContext() {
            // This needs to be lazy, because transactions from virtual databases don't have clock context.
            return new ClockContext() {

                @Override
                public Clock systemClock() {
                    return ktx.clocks().systemClock();
                }

                @Override
                public Clock transactionClock() {
                    return ktx.clocks().transactionClock();
                }

                @Override
                public Clock statementClock() {
                    return ktx.clocks().statementClock();
                }
            };
        }

        @Override
        ValueMapper<Object> createValueMapper() {
            return new DefaultValueMapper(ktx.internalTransaction());
        }

        @Override
        public UserAggregationReducer createAggregationFunction(int id) throws ProcedureException {
            performCheckBeforeOperation();
            AccessMode mode = checkAggregationFunctionAccessMode(id);
            boolean overrideAccessMode = mode.shouldBoostAggregatingFunction(id).allowsAccess();
            return createGenericAggregator(overrideAccessMode, mode, id);
        }

        @Override
        SecurityContext securityContext() {
            return ktx.securityContext();
        }

        @Override
        OverridableSecurityContext.Revertable overrideSecurityContext(SecurityContext context) {
            KernelTransaction.Revertable revertable = ktx.overrideWith(context);
            return revertable::close;
        }

        @Override
        InternalTransaction maybeInternalTransaction() {
            return ktx.internalTransaction();
        }

        @Override
        public RawIterator<AnyValue[], ProcedureException> callProcedure(
                int id,
                AnyValue[] input,
                final AccessMode.Static procedureMode,
                ProcedureCallContext procedureCallContext)
                throws ProcedureException {
            ktx.assertOpen();

            AccessMode mode = ktx.securityContext().mode();
            if (!mode.allowsExecuteProcedure(id).allowsAccess()) {
                String message = format(
                        "Executing procedure is not allowed for %s.",
                        ktx.securityContext().description());
                throw ktx.securityAuthorizationHandler()
                        .logAndGetAuthorizationException(ktx.securityContext(), message);
            }

            final SecurityContext procedureSecurityContext =
                    mode.shouldBoostProcedure(id).allowsAccess()
                            ? ktx.securityContext()
                                    .withMode(new OverriddenAccessMode(mode, procedureMode))
                                    .withMode(AdminAccessMode.FULL)
                            : ktx.securityContext().withMode(new RestrictedAccessMode(mode, procedureMode));

            final RawIterator<AnyValue[], ProcedureException> procedureCall;
            try (KernelTransaction.Revertable ignore = ktx.overrideWith(procedureSecurityContext);
                    Statement statement = ktx.acquireStatement()) {
                procedureCall = globalProcedures.callProcedure(
                        prepareContext(procedureSecurityContext, procedureCallContext), id, input, statement);
            }
            return createIterator(procedureSecurityContext, procedureCall);
        }

        private RawIterator<AnyValue[], ProcedureException> createIterator(
                SecurityContext procedureSecurityContext, RawIterator<AnyValue[], ProcedureException> procedureCall) {
            return new RawIterator<>() {
                @Override
                public boolean hasNext() throws ProcedureException {
                    try (KernelTransaction.Revertable ignore = ktx.overrideWith(procedureSecurityContext)) {
                        return procedureCall.hasNext();
                    }
                }

                @Override
                public AnyValue[] next() throws ProcedureException {
                    try (KernelTransaction.Revertable ignore = ktx.overrideWith(procedureSecurityContext)) {
                        return procedureCall.next();
                    }
                }
            };
        }
    }

    public static class ForThreadExecutionContextScope extends ProcedureCaller {

        private final ExecutionContext executionContext;
        private final OverridableSecurityContext overridableSecurityContext;
        private final AssertOpen assertOpen;
        private final SecurityAuthorizationHandler securityAuthorizationHandler;
        private final Supplier<ClockContext> clockContextSupplier;

        ForThreadExecutionContextScope(
                ExecutionContext executionContext,
                GlobalProcedures globalProcedures,
                DependencyResolver databaseDependencies,
                OverridableSecurityContext overridableSecurityContext,
                AssertOpen assertOpen,
                SecurityAuthorizationHandler securityAuthorizationHandler,
                Supplier<ClockContext> clockContextSupplier) {
            super(globalProcedures, databaseDependencies);

            this.executionContext = executionContext;
            this.overridableSecurityContext = overridableSecurityContext;
            this.assertOpen = assertOpen;
            this.securityAuthorizationHandler = securityAuthorizationHandler;
            this.clockContextSupplier = clockContextSupplier;
        }

        @Override
        public UserAggregationReducer createAggregationFunction(int id) throws ProcedureException {
            performCheckBeforeOperation();
            AccessMode mode = checkAggregationFunctionAccessMode(id);
            // The FULL access mode returns true on all shouldBoost-calls,
            // but it doesn't need any boost here since it already supports all read operations.
            boolean overrideAccessMode = mode != AccessMode.Static.FULL
                    && mode.shouldBoostAggregatingFunction(id).allowsAccess();
            if (overrideAccessMode) {
                return createGenericAggregator(true, mode, id);
            } else {
                // Generally, functions have the access mode restricted to READ during their invocation.
                // That is actually a quite expensive operation to do for every update call of an aggregation function.
                // Since only read operations are currently supported during parallel execution,
                // the expensive access mode restricting is not needed for execution context API.
                return globalProcedures.createAggregationFunction(
                        prepareContext(securityContext(), ProcedureCallContext.EMPTY), id);
            }
        }

        @Override
        SecurityContext securityContext() {
            return overridableSecurityContext.currentSecurityContext();
        }

        @Override
        OverridableSecurityContext.Revertable overrideSecurityContext(SecurityContext context) {
            return overridableSecurityContext.overrideWith(context);
        }

        @Override
        InternalTransaction maybeInternalTransaction() {
            return null;
        }

        @Override
        void performCheckBeforeOperation() {
            assertOpen.assertOpen();
        }

        @Override
        SecurityAuthorizationHandler securityAuthorizationHandler() {
            return securityAuthorizationHandler;
        }

        @Override
        ClockContext clockContext() {
            return clockContextSupplier.get();
        }

        @Override
        ValueMapper<Object> createValueMapper() {
            return new ExecutionContextValueMapper(executionContext);
        }

        @Override
        public RawIterator<AnyValue[], ProcedureException> callProcedure(
                int id, AnyValue[] input, AccessMode.Static procedureMode, ProcedureCallContext procedureCallContext)
                throws ProcedureException {
            throw new UnsupportedOperationException("Invoking a procedure is not allowed during parallel execution");
        }
    }
}
