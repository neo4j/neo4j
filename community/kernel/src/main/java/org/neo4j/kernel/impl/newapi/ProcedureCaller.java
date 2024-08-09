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
package org.neo4j.kernel.impl.newapi;

import static java.lang.String.format;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;

import java.util.function.Supplier;
import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserAggregationUpdater;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AdminAccessMode;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.api.OverridableSecurityContext;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextGraphDatabaseAPI;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextProcedureKernelTransaction;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextValueMapper;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.security.ProcedureUrlAccessChecker;
import org.neo4j.kernel.impl.security.URIAccessRules;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;

public abstract class ProcedureCaller {

    final ProcedureView procedureView;
    final DependencyResolver databaseDependencies;

    private ProcedureCaller(DependencyResolver databaseDependencies, ProcedureView procedureView) {
        this.databaseDependencies = databaseDependencies;
        this.procedureView = procedureView;
    }

    public AnyValue callFunction(int id, AnyValue[] input, ProcedureCallContext context) throws ProcedureException {
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
            return procedureView.callFunction(prepareContext(securityContext, context), id, input);
        }
    }

    public AnyValue callBuiltInFunction(int id, AnyValue[] input, ProcedureCallContext context)
            throws ProcedureException {
        performCheckBeforeOperation();
        return procedureView.callFunction(prepareContext(securityContext(), context), id, input);
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

    UserAggregationReducer createGenericAggregator(
            boolean overrideAccessMode, AccessMode mode, int functionId, ProcedureCallContext context)
            throws ProcedureException {
        final SecurityContext securityContext = overrideAccessMode
                ? securityContext().withMode(new OverriddenAccessMode(mode, AccessMode.Static.READ))
                : securityContext().withMode(new RestrictedAccessMode(mode, AccessMode.Static.READ));

        try (var ignore = overrideSecurityContext(securityContext)) {
            UserAggregationReducer aggregator =
                    procedureView.createAggregationFunction(prepareContext(securityContext, context), functionId);
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

    public UserAggregationReducer createBuiltInAggregationFunction(int id, ProcedureCallContext context)
            throws ProcedureException {
        performCheckBeforeOperation();

        return procedureView.createAggregationFunction(prepareContext(securityContext(), context), id);
    }

    Context prepareContext(SecurityContext securityContext, ProcedureCallContext procedureContext) {
        return buildContext(databaseDependencies, createValueMapper())
                .withKernelTransaction(kernelTransaction())
                .withGraphDatabaseSupplier(graphDatabaseAPISupplier())
                .withSecurityContext(securityContext)
                .withProcedureCallContext(procedureContext)
                .withClock(clockContext())
                .withUrlAccessChecker(urlAccessChecker())
                .context();
    }

    public ResourceRawIterator<AnyValue[], ProcedureException> callProcedure(
            int id, AnyValue[] input, AccessMode.Static procedureMode, ProcedureCallContext procedureCallContext)
            throws ProcedureException {
        performCheckBeforeOperation();

        SecurityContext securityContext = securityContext();
        AccessMode mode = securityContext.mode();
        if (!mode.allowsExecuteProcedure(id).allowsAccess()) {
            String message = format("Executing procedure is not allowed for %s.", securityContext.description());
            throw securityAuthorizationHandler().logAndGetAuthorizationException(securityContext, message);
        }

        SecurityContext procedureSecurityContext = mode.shouldBoostProcedure(id).allowsAccess()
                ? securityContext
                        .withMode(new OverriddenAccessMode(mode, procedureMode))
                        .withMode(AdminAccessMode.FULL)
                : securityContext.withMode(new RestrictedAccessMode(mode, procedureMode));

        ResourceRawIterator<AnyValue[], ProcedureException> procedureCall;
        try (var ignore = overrideSecurityContext(procedureSecurityContext)) {
            procedureCall = doCallProcedure(prepareContext(procedureSecurityContext, procedureCallContext), id, input);
        }

        return createIterator(procedureSecurityContext, procedureCall);
    }

    private ResourceRawIterator<AnyValue[], ProcedureException> createIterator(
            SecurityContext procedureSecurityContext,
            ResourceRawIterator<AnyValue[], ProcedureException> procedureCall) {
        return new ResourceRawIterator<>() {
            @Override
            public boolean hasNext() throws ProcedureException {
                try (var ignore = overrideSecurityContext(procedureSecurityContext)) {
                    return procedureCall.hasNext();
                }
            }

            @Override
            public AnyValue[] next() throws ProcedureException {
                try (var ignore = overrideSecurityContext(procedureSecurityContext)) {
                    return procedureCall.next();
                }
            }

            @Override
            public void close() {
                procedureCall.close();
            }
        };
    }

    abstract SecurityContext securityContext();

    abstract OverridableSecurityContext.Revertable overrideSecurityContext(SecurityContext context);

    abstract KernelTransaction kernelTransaction();

    abstract Supplier<GraphDatabaseAPI> graphDatabaseAPISupplier();

    abstract void performCheckBeforeOperation();

    abstract SecurityAuthorizationHandler securityAuthorizationHandler();

    abstract ClockContext clockContext();

    URLAccessChecker urlAccessChecker() {
        return new ProcedureUrlAccessChecker(
                this.databaseDependencies
                        .resolveDependency(URIAccessRules.class)
                        .webAccess(),
                securityAuthorizationHandler(),
                securityContext());
    }

    abstract ValueMapper<Object> createValueMapper();

    public abstract UserAggregationReducer createAggregationFunction(int id, ProcedureCallContext context)
            throws ProcedureException;

    abstract ResourceRawIterator<AnyValue[], ProcedureException> doCallProcedure(Context ctx, int id, AnyValue[] input)
            throws ProcedureException;

    public static class ForTransactionScope extends ProcedureCaller {

        private final KernelTransaction ktx;

        public ForTransactionScope(
                KernelTransaction ktx, DependencyResolver databaseDependencies, ProcedureView procedureView) {
            super(databaseDependencies, procedureView);
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
            return ktx.clocks();
        }

        @Override
        ValueMapper<Object> createValueMapper() {
            return new DefaultValueMapper(ktx.internalTransaction());
        }

        @Override
        public UserAggregationReducer createAggregationFunction(int id, ProcedureCallContext context)
                throws ProcedureException {
            performCheckBeforeOperation();
            AccessMode mode = checkAggregationFunctionAccessMode(id);
            boolean overrideAccessMode = mode.shouldBoostAggregatingFunction(id).allowsAccess();
            return createGenericAggregator(overrideAccessMode, mode, id, context);
        }

        @Override
        ResourceRawIterator<AnyValue[], ProcedureException> doCallProcedure(Context ctx, int id, AnyValue[] input)
                throws ProcedureException {
            return procedureView.callProcedure(ctx, id, input, ktx.resourceMonitor());
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
        KernelTransaction kernelTransaction() {
            return ktx;
        }

        @Override
        Supplier<GraphDatabaseAPI> graphDatabaseAPISupplier() {
            return () -> databaseDependencies.resolveDependency(GraphDatabaseAPI.class);
        }
    }

    public static class ForThreadExecutionContextScope extends ProcedureCaller {

        private final ExecutionContext executionContext;
        private final OverridableSecurityContext overridableSecurityContext;
        private final ExecutionContextProcedureKernelTransaction ktx;
        private final SecurityAuthorizationHandler securityAuthorizationHandler;
        private final Supplier<ClockContext> clockContextSupplier;

        public ForThreadExecutionContextScope(
                ExecutionContext executionContext,
                DependencyResolver databaseDependencies,
                OverridableSecurityContext overridableSecurityContext,
                ExecutionContextProcedureKernelTransaction ktx,
                SecurityAuthorizationHandler securityAuthorizationHandler,
                Supplier<ClockContext> clockContextSupplier,
                ProcedureView procedureView) {
            super(databaseDependencies, procedureView);

            this.executionContext = executionContext;
            this.overridableSecurityContext = overridableSecurityContext;
            this.ktx = ktx;
            this.securityAuthorizationHandler = securityAuthorizationHandler;
            this.clockContextSupplier = clockContextSupplier;
        }

        @Override
        public UserAggregationReducer createAggregationFunction(int id, ProcedureCallContext context)
                throws ProcedureException {
            performCheckBeforeOperation();
            AccessMode mode = checkAggregationFunctionAccessMode(id);
            // The FULL access mode returns true on all shouldBoost-calls,
            // but it doesn't need any boost here since it already supports all read operations.
            boolean overrideAccessMode = mode != AccessMode.Static.FULL
                    && mode.shouldBoostAggregatingFunction(id).allowsAccess();
            if (overrideAccessMode) {
                return createGenericAggregator(true, mode, id, context);
            } else {
                // Generally, functions have the access mode restricted to READ during their invocation.
                // That is actually a quite expensive operation to do for every update call of an aggregation function.
                // Since only read operations are currently supported during parallel execution,
                // the expensive access mode restricting is not needed for execution context API.
                return procedureView.createAggregationFunction(prepareContext(securityContext(), context), id);
            }
        }

        @Override
        ResourceRawIterator<AnyValue[], ProcedureException> doCallProcedure(Context ctx, int id, AnyValue[] input)
                throws ProcedureException {
            return procedureView.callProcedure(ctx, id, input, executionContext);
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
        ExecutionContextProcedureKernelTransaction kernelTransaction() {
            return ktx;
        }

        @Override
        Supplier<GraphDatabaseAPI> graphDatabaseAPISupplier() {
            return () -> new ExecutionContextGraphDatabaseAPI(
                    databaseDependencies.resolveDependency(GraphDatabaseAPI.class));
        }

        @Override
        void performCheckBeforeOperation() {
            ktx.assertOpen();
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
    }
}
