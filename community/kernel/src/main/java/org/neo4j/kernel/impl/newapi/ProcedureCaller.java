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

import org.neo4j.collection.RawIterator;
import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AdminAccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.values.AnyValue;

public class ProcedureCaller {

    private final KernelTransaction ktx;
    private final GlobalProcedures globalProcedures;
    private final DependencyResolver databaseDependencies;

    public ProcedureCaller(
            KernelTransaction ktx, GlobalProcedures globalProcedures, DependencyResolver databaseDependencies) {
        this.ktx = ktx;
        this.globalProcedures = globalProcedures;
        this.databaseDependencies = databaseDependencies;
    }

    public RawIterator<AnyValue[], ProcedureException> callProcedure(
            int id, AnyValue[] input, final AccessMode.Static procedureMode, ProcedureCallContext procedureCallContext)
            throws ProcedureException {
        ktx.assertOpen();

        AccessMode mode = ktx.securityContext().mode();
        if (!mode.allowsExecuteProcedure(id).allowsAccess()) {
            String message = format(
                    "Executing procedure is not allowed for %s.",
                    ktx.securityContext().description());
            throw ktx.securityAuthorizationHandler().logAndGetAuthorizationException(ktx.securityContext(), message);
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

    public RawIterator<AnyValue[], ProcedureException> createIterator(
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

    public AnyValue callFunction(int id, AnyValue[] input) throws ProcedureException {
        ktx.assertOpen();

        AccessMode mode = ktx.securityContext().mode();
        if (!mode.allowsExecuteFunction(id).allowsAccess()) {
            String message = format(
                    "Executing a user defined function is not allowed for %s.",
                    ktx.securityContext().description());
            throw ktx.securityAuthorizationHandler().logAndGetAuthorizationException(ktx.securityContext(), message);
        }

        final SecurityContext securityContext = mode.shouldBoostFunction(id).allowsAccess()
                ? ktx.securityContext().withMode(new OverriddenAccessMode(mode, AccessMode.Static.READ))
                : ktx.securityContext().withMode(new RestrictedAccessMode(mode, AccessMode.Static.READ));

        try (KernelTransaction.Revertable ignore = ktx.overrideWith(securityContext)) {
            return globalProcedures.callFunction(
                    prepareContext(securityContext, ProcedureCallContext.EMPTY), id, input);
        }
    }

    public AnyValue callBuiltInFunction(int id, AnyValue[] input) throws ProcedureException {
        ktx.assertOpen();
        return globalProcedures.callFunction(
                prepareContext(ktx.securityContext(), ProcedureCallContext.EMPTY), id, input);
    }

    public UserAggregator createAggregationFunction(int id) throws ProcedureException {
        ktx.assertOpen();

        AccessMode mode = ktx.securityContext().mode();
        if (!mode.allowsExecuteAggregatingFunction(id).allowsAccess()) {
            String message = format(
                    "Executing a user defined aggregating function is not allowed for %s.",
                    ktx.securityContext().description());
            throw ktx.securityAuthorizationHandler().logAndGetAuthorizationException(ktx.securityContext(), message);
        }

        final SecurityContext securityContext =
                mode.shouldBoostAggregatingFunction(id).allowsAccess()
                        ? ktx.securityContext().withMode(new OverriddenAccessMode(mode, AccessMode.Static.READ))
                        : ktx.securityContext().withMode(new RestrictedAccessMode(mode, AccessMode.Static.READ));

        try (KernelTransaction.Revertable ignore = ktx.overrideWith(securityContext)) {
            UserAggregator aggregator = globalProcedures.createAggregationFunction(
                    prepareContext(securityContext, ProcedureCallContext.EMPTY), id);
            return new UserAggregator() {
                @Override
                public void update(AnyValue[] input) throws ProcedureException {
                    try (KernelTransaction.Revertable ignore = ktx.overrideWith(securityContext)) {
                        aggregator.update(input);
                    }
                }

                @Override
                public AnyValue result() throws ProcedureException {
                    try (KernelTransaction.Revertable ignore = ktx.overrideWith(securityContext)) {
                        return aggregator.result();
                    }
                }
            };
        }
    }

    public UserAggregator createBuiltInAggregationFunction(int id) throws ProcedureException {
        ktx.assertOpen();

        UserAggregator aggregator = globalProcedures.createAggregationFunction(
                prepareContext(ktx.securityContext(), ProcedureCallContext.EMPTY), id);
        return new UserAggregator() {
            @Override
            public void update(AnyValue[] input) throws ProcedureException {
                aggregator.update(input);
            }

            @Override
            public AnyValue result() throws ProcedureException {
                return aggregator.result();
            }
        };
    }

    private Context prepareContext(SecurityContext securityContext, ProcedureCallContext procedureContext) {
        final InternalTransaction internalTransaction = ktx.internalTransaction();
        return buildContext(databaseDependencies, new DefaultValueMapper(internalTransaction))
                .withTransaction(internalTransaction)
                .withSecurityContext(securityContext)
                .withProcedureCallContext(procedureContext)
                .context();
    }
}
