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
package org.neo4j.fabric.transaction;

import java.time.Clock;
import java.util.stream.Stream;
import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;

/**
 * Fabric needs to evaluate functions and evaluate procedure names, without being having access to a {@link Context}.
 *
 * This class provides a wrapper around {@link ProcedureView} that satisfies these requirements.
 */
public class FabricProcedures implements Procedures {

    private final ProcedureView view;

    private final Context EMPTY_CONTEXT = new EmptyProcedureContext();

    public FabricProcedures(ProcedureView view) {
        this.view = view;
    }

    private static <T> T notAvailable() {
        throw new RuntimeException("Operation not available in static context.");
    }

    @Override
    public UserFunctionHandle functionGet(QualifiedName name, CypherScope scope) {
        return view.function(name, scope);
    }

    @Override
    public Stream<UserFunctionSignature> functionGetAll(CypherScope scope) {
        return notAvailable();
    }

    @Override
    public UserFunctionHandle aggregationFunctionGet(QualifiedName name, CypherScope scope) {
        return notAvailable();
    }

    @Override
    public Stream<UserFunctionSignature> aggregationFunctionGetAll(CypherScope scope) {
        return notAvailable();
    }

    @Override
    public ProcedureHandle procedureGet(QualifiedName name, CypherScope scope) throws ProcedureException {
        // Note:  Bolt checks connections by invoking `dbms.routing.getRoutingTable`.
        // This means that we have to be able to resolve this symbol in the planning step.
        return view.procedure(name, scope);
    }

    @Override
    public Stream<ProcedureSignature> proceduresGetAll(CypherScope scope) throws ProcedureException {
        return notAvailable();
    }

    @Override
    public ResourceRawIterator<AnyValue[], ProcedureException> procedureCallRead(
            int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException {
        return notAvailable();
    }

    @Override
    public ResourceRawIterator<AnyValue[], ProcedureException> procedureCallWrite(
            int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException {
        return notAvailable();
    }

    @Override
    public ResourceRawIterator<AnyValue[], ProcedureException> procedureCallSchema(
            int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException {
        return notAvailable();
    }

    @Override
    public ResourceRawIterator<AnyValue[], ProcedureException> procedureCallDbms(
            int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException {
        return notAvailable();
    }

    @Override
    public AnyValue functionCall(int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException {
        return view.callFunction(EMPTY_CONTEXT, id, arguments);
    }

    @Override
    public AnyValue builtInFunctionCall(int id, AnyValue[] arguments, ProcedureCallContext context)
            throws ProcedureException {
        return view.callFunction(EMPTY_CONTEXT, id, arguments);
    }

    @Override
    public UserAggregationReducer aggregationFunction(int id, ProcedureCallContext context) throws ProcedureException {
        return notAvailable();
    }

    @Override
    public UserAggregationReducer builtInAggregationFunction(int id, ProcedureCallContext context)
            throws ProcedureException {
        return notAvailable();
    }

    @Override
    public long signatureVersion() {
        return view.signatureVersion();
    }

    private static class EmptyProcedureContext implements Context {

        private EmptyProcedureContext() {
            /* We indent to restrict the usage to FabricProcedures only */
        }

        @Override
        public ValueMapper<Object> valueMapper() {
            return notAvailable();
        }

        @Override
        public SecurityContext securityContext() {
            return notAvailable();
        }

        @Override
        public DependencyResolver dependencyResolver() {
            return notAvailable();
        }

        @Override
        public GraphDatabaseAPI graphDatabaseAPI() {
            return notAvailable();
        }

        @Override
        public Thread thread() {
            return notAvailable();
        }

        @Override
        public Transaction transaction() {
            return notAvailable();
        }

        @Override
        public InternalTransaction internalTransaction() {
            return notAvailable();
        }

        @Override
        public InternalTransaction internalTransactionOrNull() {
            return notAvailable();
        }

        @Override
        public KernelTransaction kernelTransaction() {
            return notAvailable();
        }

        @Override
        public Clock systemClock() throws ProcedureException {
            return notAvailable();
        }

        @Override
        public Clock statementClock() throws ProcedureException {
            return notAvailable();
        }

        @Override
        public Clock transactionClock() throws ProcedureException {
            return notAvailable();
        }

        @Override
        public URLAccessChecker urlAccessChecker() throws ProcedureException {
            return notAvailable();
        }

        @Override
        public ProcedureCallContext procedureCallContext() {
            return notAvailable();
        }
    }
}
