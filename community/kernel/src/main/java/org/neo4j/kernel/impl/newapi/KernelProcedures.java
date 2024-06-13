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

import java.util.function.Supplier;
import java.util.stream.Stream;
import org.neo4j.collection.Dependencies;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.OverridableSecurityContext;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextProcedureKernelTransaction;
import org.neo4j.kernel.impl.api.parallel.ThreadExecutionContext;
import org.neo4j.values.AnyValue;

public abstract sealed class KernelProcedures implements Procedures {

    private final AssertOpen assertOpen;

    private KernelProcedures(AssertOpen assertOpen) {
        this.assertOpen = assertOpen;
    }

    public static final class ForTransactionScope extends KernelProcedures {

        private final KernelTransactionImplementation ktx;
        private final Dependencies databaseDependencies;
        private ProcedureCaller.ForTransactionScope procedureCaller;

        public ForTransactionScope(
                KernelTransactionImplementation ktx, Dependencies databaseDependencies, AssertOpen assertOpen) {
            super(assertOpen);
            this.ktx = ktx;
            this.databaseDependencies = databaseDependencies;
        }

        @Override
        protected ProcedureCaller getProcedureCaller() {
            return procedureCaller;
        }

        public void initialize(ProcedureView procedureView) {
            this.procedureCaller = new ProcedureCaller.ForTransactionScope(ktx, databaseDependencies, procedureView);
        }

        public void reset() {
            procedureCaller = null;
        }
    }

    public static final class ForThreadExecutionContextScope extends KernelProcedures {

        private final ProcedureCaller.ForThreadExecutionContextScope procedureCaller;

        public ForThreadExecutionContextScope(
                ThreadExecutionContext executionContext,
                Dependencies databaseDependencies,
                OverridableSecurityContext overridableSecurityContext,
                ExecutionContextProcedureKernelTransaction kernelTransaction,
                SecurityAuthorizationHandler securityAuthorizationHandler,
                Supplier<ClockContext> clockContextSupplier,
                ProcedureView procedureView) {
            super(kernelTransaction);
            this.procedureCaller = new ProcedureCaller.ForThreadExecutionContextScope(
                    executionContext,
                    databaseDependencies,
                    overridableSecurityContext,
                    kernelTransaction,
                    securityAuthorizationHandler,
                    clockContextSupplier,
                    procedureView);
        }

        @Override
        protected ProcedureCaller getProcedureCaller() {
            return procedureCaller;
        }

        @Override
        public RawIterator<AnyValue[], ProcedureException> procedureCallWrite(
                int id, AnyValue[] arguments, ProcedureCallContext context) {
            throw new UnsupportedOperationException(
                    "Invoking procedure with WRITE access mode is not allowed during parallel execution.");
        }

        @Override
        public RawIterator<AnyValue[], ProcedureException> procedureCallSchema(
                int id, AnyValue[] arguments, ProcedureCallContext context) {
            throw new UnsupportedOperationException(
                    "Invoking procedure with SCHEMA access mode is not allowed during parallel execution.");
        }
    }

    protected abstract ProcedureCaller getProcedureCaller();

    private void performCheckBeforeOperation() {
        assertOpen.assertOpen();
    }

    @Override
    public RawIterator<AnyValue[], ProcedureException> procedureCallRead(
            int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException {
        return getProcedureCaller().callProcedure(id, arguments, AccessMode.Static.READ, context);
    }

    @Override
    public RawIterator<AnyValue[], ProcedureException> procedureCallWrite(
            int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException {
        return getProcedureCaller().callProcedure(id, arguments, AccessMode.Static.TOKEN_WRITE, context);
    }

    @Override
    public RawIterator<AnyValue[], ProcedureException> procedureCallSchema(
            int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException {
        return getProcedureCaller().callProcedure(id, arguments, AccessMode.Static.SCHEMA, context);
    }

    @Override
    public RawIterator<AnyValue[], ProcedureException> procedureCallDbms(
            int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException {
        return getProcedureCaller().callProcedure(id, arguments, AccessMode.Static.ACCESS, context);
    }

    @Override
    public AnyValue functionCall(int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException {
        return getProcedureCaller().callFunction(id, arguments, context);
    }

    @Override
    public AnyValue builtInFunctionCall(int id, AnyValue[] arguments, ProcedureCallContext context)
            throws ProcedureException {
        return getProcedureCaller().callBuiltInFunction(id, arguments, context);
    }

    @Override
    public UserAggregationReducer aggregationFunction(int id, ProcedureCallContext context) throws ProcedureException {
        return getProcedureCaller().createAggregationFunction(id, context);
    }

    @Override
    public UserAggregationReducer builtInAggregationFunction(int id, ProcedureCallContext context)
            throws ProcedureException {
        return getProcedureCaller().createBuiltInAggregationFunction(id, context);
    }

    @Override
    public UserFunctionHandle functionGet(QualifiedName name, CypherScope scope) {
        performCheckBeforeOperation();
        return getProcedureCaller().procedureView.function(name, scope);
    }

    @Override
    public Stream<UserFunctionSignature> functionGetAll(CypherScope scope) {
        performCheckBeforeOperation();
        return getProcedureCaller().procedureView.getAllNonAggregatingFunctions(scope);
    }

    @Override
    public ProcedureHandle procedureGet(QualifiedName name, CypherScope scope) throws ProcedureException {
        performCheckBeforeOperation();
        return getProcedureCaller().procedureView.procedure(name, scope);
    }

    @Override
    public Stream<ProcedureSignature> proceduresGetAll(CypherScope scope) {
        performCheckBeforeOperation();
        return getProcedureCaller().procedureView.getAllProcedures(scope);
    }

    @Override
    public UserFunctionHandle aggregationFunctionGet(QualifiedName name, CypherScope scope) {
        performCheckBeforeOperation();
        return getProcedureCaller().procedureView.aggregationFunction(name, scope);
    }

    @Override
    public Stream<UserFunctionSignature> aggregationFunctionGetAll(CypherScope scope) {
        performCheckBeforeOperation();
        return getProcedureCaller().procedureView.getAllAggregatingFunctions(scope);
    }

    @Override
    public long signatureVersion() {
        return getProcedureCaller().procedureView.signatureVersion();
    }
}
