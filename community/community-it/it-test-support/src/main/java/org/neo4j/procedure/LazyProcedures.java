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
package org.neo4j.procedure;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.values.AnyValue;

public class LazyProcedures implements GlobalProcedures, Consumer<Supplier<GlobalProcedures>> {
    private Supplier<GlobalProcedures> initializer;
    private volatile GlobalProcedures globalProcedures;

    private final LazyProcedureView lazyProcedureView = new LazyProcedureView();

    private void init() {
        if (globalProcedures != null) {
            return;
        }
        synchronized (this) {
            if (globalProcedures == null) {
                requireNonNull(initializer);
                globalProcedures = initializer.get();
            }
        }
    }

    @Override
    public void register(CallableProcedure proc) throws ProcedureException {
        init();
        globalProcedures.register(proc);
    }

    @Override
    public void register(CallableUserFunction function) throws ProcedureException {
        init();
        globalProcedures.register(function);
    }

    @Override
    public void register(CallableUserAggregationFunction function) throws ProcedureException {
        init();
        globalProcedures.register(function);
    }

    @Override
    public void registerProcedure(Class<?> proc) throws ProcedureException {
        init();
        globalProcedures.registerProcedure(proc);
    }

    @Override
    public void registerFunction(Class<?> func) throws ProcedureException {
        init();
        globalProcedures.registerFunction(func);
    }

    @Override
    public void registerAggregationFunction(Class<?> func) throws ProcedureException {
        init();
        globalProcedures.registerAggregationFunction(func);
    }

    @Override
    public void registerType(Class<?> javaClass, Neo4jTypes.AnyType type) {
        init();
        globalProcedures.registerType(javaClass, type);
    }

    @Override
    public <T> void registerComponent(
            Class<T> cls, ThrowingFunction<Context, T, ProcedureException> provider, boolean safe) {
        init();
        globalProcedures.registerComponent(cls, provider, safe);
    }

    @Override
    public void unregister(QualifiedName name) {
        var procedures = globalProcedures;
        if (procedures != null) {
            procedures.unregister(name);
        }
    }

    @Override
    public void accept(Supplier<GlobalProcedures> procedureSupplier) {
        requireNonNull(procedureSupplier);
        if (initializer != null) {
            throw new IllegalStateException("Lazy procedures already have initializer: " + initializer);
        }
        initializer = procedureSupplier;
    }

    @Override
    public ProcedureView getCurrentView() {
        /* To honor the LazyProcedures ideal, we want to delay initialization until a call explicitly needs a procedure.
          Typically, a ProcedureView will be collected at KernelTransaction.initialize, which may occur before any call
          made to query the procedure state. This means that we still want to delay initialization a while longer,
          until a call that actually needs the procedure is detected.

         To achieve this, we return a LazyProcedureView in case LazyProcedures has not been initialized. The purpose
         of this class is to forward the initialization signal to LazyProcedures. Once LazyProcedures has been initialized,
         we revert to the normal behaviour of returning a ProcedureView.
        */
        if (globalProcedures == null) {
            return lazyProcedureView;
        }

        return globalProcedures.getCurrentView();
    }

    @Override
    public LoadInformation reloadProceduresFromDisk(Transaction tx, Predicate<String> namespaceFilter)
            throws KernelException, IOException {
        init();
        return globalProcedures.reloadProceduresFromDisk(tx, namespaceFilter);
    }

    private class LazyProcedureView implements ProcedureView {

        private volatile ProcedureView view;

        private LazyProcedureView() {}

        private void initView() {
            if (view != null) {
                return;
            }

            synchronized (this) {
                if (view == null) {
                    init();
                    view = globalProcedures.getCurrentView();
                }
            }
        }

        @Override
        public ProcedureHandle procedure(QualifiedName name, CypherScope scope) throws ProcedureException {
            initView();
            return view.procedure(name, scope);
        }

        @Override
        public UserFunctionHandle function(QualifiedName name, CypherScope scope) {
            initView();
            return view.function(name, scope);
        }

        @Override
        public UserFunctionHandle aggregationFunction(QualifiedName name, CypherScope scope) {
            initView();
            return view.aggregationFunction(name, scope);
        }

        @Override
        public Stream<ProcedureSignature> getAllProcedures(CypherScope scope) {
            initView();
            return view.getAllProcedures(scope);
        }

        @Override
        public Stream<UserFunctionSignature> getAllNonAggregatingFunctions(CypherScope scope) {
            initView();
            return view.getAllNonAggregatingFunctions(scope);
        }

        @Override
        public Stream<UserFunctionSignature> getAllAggregatingFunctions(CypherScope scope) {
            initView();
            return view.getAllAggregatingFunctions(scope);
        }

        @Override
        public ResourceRawIterator<AnyValue[], ProcedureException> callProcedure(
                Context ctx, int id, AnyValue[] input, ResourceMonitor resourceMonitor) throws ProcedureException {
            initView();
            return view.callProcedure(ctx, id, input, resourceMonitor);
        }

        @Override
        public AnyValue callFunction(Context ctx, int id, AnyValue[] input) throws ProcedureException {
            initView();
            return view.callFunction(ctx, id, input);
        }

        @Override
        public UserAggregationReducer createAggregationFunction(Context ctx, int id) throws ProcedureException {
            initView();
            return view.createAggregationFunction(ctx, id);
        }

        @Override
        public <T> ThrowingFunction<Context, T, ProcedureException> lookupComponentProvider(
                Class<T> cls, boolean safe) {
            initView();
            return view.lookupComponentProvider(cls, safe);
        }

        @Override
        public int[] getProcedureIds(String procedureGlobbing) {
            initView();
            return view.getProcedureIds(procedureGlobbing);
        }

        @Override
        public int[] getAdminProcedureIds() {
            initView();
            return view.getAdminProcedureIds();
        }

        @Override
        public int[] getFunctionIds(String functionGlobbing) {
            initView();
            return view.getFunctionIds(functionGlobbing);
        }

        @Override
        public int[] getAggregatingFunctionIds(String functionGlobbing) {
            initView();
            return view.getAggregatingFunctionIds(functionGlobbing);
        }

        @Override
        public long signatureVersion() {
            // Since signatureValues are now used by the query planner cache, basically any transactions
            // would cause initialization. To avoid this, we instead return a placeholder value.
            if (view == null) {
                return -1;
            }
            return view.signatureVersion();
        }
    }
}
