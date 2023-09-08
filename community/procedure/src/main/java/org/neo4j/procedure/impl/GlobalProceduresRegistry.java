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
package org.neo4j.procedure.impl;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.NullLog;
import org.neo4j.procedure.builtin.SpecialBuiltInProcedures;
import org.neo4j.util.VisibleForTesting;

/**
 * This is the coordinating service for procedures in the DBMS. It loads procedures from a specified
 * directory at startup, but also allows programmatic registration of them - and then, of course, allows
 * invoking procedures.
 */
public class GlobalProceduresRegistry extends LifecycleAdapter implements GlobalProcedures {
    private final ProcedureRegistry registry = new ProcedureRegistry();
    private final TypeCheckers typeCheckers;
    private final ComponentRegistry safeComponents = new ComponentRegistry();
    private final ComponentRegistry allComponents = new ComponentRegistry();
    private final ProcedureCompiler compiler;
    private final Supplier<List<CallableProcedure>> builtin;
    private final Path proceduresDirectory;
    private final InternalLog log;

    private final RegistrationUpdater updater = new RegistrationUpdater();

    private final List<String> reservedProcedureNamespace;

    private static final AtomicLong SIGNATURE_VERSION_GENERATOR = new AtomicLong(0);
    private final AtomicReference<ProcedureView> currentProcedureView =
            new AtomicReference<>(makeSnapshot(registry, safeComponents, allComponents));

    @VisibleForTesting
    public GlobalProceduresRegistry() {
        this(SpecialBuiltInProcedures.from("N/A", "N/A"), null, NullLog.getInstance(), ProcedureConfig.DEFAULT);
    }

    public GlobalProceduresRegistry(
            Supplier<List<CallableProcedure>> builtin,
            Path proceduresDirectory,
            InternalLog log,
            ProcedureConfig config) {
        this.builtin = builtin;
        this.proceduresDirectory = proceduresDirectory;
        this.log = log;
        this.typeCheckers = new TypeCheckers();
        this.compiler = new ProcedureCompiler(typeCheckers, safeComponents, allComponents, log, config);
        this.reservedProcedureNamespace = config.reservedProcedureNamespaces();
    }

    /**
     * Register a new procedure.
     * @param proc the procedure.
     */
    @Override
    public void register(CallableProcedure proc) throws ProcedureException {
        try (var ignored = updater.acquire()) {
            registry.register(proc);
        }
    }

    /**
     * Register a new function.
     * @param function the function.
     */
    @Override
    public void register(CallableUserFunction function) throws ProcedureException {
        try (var ignored = updater.acquire()) {
            registry.register(function);
        }
    }

    /**
     * Register a new function.
     * @param function the function.
     */
    @Override
    public void register(CallableUserAggregationFunction function) throws ProcedureException {
        try (var ignored = updater.acquire()) {
            registry.register(function);
        }
    }

    /**
     * Register a new internal procedure defined with annotations on a java class.
     * @param proc the procedure class
     */
    @Override
    public void registerProcedure(Class<?> proc) throws ProcedureException {
        try (var ignored = updater.acquire()) {
            for (var procedure : compiler.compileProcedure(proc, true)) {
                registry.register(procedure);
            }
        }
    }

    /**
     * Register a new function defined with annotations on a java class.
     * @param func the function class
     */
    @Override
    public void registerFunction(Class<?> func) throws ProcedureException {
        try (var ignored = updater.acquire()) {
            for (var function : compiler.compileFunction(func, false)) {
                registry.register(function);
            }
        }
    }

    /**
     * Register a new aggregation function defined with annotations on a java class.
     * @param func the function class
     */
    @Override
    public void registerAggregationFunction(Class<?> func) throws ProcedureException {
        try (var ignored = updater.acquire()) {
            for (var aggregation : compiler.compileAggregationFunction(func)) {
                registry.register(aggregation);
            }
        }
    }

    /**
     * Registers a type and its mapping to Neo4jTypes
     *
     * @param javaClass
     *         the class of the native type
     * @param type
     *         the mapping to Neo4jTypes
     */
    @Override
    public void registerType(Class<?> javaClass, Neo4jTypes.AnyType type) {
        typeCheckers.registerType(javaClass, new TypeCheckers.DefaultValueConverter(type));
    }

    /**
     * Registers a component, these become available in reflective procedures for injection.
     * @param cls the type of component to be registered (this is what users 'ask' for in their field declaration)
     * @param provider a function that supplies the component, given the context of a procedure invocation
     * @param safe set to false if this component can bypass security, true if it respects security
     */
    @Override
    public <T> void registerComponent(
            Class<T> cls, ThrowingFunction<Context, T, ProcedureException> provider, boolean safe) {
        try (var ignored = updater.acquire()) {
            if (safe) {
                safeComponents.register(cls, provider);
            }
            allComponents.register(cls, provider);
        }
    }

    public ProcedureView getCurrentView() {
        return currentProcedureView.getAcquire();
    }

    @Override
    public void start() throws Exception {
        try (var ignored = updater.acquire()) {
            // We must not allow external sources to register procedures in the reserved namespaces.
            // Thus, we restrict the allowed namespaces when loading from disk. The built-in procedure
            // classes will be able to register in any namespace with the unrestricted compiler.
            var restrictedCompiler = compiler.withAdditionalProcedureRestrictions(
                    NamingRestrictions.rejectReservedNamespace(reservedProcedureNamespace));
            ProcedureJarLoader loader = new ProcedureJarLoader(restrictedCompiler, log);
            ProcedureJarLoader.Callables callables = loader.loadProceduresFromDir(proceduresDirectory);
            for (CallableProcedure procedure : callables.procedures()) {
                registry.register(procedure);
            }

            for (CallableUserFunction function : callables.functions()) {
                registry.register(function);
            }

            for (CallableUserAggregationFunction function : callables.aggregationFunctions()) {
                registry.register(function);
            }

            // And register built-in procedures
            for (var procedure : builtin.get()) {
                registry.register(procedure);
            }
        }
    }

    @VisibleForTesting
    @Override
    public void unregister(QualifiedName name) {
        try (var ignored = updater.acquire()) {
            registry.unregister(name);
        }
    }

    public BulkRegistration bulk() {
        return new BulkRegistration(updater.acquire());
    }

    public class BulkRegistration implements GlobalProcedures, Resource {
        final Resource onClose;

        private BulkRegistration(Resource onClose) {
            this.onClose = onClose;
        }

        /**
         * Register a new procedure.
         * @param proc the procedure.
         */
        @Override
        public void register(CallableProcedure proc) throws ProcedureException {
            registry.register(proc);
        }

        /**
         * Register a new function.
         * @param function the function.
         */
        @Override
        public void register(CallableUserFunction function) throws ProcedureException {
            registry.register(function);
        }

        /**
         * Register a new function.
         * @param function the function.
         */
        @Override
        public void register(CallableUserAggregationFunction function) throws ProcedureException {
            registry.register(function);
        }

        /**
         * Register a new internal procedure defined with annotations on a java class.
         * @param proc the procedure class
         */
        @Override
        public void registerProcedure(Class<?> proc) throws ProcedureException {
            for (var procedure : compiler.compileProcedure(proc, true)) {
                registry.register(procedure);
            }
        }

        /**
         * Register a new function defined with annotations on a java class.
         * @param func the function class
         */
        @Override
        public void registerFunction(Class<?> func) throws ProcedureException {
            for (var function : compiler.compileFunction(func, false)) {
                registry.register(function);
            }
        }

        /**
         * Register a new aggregation function defined with annotations on a java class.
         * @param func the function class
         */
        @Override
        public void registerAggregationFunction(Class<?> func) throws ProcedureException {
            for (var aggregation : compiler.compileAggregationFunction(func)) {
                registry.register(aggregation);
            }
        }

        /**
         * Registers a type and its mapping to Neo4jTypes
         *
         * @param javaClass
         *         the class of the native type
         * @param type
         *         the mapping to Neo4jTypes
         */
        @Override
        public void registerType(Class<?> javaClass, Neo4jTypes.AnyType type) {
            typeCheckers.registerType(javaClass, new TypeCheckers.DefaultValueConverter(type));
        }

        /**
         * Registers a component, these become available in reflective procedures for injection.
         * @param cls the type of component to be registered (this is what users 'ask' for in their field declaration)
         * @param provider a function that supplies the component, given the context of a procedure invocation
         * @param safe set to false if this component can bypass security, true if it respects security
         */
        @Override
        public <T> void registerComponent(
                Class<T> cls, ThrowingFunction<Context, T, ProcedureException> provider, boolean safe) {
            if (safe) {
                safeComponents.register(cls, provider);
            }
            allComponents.register(cls, provider);
        }

        @VisibleForTesting
        @Override
        public void unregister(QualifiedName name) {
            registry.unregister(name);
        }

        @Override
        public ProcedureView getCurrentView() {
            return currentProcedureView.getAcquire();
        }

        @Override
        public void close() {
            onClose.close();
        }
    }

    private static ProcedureView makeSnapshot(
            ProcedureRegistry registry, ComponentRegistry safeComponents, ComponentRegistry allComponents) {
        return ProcedureViewImpl.snapshot(
                SIGNATURE_VERSION_GENERATOR.incrementAndGet(), registry, safeComponents, allComponents);
    }

    /**
     * The RegistrationLatch is responsible for protecting concurrent mutation
     * of the internals of the GlobalProceduresRegistry, as well as generating
     * new snapshots upon finishing mutations.
     */
    private class RegistrationUpdater {
        private final ReentrantLock lock = new ReentrantLock();

        Resource acquire() {
            lock.lock();
            return () -> {
                try {
                    currentProcedureView.setRelease(makeSnapshot(registry, safeComponents, allComponents));
                } finally {
                    lock.unlock();
                }
            };
        }
    }
}
