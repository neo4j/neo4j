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

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.NullLog;
import org.neo4j.procedure.builtin.SpecialBuiltInProcedures;
import org.neo4j.string.Globbing;
import org.neo4j.util.VisibleForTesting;

/**
 * This is the coordinating service for procedures in the DBMS. It loads procedures from a specified
 * directory at startup, but also allows programmatic registration of them - and then, of course, allows
 * invoking procedures.
 */
public class GlobalProceduresRegistry extends LifecycleAdapter implements GlobalProcedures {
    private final Cypher5TypeCheckers typeCheckers;
    private ProcedureRegistry registry = new ProcedureRegistry(); // Synchronized by updater
    private final ComponentRegistry safeComponents = new ComponentRegistry(); // Synchronized by updater
    private final ComponentRegistry allComponents = new ComponentRegistry(); // Synchronized by updater
    private final ProcedureCompiler compiler;
    private final Supplier<List<CallableProcedure>> builtin;
    private final Path proceduresDirectory;
    private final RegistrationUpdater updater = new RegistrationUpdater();

    private final ProcedureJarLoader loader;
    private final Predicate<String> isReservedNamespace;

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
        this.typeCheckers = new Cypher5TypeCheckers();
        this.compiler = new ProcedureCompiler(typeCheckers, safeComponents, allComponents, log, config);

        // We must not allow external sources to register procedures in the reserved namespaces.
        // Thus, we restrict the allowed namespaces when loading from disk. The built-in procedure
        // classes will be able to register in any namespace with the unrestricted compiler.
        var restrictedCompiler = compiler.withAdditionalProcedureRestrictions(
                NamingRestrictions.rejectReservedNamespace(config.reservedProcedureNamespaces()));
        this.loader = new ProcedureJarLoader(restrictedCompiler, log, config.procedureReloadEnabled());
        this.isReservedNamespace = Globbing.compose(config.reservedProcedureNamespaces(), List.of());
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
        typeCheckers.registerType(javaClass, new Cypher5TypeCheckers.DefaultValueConverter(type));
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

    @Override
    public ProcedureView getCurrentView() {
        return currentProcedureView.getAcquire();
    }

    @Override
    public void start() throws Exception {
        try (var ignored = updater.acquire()) {
            unguardedLoadFromDisk(registry, (name) -> true);
            for (var procedure : builtin.get()) {
                registry.register(procedure);
            }
        }
    }

    @Override
    public LoadInformation reloadProceduresFromDisk(Transaction tx, Predicate<String> shouldLoadNamespace)
            throws IOException, KernelException {
        try (var ignored = updater.acquireFromTransaction((InternalTransaction) tx)) {
            Predicate<QualifiedName> shouldTombstone = (name) -> {
                var str = name.toString();
                return shouldLoadNamespace.test(str) && !isReservedNamespace.test(str);
            };

            // To avoid tainting the state in case of failure, we create an intermediate.
            var culled = ProcedureRegistry.tombstone(registry, shouldTombstone);
            var out = unguardedLoadFromDisk(culled, shouldLoadNamespace);
            registry = culled;

            return out;
        }
    }

    private LoadInformation unguardedLoadFromDisk(ProcedureRegistry registry, Predicate<String> shouldLoadNamespaces)
            throws IOException, KernelException {
        // We must not allow external sources to register procedures in the reserved namespaces.
        // Thus, we restrict the allowed namespaces when loading from disk. The built-in procedure
        // classes will be able to register in any namespace with the unrestricted compiler.
        ProcedureJarLoader.Callables callables =
                loader.loadProceduresFromDir(proceduresDirectory, shouldLoadNamespaces);
        for (var procedure : callables.procedures()) {
            registry.register(procedure);
        }
        for (var function : callables.functions()) {
            registry.register(function);
        }
        for (var aggregation : callables.aggregationFunctions()) {
            registry.register(aggregation);
        }

        return new LoadInformation(
                callables.procedures().stream()
                        .map(CallableProcedure::signature)
                        .toList(),
                callables.functions().stream()
                        .map(CallableUserFunction::signature)
                        .toList(),
                callables.aggregationFunctions().stream()
                        .map(CallableUserAggregationFunction::signature)
                        .toList());
    }

    @Override
    public void stop() throws Exception {
        loader.close();
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
            typeCheckers.registerType(javaClass, new Cypher5TypeCheckers.DefaultValueConverter(type));
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

        @SuppressWarnings("RedundantThrows")
        @Override
        public LoadInformation reloadProceduresFromDisk(Transaction tx, Predicate<String> namespaceFilter)
                throws KernelException, IOException {
            throw new UnsupportedOperationException("bulk registration does not support loading from disk");
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

        Resource acquireFromTransaction(InternalTransaction tx) {
            // When we reload procedures from disk, we want to verify that nothing has happened in-between
            // the transaction started, and the lock was claimed. Furthermore, we should not wait for the lock,
            // but rather fail with a transient error to avoid blocking a transaction.
            if (!lock.tryLock()) {
                throw new TransientTransactionFailureException(
                        Status.Procedure.ProcedureCallFailed,
                        "The procedure registry is busy. You may retry this operation.");
            }

            if (tx.kernelTransaction().procedures().signatureVersion()
                    != getCurrentView().signatureVersion()) {
                lock.unlock();
                throw new TransientTransactionFailureException(
                        Status.Procedure.ProcedureCallFailed,
                        "The procedure registry was modified by another transaction. You may retry this operation.");
            }

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
