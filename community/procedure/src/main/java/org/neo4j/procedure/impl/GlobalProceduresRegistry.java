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
package org.neo4j.procedure.impl;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;
import org.neo4j.function.ThrowingFunction;
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
    private ProcedureRegistry registry = new ProcedureRegistry();
    private final TypeCheckers typeCheckers;
    private final ComponentRegistry safeComponents = new ComponentRegistry();
    private final ComponentRegistry allComponents = new ComponentRegistry();
    private final ProcedureCompiler compiler;
    private final Supplier<List<CallableProcedure>> builtin;
    private final Path proceduresDirectory;
    private final InternalLog log;

    private ProcedureView currentProcedureView = new ProcedureViewImpl(registry, safeComponents, allComponents);

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
    }

    /**
     * Register a new procedure. This method must not be called concurrently with {@link #procedure(QualifiedName)}.
     * @param proc the procedure.
     */
    @Override
    public void register(CallableProcedure proc) throws ProcedureException {
        registry.register(proc, false);
    }

    /**
     * Register a new function. This method must not be called concurrently with {@link #procedure(QualifiedName)}.
     * @param function the function.
     */
    @Override
    public void register(CallableUserFunction function) throws ProcedureException {
        registry.register(function, false);
    }

    /**
     * Register a new function. This method must not be called concurrently with {@link #procedure(QualifiedName)}.
     * @param function the function.
     */
    @Override
    public void register(CallableUserAggregationFunction function) throws ProcedureException {
        registry.register(function, false);
    }

    /**
     * Register a new internal procedure defined with annotations on a java class.
     * @param proc the procedure class
     */
    @Override
    public void registerProcedure(Class<?> proc) throws ProcedureException {
        for (CallableProcedure procedure : compiler.compileProcedure(proc, true)) {
            register(procedure);
        }
    }

    /**
     * Register a new aggregation function defined with annotations on a java class.
     * @param func the function class
     */
    @Override
    public void registerAggregationFunction(Class<?> func) throws ProcedureException {
        for (CallableUserAggregationFunction function : compiler.compileAggregationFunction(func)) {
            register(function);
        }
    }

    /**
     * Register a new function defined with annotations on a java class.
     * @param func the function class
     */
    @Override
    public void registerFunction(Class<?> func) throws ProcedureException {
        for (CallableUserFunction function : compiler.compileFunction(func, false)) {
            register(function);
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

    public ProcedureView getCurrentView() {
        return currentProcedureView;
    }

    @Override
    public void start() throws Exception {
        ProcedureJarLoader loader = new ProcedureJarLoader(compiler, log);
        ProcedureJarLoader.Callables callables = loader.loadProceduresFromDir(proceduresDirectory);
        for (CallableProcedure procedure : callables.procedures()) {
            register(procedure);
        }

        for (CallableUserFunction function : callables.functions()) {
            register(function);
        }

        for (CallableUserAggregationFunction function : callables.aggregationFunctions()) {
            register(function);
        }

        // And register built-in procedures
        for (var procedure : builtin.get()) {
            register(procedure);
        }
    }

    @VisibleForTesting
    @Override
    public void unregister(QualifiedName name) {
        registry.unregister(name);
    }
}
