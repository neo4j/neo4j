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

import java.util.function.Predicate;
import java.util.stream.Stream;
import org.neo4j.collection.RawIterator;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.helpers.collection.LfuCache;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.string.Globbing;
import org.neo4j.values.AnyValue;

public class ProcedureViewImpl implements ProcedureView {

    private static final int LOOKUP_CACHE_SIZE = 100;

    private final long signatureVersion;
    private final ProcedureRegistry registry;
    private final ComponentRegistry safeComponents;
    private final ComponentRegistry allComponents;

    private final LfuCache<String, int[]> proceduresLookupCache = new LfuCache<>("procedures", LOOKUP_CACHE_SIZE);
    private final LfuCache<String, int[]> functionsLookupCache = new LfuCache<>("functions", LOOKUP_CACHE_SIZE);
    private final LfuCache<String, int[]> aggregationFunctionsLookupCache =
            new LfuCache<>("aggregationFunctions", LOOKUP_CACHE_SIZE);

    private ProcedureViewImpl(
            long signatureVersion,
            ProcedureRegistry registryView,
            ComponentRegistry safeComponents,
            ComponentRegistry allComponents) {
        this.signatureVersion = signatureVersion;
        this.registry = registryView;
        this.safeComponents = safeComponents;
        this.allComponents = allComponents;
    }

    /**
     *  Produces an immutable snapshot of the current registries
     *
     * @param procedureRegistry the registered procedures
     * @param safeComponents the registered "safe" components
     * @param allComponents  all registered components
     *
     * @return an immutable view of the registries
     */
    public static ProcedureView snapshot(
            long signatureVersion,
            ProcedureRegistry registry,
            ComponentRegistry safeComponents,
            ComponentRegistry allComponents) {
        return new ProcedureViewImpl(
                signatureVersion,
                ProcedureRegistry.copyOf(registry),
                ComponentRegistry.copyOf(safeComponents),
                ComponentRegistry.copyOf(allComponents));
    }

    /**
     * Lookup registered component providers functions that capable to provide user requested type in scope of procedure invocation context.
     * <p>
     * Typically, getters for the components are injected into the compiled procedure classes at procedure compilation time.
     * However, if the procedure/function is implemented directly via either
     * {@link org.neo4j.kernel.api.procedure.CallableProcedure}, {@link org.neo4j.kernel.api.procedure.CallableUserFunction}
     * , or {@link org.neo4j.kernel.api.procedure.CallableUserAggregationFunction} they do not receive their component
     * getters. This function provides support for accessing components in these cases.
     * </p>
     * <p>Note: GDS relies on this functionality.</p>
     * @param cls the type of registered component
     * @param safe set to false if desired component can bypass security, true if it respects security
     * @return registered provider function if registered, null otherwise
     */
    @Override
    public <T> ThrowingFunction<Context, T, ProcedureException> lookupComponentProvider(Class<T> cls, boolean safe) {
        var registryView = safe ? safeComponents : allComponents;
        return registryView.providerFor(cls);
    }

    @Override
    public ProcedureHandle procedure(QualifiedName name) throws ProcedureException {
        return registry.procedure(name);
    }

    @Override
    public UserFunctionHandle function(QualifiedName name) {
        return registry.function(name);
    }

    @Override
    public UserFunctionHandle aggregationFunction(QualifiedName name) {
        return registry.aggregationFunction(name);
    }

    @Override
    public Stream<ProcedureSignature> getAllProcedures() {
        return registry.getAllProcedures();
    }

    @Override
    public Stream<UserFunctionSignature> getAllNonAggregatingFunctions() {
        return registry.getAllNonAggregatingFunctions();
    }

    @Override
    public Stream<UserFunctionSignature> getAllAggregatingFunctions() {
        return registry.getAllAggregatingFunctions();
    }

    @Override
    public RawIterator<AnyValue[], ProcedureException> callProcedure(
            Context ctx, int id, AnyValue[] input, ResourceMonitor resourceMonitor) throws ProcedureException {
        return registry.callProcedure(ctx, id, input, resourceMonitor);
    }

    @Override
    public AnyValue callFunction(Context ctx, int id, AnyValue[] input) throws ProcedureException {
        return registry.callFunction(ctx, id, input);
    }

    @Override
    public UserAggregationReducer createAggregationFunction(Context ctx, int id) throws ProcedureException {
        return registry.createAggregationFunction(ctx, id);
    }

    @Override
    public int[] getProcedureIds(String procedureGlobbing) {
        return proceduresLookupCache.computeIfAbsent(procedureGlobbing, this::matchProcedure);
    }

    @Override
    public int[] getAdminProcedureIds() {
        return registry.getIdsOfProceduresMatching(p -> p.signature().admin());
    }

    @Override
    public int[] getFunctionIds(String functionGlobbing) {
        return functionsLookupCache.computeIfAbsent(functionGlobbing, this::matchFunction);
    }

    @Override
    public int[] getAggregatingFunctionIds(String functionGlobbing) {
        return aggregationFunctionsLookupCache.computeIfAbsent(functionGlobbing, this::matchAggregation);
    }

    @Override
    public long signatureVersion() {
        return signatureVersion;
    }

    private int[] matchProcedure(String glob) {
        Predicate<String> matcherPredicate = Globbing.globPredicate(glob);
        return registry.getIdsOfProceduresMatching(
                p -> matcherPredicate.test(p.signature().name().toString()));
    }

    private int[] matchFunction(String glob) {
        Predicate<String> matcherPredicate = Globbing.globPredicate(glob);
        return registry.getIdsOfFunctionsMatching(
                p -> matcherPredicate.test(p.signature().name().toString()));
    }

    private int[] matchAggregation(String glob) {
        Predicate<String> matcherPredicate = Globbing.globPredicate(glob);
        return registry.getIdsOfAggregatingFunctionsMatching(
                p -> matcherPredicate.test(p.signature().name().toString()));
    }
}
