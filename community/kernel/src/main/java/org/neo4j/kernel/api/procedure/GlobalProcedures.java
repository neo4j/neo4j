/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.procedure;

import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.values.AnyValue;

public interface GlobalProcedures
{
    void register( CallableProcedure proc ) throws ProcedureException;

    void register( CallableUserFunction function ) throws ProcedureException;

    void register( CallableUserAggregationFunction function ) throws ProcedureException;

    void register( CallableUserFunction function, boolean overrideCurrentImplementation ) throws ProcedureException;

    void register( CallableUserAggregationFunction function, boolean overrideCurrentImplementation ) throws ProcedureException;

    void register( CallableProcedure proc, boolean overrideCurrentImplementation ) throws ProcedureException;

    void registerProcedure( Class<?> proc ) throws KernelException;

    void registerProcedure( Class<?> proc, boolean overrideCurrentImplementation ) throws KernelException;

    void registerProcedure( Class<?> proc, boolean overrideCurrentImplementation, String warning ) throws KernelException;

    void registerBuiltInFunctions( Class<?> func ) throws KernelException;

    void registerFunction( Class<?> func ) throws KernelException;

    void registerAggregationFunction( Class<?> func, boolean overrideCurrentImplementation ) throws KernelException;

    void registerAggregationFunction( Class<?> func ) throws KernelException;

    void registerFunction( Class<?> func, boolean overrideCurrentImplementation ) throws KernelException;

    void registerType( Class<?> javaClass, Neo4jTypes.AnyType type );

    <T> void registerComponent( Class<T> cls, ThrowingFunction<Context,T,ProcedureException> provider, boolean safe );

    ProcedureHandle procedure( QualifiedName name ) throws ProcedureException;

    UserFunctionHandle function( QualifiedName name );

    UserFunctionHandle aggregationFunction( QualifiedName name );

    Set<ProcedureSignature> getAllProcedures();

    Stream<UserFunctionSignature> getAllNonAggregatingFunctions();

    Stream<UserFunctionSignature> getAllAggregatingFunctions();

    RawIterator<AnyValue[], ProcedureException> callProcedure( Context ctx, int id, AnyValue[] input,
            ResourceTracker resourceTracker ) throws ProcedureException;

    AnyValue callFunction( Context ctx, int id, AnyValue[] input ) throws ProcedureException;

    UserAggregator createAggregationFunction( Context ctx, int id ) throws ProcedureException;
}
