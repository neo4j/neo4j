/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.procedure;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
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
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.values.AnyValue;

import static java.util.Objects.requireNonNull;

public class LazyProcedures implements GlobalProcedures, Consumer<Supplier<GlobalProcedures>>
{
    private Supplier<GlobalProcedures> initializer;
    private volatile GlobalProcedures globalProcedures;

    private void init()
    {
        if ( globalProcedures != null )
        {
            return;
        }
        synchronized ( this )
        {
            if ( globalProcedures == null )
            {
                requireNonNull( initializer );
                globalProcedures = initializer.get();
            }
        }
    }

    @Override
    public void register( CallableProcedure proc ) throws ProcedureException
    {
        init();
        globalProcedures.register( proc );
    }

    @Override
    public void register( CallableUserFunction function ) throws ProcedureException
    {
        init();
        globalProcedures.register( function );
    }

    @Override
    public void register( CallableUserAggregationFunction function ) throws ProcedureException
    {
        init();
        globalProcedures.register( function );
    }

    @Override
    public void register( CallableUserFunction function, boolean overrideCurrentImplementation ) throws ProcedureException
    {
        init();
        globalProcedures.register( function, overrideCurrentImplementation );
    }

    @Override
    public void registerBuiltIn( CallableUserFunction function ) throws ProcedureException
    {
        init();
        globalProcedures.registerBuiltIn( function );
    }

    @Override
    public void register( CallableUserAggregationFunction function, boolean overrideCurrentImplementation ) throws ProcedureException
    {
        init();
        globalProcedures.register( function, overrideCurrentImplementation );
    }

    @Override
    public void register( CallableProcedure proc, boolean overrideCurrentImplementation ) throws ProcedureException
    {
        init();
        globalProcedures.register( proc, overrideCurrentImplementation );
    }

    @Override
    public void registerProcedure( Class<?> proc ) throws KernelException
    {
        init();
        globalProcedures.registerProcedure( proc );
    }

    @Override
    public void registerProcedure( Class<?> proc, boolean overrideCurrentImplementation ) throws KernelException
    {
        init();
        globalProcedures.registerProcedure( proc, overrideCurrentImplementation );
    }

    @Override
    public void registerProcedure( Class<?> proc, boolean overrideCurrentImplementation, String warning ) throws KernelException
    {
        init();
        globalProcedures.registerProcedure( proc, overrideCurrentImplementation, warning );
    }

    @Override
    public void registerBuiltInFunctions( Class<?> func ) throws KernelException
    {
        init();
        globalProcedures.registerBuiltInFunctions( func );
    }

    @Override
    public void registerFunction( Class<?> func ) throws KernelException
    {
        init();
        globalProcedures.registerFunction( func );
    }

    @Override
    public void registerAggregationFunction( Class<?> func, boolean overrideCurrentImplementation ) throws KernelException
    {
        init();
        globalProcedures.registerAggregationFunction( func, overrideCurrentImplementation );
    }

    @Override
    public void registerAggregationFunction( Class<?> func ) throws KernelException
    {
        init();
        globalProcedures.registerAggregationFunction( func );
    }

    @Override
    public void registerFunction( Class<?> func, boolean overrideCurrentImplementation ) throws KernelException
    {
        init();
        globalProcedures.registerFunction( func, overrideCurrentImplementation );
    }

    @Override
    public void registerType( Class<?> javaClass, Neo4jTypes.AnyType type )
    {
        init();
        globalProcedures.registerType( javaClass, type );
    }

    @Override
    public <T> void registerComponent( Class<T> cls, ThrowingFunction<Context,T,ProcedureException> provider, boolean safe )
    {
        init();
        globalProcedures.registerComponent( cls, provider, safe );
    }

    @Override
    public ProcedureHandle procedure( QualifiedName name ) throws ProcedureException
    {
        init();
        return globalProcedures.procedure( name );
    }

    @Override
    public UserFunctionHandle function( QualifiedName name )
    {
        init();
        return globalProcedures.function( name );
    }

    @Override
    public UserFunctionHandle aggregationFunction( QualifiedName name )
    {
        init();
        return globalProcedures.aggregationFunction( name );
    }

    @Override
    public int[] getIdsOfFunctionsMatching( Predicate<CallableUserFunction> predicate )
    {
        init();
        return globalProcedures.getIdsOfFunctionsMatching( predicate );
    }

    @Override
    public int[] getIdsOfAggregatingFunctionsMatching( Predicate<CallableUserAggregationFunction> predicate )
    {
        init();
        return globalProcedures.getIdsOfAggregatingFunctionsMatching( predicate );
    }

    @Override
    public boolean isBuiltInFunction( int id )
    {
        init();
        return globalProcedures.isBuiltInFunction( id );
    }

    @Override
    public boolean isBuiltInAggregatingFunction( int id )
    {
        init();
        return globalProcedures.isBuiltInAggregatingFunction( id );
    }

    @Override
    public Set<ProcedureSignature> getAllProcedures()
    {
        init();
        return globalProcedures.getAllProcedures();
    }

    @Override
    public int[] getIdsOfProceduresMatching( Predicate<CallableProcedure> predicate )
    {
        init();
        return globalProcedures.getIdsOfProceduresMatching( predicate );
    }

    @Override
    public Stream<UserFunctionSignature> getAllNonAggregatingFunctions()
    {
        init();
        return globalProcedures.getAllNonAggregatingFunctions();
    }

    @Override
    public Stream<UserFunctionSignature> getAllAggregatingFunctions()
    {
        init();
        return globalProcedures.getAllAggregatingFunctions();
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> callProcedure( Context ctx, int id, AnyValue[] input, ResourceTracker resourceTracker )
            throws ProcedureException
    {
        init();
        return globalProcedures.callProcedure( ctx, id, input, resourceTracker );
    }

    @Override
    public AnyValue callFunction( Context ctx, int id, AnyValue[] input ) throws ProcedureException
    {
        init();
        return globalProcedures.callFunction( ctx, id, input );
    }

    @Override
    public UserAggregator createAggregationFunction( Context ctx, int id ) throws ProcedureException
    {
        init();
        return globalProcedures.createAggregationFunction( ctx, id );
    }

    public <T> ThrowingFunction<Context,T,ProcedureException> lookupComponentProvider( Class<T> cls, boolean safe )
    {
        init();
        return globalProcedures.lookupComponentProvider(cls, safe);
    }

    @Override
    public void accept( Supplier<GlobalProcedures> procedureSupplier )
    {
        requireNonNull( procedureSupplier );
        if ( initializer != null )
        {
            throw new IllegalStateException( "Lazy procedures already have initializer: " + initializer );
        }
        initializer = procedureSupplier;
    }
}
