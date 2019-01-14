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
package org.neo4j.kernel.impl.proc;

import java.io.File;
import java.util.Set;

import org.neo4j.collection.RawIterator;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.builtinprocs.SpecialBuiltInProcedures;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;

/**
 * This is the coordinating service for procedures in the database. It loads procedures from a specified
 * directory at startup, but also allows programmatic registration of them - and then, of course, allows
 * invoking procedures.
 */
public class Procedures extends LifecycleAdapter
{
    private final ProcedureRegistry registry = new ProcedureRegistry();
    private final TypeMappers typeMappers;
    private final ComponentRegistry safeComponents = new ComponentRegistry();
    private final ComponentRegistry allComponents = new ComponentRegistry();
    private final ReflectiveProcedureCompiler compiler;
    private final ThrowingConsumer<Procedures, ProcedureException> builtin;
    private final File pluginDir;
    private final Log log;

    /**
     * Used by testing.
     */
    public Procedures()
    {
        this( null, new SpecialBuiltInProcedures( "N/A", "N/A" ), null, NullLog.getInstance(), ProcedureConfig.DEFAULT );
    }

    public Procedures(
            EmbeddedProxySPI proxySPI,
            ThrowingConsumer<Procedures,ProcedureException> builtin,
            File pluginDir,
            Log log,
            ProcedureConfig config )
    {
        this.builtin = builtin;
        this.pluginDir = pluginDir;
        this.log = log;
        this.typeMappers = new TypeMappers( proxySPI );
        this.compiler = new ReflectiveProcedureCompiler( typeMappers, safeComponents, allComponents, log, config );
    }

    /**
     * Register a new procedure. This method must not be called concurrently with {@link #procedure(QualifiedName)}.
     * @param proc the procedure.
     */
    public void register( CallableProcedure proc ) throws ProcedureException
    {
        register( proc, false );
    }

    /**
     * Register a new function. This method must not be called concurrently with {@link #procedure(QualifiedName)}.
     * @param function the fucntion.
     */
    public void register( CallableUserFunction function ) throws ProcedureException
    {
        register( function, false );
    }

    /**
     * Register a new function. This method must not be called concurrently with {@link #procedure(QualifiedName)}.
     * @param function the fucntion.
     */
    public void register( CallableUserAggregationFunction function ) throws ProcedureException
    {
        register( function, false );
    }

    /**
     * Register a new procedure. This method must not be called concurrently with {@link #procedure(QualifiedName)}.
     * @param function the function.
     */
    public void register( CallableUserFunction function, boolean overrideCurrentImplementation ) throws ProcedureException
    {
        registry.register( function, overrideCurrentImplementation );
    }

    /**
     * Register a new procedure. This method must not be called concurrently with {@link #procedure(QualifiedName)}.
     * @param function the function.
     */
    public void register( CallableUserAggregationFunction function, boolean overrideCurrentImplementation ) throws ProcedureException
    {
        registry.register( function, overrideCurrentImplementation );
    }

    /**
     * Register a new procedure. This method must not be called concurrently with {@link #procedure(QualifiedName)}.
     * @param proc the procedure.
     */
    public void register( CallableProcedure proc, boolean overrideCurrentImplementation ) throws ProcedureException
    {
        registry.register( proc, overrideCurrentImplementation );
    }

    /**
     * Register a new internal procedure defined with annotations on a java class.
     * @param proc the procedure class
     */
    public void registerProcedure( Class<?> proc ) throws KernelException
    {
        registerProcedure( proc, false );
    }

    /**
     * Register a new internal procedure defined with annotations on a java class.
     * @param proc the procedure class
     * @param overrideCurrentImplementation set to true if procedures within this class should override older procedures with the same name
     */
    public void registerProcedure( Class<?> proc, boolean overrideCurrentImplementation ) throws KernelException
    {
        registerProcedure( proc, overrideCurrentImplementation, null );
    }

    /**
     * Register a new internal procedure defined with annotations on a java class.
     * @param proc the procedure class
     * @param overrideCurrentImplementation set to true if procedures within this class should override older procedures with the same name
     * @param warning the warning the procedure should generate when called
     */
    public void registerProcedure( Class<?> proc, boolean overrideCurrentImplementation, String warning ) throws KernelException
    {
        for ( CallableProcedure procedure : compiler.compileProcedure( proc, warning, true ) )
        {
            register( procedure, overrideCurrentImplementation );
        }
    }

    /**
     * Register a new function defined with annotations on a java class.
     * @param func the function class
     */
    public void registerBuiltInFunctions( Class<?> func ) throws KernelException
    {
        for ( CallableUserFunction function : compiler.withoutNamingRestrictions().compileFunction( func ) )
        {
            register( function, false );
        }
    }

    /**
     * Register a new function defined with annotations on a java class.
     * @param func the function class
     */
    public void registerFunction( Class<?> func ) throws KernelException
    {
        registerFunction( func, false );
    }

    /**
     * Register a new aggregation function defined with annotations on a java class.
     * @param func the function class
     */
    public void registerAggregationFunction( Class<?> func, boolean overrideCurrentImplementation ) throws KernelException
    {
        for ( CallableUserAggregationFunction function : compiler.compileAggregationFunction( func ) )
        {
            register( function, overrideCurrentImplementation );
        }
    }

    /**
     * Register a new aggregation function defined with annotations on a java class.
     * @param func the function class
     */
    public void registerAggregationFunction( Class<?> func ) throws KernelException
    {
        registerAggregationFunction( func, false );
    }

    /**
     * Register a new function defined with annotations on a java class.
     * @param func the function class
     */
    public void registerFunction( Class<?> func, boolean overrideCurrentImplementation ) throws KernelException
    {
        for ( CallableUserFunction function : compiler.compileFunction( func ) )
        {
            register( function, overrideCurrentImplementation );
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
    public void registerType( Class<?> javaClass, Neo4jTypes.AnyType type )
    {
        typeMappers.registerType( javaClass, new TypeMappers.DefaultValueConverter( type, javaClass ) );
    }

    /**
     * Registers a component, these become available in reflective procedures for injection.
     * @param cls the type of component to be registered (this is what users 'ask' for in their field declaration)
     * @param provider a function that supplies the component, given the context of a procedure invocation
     * @param safe set to false if this component can bypass security, true if it respects security
     */
    public <T> void registerComponent( Class<T> cls, ComponentRegistry.Provider<T> provider, boolean safe )
    {
        if ( safe )
        {
            safeComponents.register( cls, provider );
        }
        allComponents.register( cls, provider );
    }

    public ProcedureHandle procedure( QualifiedName name ) throws ProcedureException
    {
        return registry.procedure( name );
    }

    public UserFunctionHandle function( QualifiedName name )
    {
        return registry.function( name );
    }

    public UserFunctionHandle aggregationFunction( QualifiedName name )
    {
        return registry.aggregationFunction( name );
    }

    public Set<ProcedureSignature> getAllProcedures()
    {
        return registry.getAllProcedures();
    }

    public Set<UserFunctionSignature> getAllFunctions()
    {
        return registry.getAllFunctions();
    }

    public RawIterator<Object[], ProcedureException> callProcedure( Context ctx, QualifiedName name,
                                                           Object[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        return registry.callProcedure( ctx, name, input, resourceTracker );
    }

    public RawIterator<Object[], ProcedureException> callProcedure( Context ctx, int id,
            Object[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        return registry.callProcedure( ctx, id, input, resourceTracker );
    }

    public AnyValue callFunction( Context ctx, QualifiedName name, AnyValue[] input ) throws ProcedureException
    {
        return registry.callFunction( ctx, name, input );
    }

    public AnyValue callFunction( Context ctx, int id, AnyValue[] input ) throws ProcedureException
    {
        return registry.callFunction( ctx, id, input );
    }

    public UserAggregator createAggregationFunction( Context ctx, QualifiedName name ) throws ProcedureException
    {
        return registry.createAggregationFunction( ctx, name );
    }

    public UserAggregator createAggregationFunction( Context ctx, int id ) throws ProcedureException
    {
        return registry.createAggregationFunction( ctx, id );
    }

    public ValueMapper<Object> valueMapper()
    {
        return typeMappers;
    }

    @Override
    public void start() throws Throwable
    {

        ProcedureJarLoader loader = new ProcedureJarLoader( compiler, log );
        ProcedureJarLoader.Callables callables = loader.loadProceduresFromDir( pluginDir );
        for ( CallableProcedure procedure : callables.procedures() )
        {
            register( procedure );
        }

        for ( CallableUserFunction function : callables.functions() )
        {
            register( function );
        }

        for ( CallableUserAggregationFunction function : callables.aggregationFunctions() )
        {
            register( function );
        }

        // And register built-in procedures
        builtin.accept( this );
    }
}
