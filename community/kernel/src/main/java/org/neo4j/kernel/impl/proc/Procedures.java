/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.util.Optional;
import java.util.Set;

import org.neo4j.collection.RawIterator;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.kernel.api.proc.UserFunctionSignature;
import org.neo4j.kernel.builtinprocs.SpecialBuiltInProcedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

/**
 * This is the coordinating service for procedures in the database. It loads procedures from a specified
 * directory at startup, but also allows programmatic registration of them - and then, of course, allows
 * invoking procedures.
 */
public class Procedures extends LifecycleAdapter
{
    private final ProcedureRegistry registry = new ProcedureRegistry();
    private final TypeMappers typeMappers = new TypeMappers();
    private final ComponentRegistry components = new ComponentRegistry();
    private final ReflectiveProcedureCompiler compiler;
    private final ThrowingConsumer<Procedures, ProcedureException> builtin;
    private final File pluginDir;
    private final Log log;

    public Procedures()
    {
        this( new SpecialBuiltInProcedures( "N/A", "N/A" ), null, NullLog.getInstance(), ProcedureAllowedConfig.DEFAULT );
    }

    public Procedures( ThrowingConsumer<Procedures,ProcedureException> builtin, File pluginDir, Log log,
            ProcedureAllowedConfig config )
    {
        this.builtin = builtin;
        this.pluginDir = pluginDir;
        this.log = log;
        this.compiler = new ReflectiveProcedureCompiler( typeMappers, components, log, config );
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
     * Register a new procedure. This method must not be called concurrently with {@link #procedure(QualifiedName)}.
     * @param function the function.
     */
    public void register( CallableUserFunction function, boolean overrideCurrentImplementation ) throws ProcedureException
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
     * Register a new procedure defined with annotations on a java class.
     * @param proc the procedure class
     */
    public void registerProcedure( Class<?> proc ) throws KernelException
    {
        registerProcedure( proc, false );
    }

    /**
     * Register a new procedure defined with annotations on a java class.
     * @param proc the procedure class
     */
    public void registerProcedure( Class<?> proc, boolean overrideCurrentImplementation ) throws KernelException
    {
        for ( CallableProcedure procedure : compiler.compileProcedure( proc ) )
        {
            register( procedure, overrideCurrentImplementation );
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
     * Registers a type and how to convert it to a Neo4jType
     * @param javaClass the class of the native type
     * @param toNeo the conversion to Neo4jTypes
     */
    public void registerType( Class<?> javaClass, TypeMappers.NeoValueConverter toNeo )
    {
        typeMappers.registerType( javaClass, toNeo );
    }

    /**
     * Registers a component, these become available in reflective procedures for injection.
     *
     * @param cls the type of component to be registered (this is what users 'ask' for in their field declaration)
     * @param provider a function that supplies the component, given the context of a procedure invocation
     */
    public <T> void registerComponent( Class<T> cls, ComponentRegistry.Provider<T> provider )
    {
        components.register( cls, provider );
    }

    public ProcedureSignature procedure( QualifiedName name ) throws ProcedureException
    {
        return registry.procedure( name );
    }

    public Optional<UserFunctionSignature> function( QualifiedName name )
    {
        return registry.function( name );
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
                                                           Object[] input ) throws ProcedureException
    {
        return registry.callProcedure( ctx, name, input );
    }

    public Object callFunction( Context ctx, QualifiedName name,
            Object[] input ) throws ProcedureException
    {
        return registry.callFunction( ctx, name, input );
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

        // And register built-in procedures
        builtin.accept( this );
    }
}
