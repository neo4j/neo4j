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
import java.io.IOException;
import java.util.Set;

import org.neo4j.collection.RawIterator;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.Procedure;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;


public class Procedures
{
    private final ProcedureRegistry registry = new ProcedureRegistry();
    private final TypeMappers typeMappers = new TypeMappers();
    private final ComponentRegistry components = new ComponentRegistry();
    private final ReflectiveProcedureCompiler compiler = new ReflectiveProcedureCompiler(typeMappers, components);
    private final Log log;

    public Procedures()
    {
        this( NullLog.getInstance() );
    }

    public Procedures( Log log )
    {
        this.log = log;
    }

    /**
     * Register a new procedure. This method must not be called concurrently with {@link #get(ProcedureSignature.ProcedureName)}.
     * @param proc the procedure.
     */
    public synchronized void register( Procedure proc ) throws ProcedureException
    {
        registry.register( proc );
    }

    /**
     * Register a new procedure defined with annotations on a java class.
     * @param proc the procedure class
     */
    public synchronized void register( Class<?> proc ) throws KernelException
    {
        for ( Procedure procedure : compiler.compile( proc ) )
        {
            register( procedure );
        }
    }

    /**
     * Registers a type and how to convert it to a Neo4jType
     * @param javaClass the class of the native type
     * @param toNeo the conversion to Neo4jTypes
     */
    public synchronized void registerType( Class<?> javaClass, TypeMappers.NeoValueConverter toNeo )
    {
        typeMappers.registerType( javaClass, toNeo );
    }

    /**
     * Registers a component, these become available in reflective procedures for injection.
     *
     * @param cls the type of component to be registered (this is what users 'ask' for in their field declaration)
     * @param supplier a function that supplies the actual component, given the context of a procedure invocation
     */
    public synchronized <T> void registerComponent( Class<T> cls, ThrowingFunction<Procedure.Context, T, ProcedureException> supplier )
    {
        components.register( cls, supplier );
    }

    /**
     * Find all procedure-annotated methods in jar files in the given directory and register them.
     * @param dir directory to look for jarfiles in
     * @throws IOException
     * @throws KernelException
     */
    public synchronized void loadFromDirectory( File dir ) throws IOException, KernelException
    {
        for ( Procedure procedure : new ProcedureJarLoader( compiler, log ).loadProceduresFromDir( dir ) )
        {
            register( procedure );
        }
    }

    public ProcedureSignature get( ProcedureSignature.ProcedureName name ) throws ProcedureException
    {
        return registry.get( name );
    }

    public Set<ProcedureSignature> getAll()
    {
        return registry.getAll();
    }

    public RawIterator<Object[], ProcedureException> call( Procedure.Context ctx, ProcedureSignature.ProcedureName name,
                                                           Object[] input ) throws ProcedureException
    {
        return registry.call( ctx, name, input );
    }
}
