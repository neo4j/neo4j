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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.Procedure;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.proc.ProcedureSignature.FieldSignature;
import org.neo4j.kernel.api.proc.ProcedureSignature.ProcedureName;
import org.neo4j.kernel.impl.proc.OutputMappers.OutputMapper;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

/**
 * Handles converting a class into one or more callable {@link Procedure}.
 */
public class ReflectiveProcedureCompiler
{
    private final MethodHandles.Lookup lookup = MethodHandles.lookup();
    private final OutputMappers outputMappers;
    private final MethodSignatureCompiler inputSignatureDeterminer;

    public ReflectiveProcedureCompiler( TypeMappers typeMappers )
    {
        inputSignatureDeterminer = new MethodSignatureCompiler(typeMappers);
        outputMappers = new OutputMappers( typeMappers );
    }

    public List<Procedure> compile( Class<?> procDefinition ) throws KernelException
    {
        try
        {
            List<Method> procedureMethods = asList( procDefinition.getDeclaredMethods() ).stream()
                    .filter( m -> m.isAnnotationPresent( ReadOnlyProcedure.class ) )
                    .collect( Collectors.toList() );

            if( procedureMethods.isEmpty() )
            {
                return emptyList();
            }

            MethodHandle constructor = constructor( procDefinition );

            ArrayList<Procedure> out = new ArrayList<>( procedureMethods.size() );
            for ( Method method : procedureMethods )
            {
                out.add( compileProcedure( procDefinition, constructor, method ) );
            }
            out.sort( (a,b) -> a.signature().name().toString().compareTo( b.signature().name().toString() ) );
            return out;
        }
        catch( KernelException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new ProcedureException( Status.Procedure.FailedRegistration, e,
                    "Failed to compile procedure defined in `%s`: %s", procDefinition.getSimpleName(), e.getMessage() );
        }
    }

    private ReflectiveProcedure compileProcedure( Class<?> procDefinition, MethodHandle constructor, Method method )
            throws ProcedureException, IllegalAccessException
    {
        ProcedureName procName = extractName( procDefinition, method );

        List<FieldSignature> inputSignature = inputSignatureDeterminer.signatureFor( method );
        OutputMapper outputMapper = outputMappers.mapper( method );
        MethodHandle procedureMethod = lookup.unreflect( method );

        ProcedureSignature signature = new ProcedureSignature( procName, inputSignature, outputMapper.signature() );

        return new ReflectiveProcedure( signature, constructor, procedureMethod, outputMapper );
    }

    private MethodHandle constructor( Class<?> procDefinition ) throws ProcedureException
    {
        try
        {
            return lookup.unreflectConstructor( procDefinition.getConstructor() );
        }
        catch ( IllegalAccessException | NoSuchMethodException e )
        {
            throw new ProcedureException( Status.Procedure.FailedRegistration, e,
                    "Unable to find a usable public no-argument constructor in the class `%s`. " +
                    "Please add a valid, public constructor, recompile the class and try again.",
                    procDefinition.getSimpleName() );
        }
    }

    private ProcedureName extractName( Class<?> procDefinition, Method m )
    {
        String[] namespace = procDefinition.getPackage().getName().split( "\\." );
        String name = m.getName();
        return new ProcedureName( namespace, name );
    }

    private static class ReflectiveProcedure implements Procedure
    {
        private final ProcedureSignature signature;
        private final MethodHandle constructor;
        private final MethodHandle procedureMethod;
        private final OutputMapper outputMapper;


        public ReflectiveProcedure( ProcedureSignature signature, MethodHandle constructor,
                                    MethodHandle procedureMethod, OutputMapper outputMapper )
        {
            this.signature = signature;
            this.constructor = constructor;
            this.procedureMethod = procedureMethod;
            this.outputMapper = outputMapper;
        }

        @Override
        public ProcedureSignature signature()
        {
            return signature;
        }

        @Override
        public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
        {
            // For now, create a new instance of the class for each invocation. In the future, we'd like to keep instances local to
            // at least the executing session, but we don't yet have good interfaces to the kernel to model that with.
            try
            {
                Object cls = constructor.invoke();

                Object[] args = new Object[signature.inputSignature().size() + 1];
                args[0] = cls;
                System.arraycopy( input, 0, args, 1, input.length );

                Iterator<?> out = ((Stream<?>) procedureMethod.invokeWithArguments( args )).iterator();

                return new MappingIterator( out );
            }
            catch ( Throwable throwable )
            {
                throw new ProcedureException( Status.Procedure.CallFailed, throwable,
                        "Failed to invoke procedure `%s`: %s", signature, throwable.getMessage() );
            }
        }

        private class MappingIterator implements RawIterator<Object[],ProcedureException>
        {
            private final Iterator<?> out;

            public MappingIterator( Iterator<?> out )
            {
                this.out = out;
            }

            @Override
            public boolean hasNext() throws ProcedureException
            {
                try
                {
                    return out.hasNext();
                }
                catch( RuntimeException e )
                {
                    throw new ProcedureException( Status.Procedure.CallFailed, e,
                            "Failed to call procedure `%s`: %s", signature, e.getMessage() );
                }
            }

            @Override
            public Object[] next() throws ProcedureException
            {
                try
                {
                    Object record = out.next();
                    return outputMapper.apply( record );
                }
                catch( RuntimeException e )
                {
                    throw new ProcedureException( Status.Procedure.CallFailed, e,
                            "Failed to call procedure `%s`: %s", signature, e.getMessage() );
                }
            }
        }
    }
}
