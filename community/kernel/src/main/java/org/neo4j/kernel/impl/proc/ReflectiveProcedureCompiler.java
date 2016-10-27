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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.FieldSignature;
import org.neo4j.kernel.api.proc.Mode;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.kernel.api.proc.UserFunctionSignature;
import org.neo4j.kernel.impl.proc.OutputMappers.OutputMapper;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static org.neo4j.helpers.collection.Iterators.asRawIterator;

/**
 * Handles converting a class into one or more callable {@link CallableProcedure}.
 */
public class ReflectiveProcedureCompiler
{
    private final MethodHandles.Lookup lookup = MethodHandles.lookup();
    private final OutputMappers outputMappers;
    private final MethodSignatureCompiler inputSignatureDeterminer;
    private final FieldInjections fieldInjections;
    private final Log log;
    private final TypeMappers typeMappers;
    private final ProcedureAllowedConfig config;

    public ReflectiveProcedureCompiler( TypeMappers typeMappers, ComponentRegistry components, Log log,
            ProcedureAllowedConfig config )
    {
        inputSignatureDeterminer = new MethodSignatureCompiler( typeMappers );
        outputMappers = new OutputMappers( typeMappers );
        this.fieldInjections = new FieldInjections( components );
        this.log = log;
        this.typeMappers = typeMappers;
        this.config = config;
    }

    public List<CallableUserFunction> compileFunction( Class<?> fcnDefinition ) throws KernelException
    {
        try
        {
            List<Method> procedureMethods = Arrays.stream( fcnDefinition.getDeclaredMethods() )
                    .filter( m -> m.isAnnotationPresent( UserFunction.class ) )
                    .collect( Collectors.toList() );

            if ( procedureMethods.isEmpty() )
            {
                return emptyList();
            }

            MethodHandle constructor = constructor( fcnDefinition );

            ArrayList<CallableUserFunction> out = new ArrayList<>( procedureMethods.size() );
            for ( Method method : procedureMethods )
            {
                out.add( compileFunction( fcnDefinition, constructor, method ) );
            }
            out.sort( ( a, b ) -> a.signature().name().toString().compareTo( b.signature().name().toString() ) );
            return out;
        }
        catch ( KernelException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed, e,
                    "Failed to compile function defined in `%s`: %s", fcnDefinition.getSimpleName(), e.getMessage() );
        }
    }

    public List<CallableProcedure> compileProcedure( Class<?> procDefinition ) throws KernelException
    {
        try
        {
            List<Method> procedureMethods = Arrays.stream( procDefinition.getDeclaredMethods() )
                    .filter( m -> m.isAnnotationPresent( Procedure.class ) )
                    .collect( Collectors.toList() );

            if ( procedureMethods.isEmpty() )
            {
                return emptyList();
            }

            MethodHandle constructor = constructor( procDefinition );

            ArrayList<CallableProcedure> out = new ArrayList<>( procedureMethods.size() );
            for ( Method method : procedureMethods )
            {
                out.add( compileProcedure( procDefinition, constructor, method ) );
            }
            out.sort( ( a, b ) -> a.signature().name().toString().compareTo( b.signature().name().toString() ) );
            return out;
        }
        catch ( KernelException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed, e,
                    "Failed to compile procedure defined in `%s`: %s", procDefinition.getSimpleName(), e.getMessage() );
        }
    }

    private ReflectiveProcedure compileProcedure( Class<?> procDefinition, MethodHandle constructor, Method method )
            throws ProcedureException, IllegalAccessException
    {
        String valueName = method.getAnnotation( Procedure.class ).value();
        String definedName = method.getAnnotation( Procedure.class ).name();
        QualifiedName procName = extractName( procDefinition, method, valueName, definedName );

        List<FieldSignature> inputSignature = inputSignatureDeterminer.signatureFor( method );
        OutputMapper outputMapper = outputMappers.mapper( method );
        MethodHandle procedureMethod = lookup.unreflect( method );
        List<FieldInjections.FieldSetter> setters = fieldInjections.setters( procDefinition );

        Optional<String> description = description( method );
        Procedure procedure = method.getAnnotation( Procedure.class );
        Mode mode = mode( procedure.mode() );
        if ( method.isAnnotationPresent( PerformsWrites.class ) )
        {
            if ( !procedure.mode().equals( org.neo4j.procedure.Mode.DEFAULT ) )
            {
                throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                        "Conflicting procedure annotation, cannot use PerformsWrites and mode" );
            }
            else
            {
                mode = Mode.READ_WRITE;
            }
        }

        Optional<String> deprecated = deprecated( method, procedure::deprecatedBy,
                "Use of @Procedure(deprecatedBy) without @Deprecated in " + procName );

        ProcedureSignature signature =
                new ProcedureSignature( procName, inputSignature, outputMapper.signature(),
                        mode, deprecated, config.rolesFor( procName.toString() ), description );

        return new ReflectiveProcedure( signature, constructor, procedureMethod, outputMapper, setters );
    }

    private ReflectiveUserFunction compileFunction( Class<?> procDefinition, MethodHandle constructor, Method method )
            throws ProcedureException, IllegalAccessException
    {
        String valueName = method.getAnnotation( UserFunction.class ).value();
        String definedName = method.getAnnotation( UserFunction.class ).name();
        QualifiedName procName = extractName( procDefinition, method, valueName, definedName );

        if (procName.namespace() == null || procName.namespace().length == 0)
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "It is not allowed to define functions in the root namespace please use a namespace, e.g. `@UserFunction(\"org.example.com.%s\")",
                    procName.name() );
        }

        List<FieldSignature> inputSignature = inputSignatureDeterminer.signatureFor( method );
        Class<?> returnType = method.getReturnType();
        TypeMappers.NeoValueConverter valueConverter = typeMappers.converterFor( returnType );
        MethodHandle procedureMethod = lookup.unreflect( method );
        List<FieldInjections.FieldSetter> setters = fieldInjections.setters( procDefinition );

        Optional<String> description = description( method );
        UserFunction function = method.getAnnotation( UserFunction.class );

        Optional<String> deprecated = deprecated( method, function::deprecatedBy,
                "Use of @UserFunction(deprecatedBy) without @Deprecated in " + procName );

        UserFunctionSignature signature =
                new UserFunctionSignature( procName, inputSignature, valueConverter.type(), deprecated,
                        config.rolesFor( procName.toString() ), description );

        return new ReflectiveUserFunction( signature, constructor, procedureMethod, valueConverter, setters );
    }

    private Optional<String> deprecated( Method method, Supplier<String> supplier, String warning )
    {
        String deprecatedBy = supplier.get();
        Optional<String> deprecated = Optional.empty();
        if ( method.isAnnotationPresent( Deprecated.class ) )
        {
            deprecated = Optional.of( deprecatedBy );
        }
        else if ( !deprecatedBy.isEmpty() )
        {
            log.warn( warning );
            deprecated = Optional.of( deprecatedBy );
        }

        return deprecated;
    }

    private Mode mode( org.neo4j.procedure.Mode incoming )
    {
        switch ( incoming )
        {
        case DBMS:
            return Mode.DBMS;
        case SCHEMA:
            return Mode.SCHEMA_WRITE;
        case WRITE:
            return Mode.READ_WRITE;
        default:
            return Mode.READ_ONLY;

        }
    }

    private Optional<String> description( Method method )
    {
        if ( method.isAnnotationPresent( Description.class ) )
        {
            return Optional.of( method.getAnnotation( Description.class ).value() );
        }
        else
        {
            return Optional.empty();
        }
    }

    private MethodHandle constructor( Class<?> procDefinition ) throws ProcedureException
    {
        try
        {
            return lookup.unreflectConstructor( procDefinition.getConstructor() );
        }
        catch ( IllegalAccessException | NoSuchMethodException e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed, e,
                    "Unable to find a usable public no-argument constructor in the class `%s`. " +
                    "Please add a valid, public constructor, recompile the class and try again.",
                    procDefinition.getSimpleName() );
        }
    }

    private QualifiedName extractName( Class<?> procDefinition, Method m, String valueName, String definedName )
    {
        String procName = (definedName.trim().isEmpty() ? valueName : definedName);
        if ( procName.trim().length() > 0 )
        {
            String[] split = procName.split( "\\." );
            if ( split.length == 1 )
            {
                return new QualifiedName( new String[0], split[0] );
            }
            else
            {
                int lastElement = split.length - 1;
                return new QualifiedName( Arrays.copyOf( split, lastElement ), split[lastElement] );
            }
        }
        Package pkg = procDefinition.getPackage();
        // Package is null if class is in root package
        String[] namespace = pkg == null ? new String[0] : pkg.getName().split( "\\." );
        String name = m.getName();
        return new QualifiedName( namespace, name );
    }

    private abstract static class ReflectiveBase
    {
        protected final MethodHandle constructor;
        protected final MethodHandle procedureMethod;
        protected final List<FieldInjections.FieldSetter> fieldSetters;

        protected ReflectiveBase( MethodHandle constructor, MethodHandle procedureMethod,
                List<FieldInjections.FieldSetter> fieldSetters )
        {
            this.constructor = constructor;
            this.procedureMethod = procedureMethod;
            this.fieldSetters = fieldSetters;
        }

        protected void inject( Context ctx, Object object ) throws ProcedureException
        {
            for ( FieldInjections.FieldSetter setter : fieldSetters )
            {
                setter.apply( ctx, object );
            }
        }

        protected Object[] args( int numberOfDeclaredArguments, Object cls, Object[] input )
        {
            Object[] args = new Object[numberOfDeclaredArguments + 1];
            args[0] = cls;
            System.arraycopy( input, 0, args, 1, numberOfDeclaredArguments );
            return args;
        }
    }

    private static class ReflectiveProcedure extends ReflectiveBase implements CallableProcedure
    {
        protected final ProcedureSignature signature;
        private final OutputMapper outputMapper;

        public ReflectiveProcedure( ProcedureSignature signature, MethodHandle constructor,
                MethodHandle procedureMethod, OutputMapper outputMapper,
                List<FieldInjections.FieldSetter> fieldSetters )
        {
            super( constructor, procedureMethod, fieldSetters );
            this.signature = signature;
            this.outputMapper = outputMapper;
        }

        @Override
        public ProcedureSignature signature()
        {
            return signature;
        }

        @Override
        public RawIterator<Object[],ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
        {
            // For now, create a new instance of the class for each invocation. In the future, we'd like to keep
            // instances local to
            // at least the executing session, but we don't yet have good interfaces to the kernel to model that with.
            try
            {
                int numberOfDeclaredArguments = signature.inputSignature().size();
                if ( numberOfDeclaredArguments != input.length )
                {
                    throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                            "Procedure `%s` takes %d arguments but %d was provided.",
                            signature.name(),
                            numberOfDeclaredArguments, input.length );
                }

                Object cls = constructor.invoke();
                //API injection
                inject( ctx, cls );

                // Call the method
                Object[] args = args( numberOfDeclaredArguments, cls, input );

                Object rs = procedureMethod.invokeWithArguments( args );

                // This also handles VOID
                if ( rs == null )
                {
                    return asRawIterator( emptyIterator() );
                }
                else
                {
                    return new MappingIterator( ((Stream<?>) rs).iterator() );
                }
            }
            catch ( Throwable throwable )
            {
                if ( throwable instanceof Status.HasStatus )
                {
                    throw new ProcedureException( ((Status.HasStatus) throwable).status(), throwable,
                            throwable.getMessage() );
                }
                else
                {
                    throw new ProcedureException( Status.Procedure.ProcedureCallFailed, throwable,
                            "Failed to invoke procedure `%s`: %s", signature.name(), "Caused by: " + throwable );
                }
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
                catch ( RuntimeException e )
                {
                    throw new ProcedureException( Status.Procedure.ProcedureCallFailed, e,
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
                catch ( RuntimeException e )
                {
                    throw new ProcedureException( Status.Procedure.ProcedureCallFailed, e,
                            "Failed to call procedure `%s`: %s", signature, e.getMessage() );
                }
            }
        }
    }

    private static class ReflectiveUserFunction extends ReflectiveBase implements CallableUserFunction
    {

        private final TypeMappers.NeoValueConverter valueConverter;
        private final UserFunctionSignature signature;

        public ReflectiveUserFunction( UserFunctionSignature signature, MethodHandle constructor,
                MethodHandle procedureMethod, TypeMappers.NeoValueConverter outputMapper,
                List<FieldInjections.FieldSetter> fieldSetters )
        {
            super( constructor, procedureMethod, fieldSetters );
            this.signature = signature;
            this.valueConverter = outputMapper;
        }

        @Override
        public UserFunctionSignature signature()
        {
            return signature;
        }

        @Override
        public Object apply( Context ctx, Object[] input ) throws ProcedureException
        {
            // For now, create a new instance of the class for each invocation. In the future, we'd like to keep
            // instances local to
            // at least the executing session, but we don't yet have good interfaces to the kernel to model that with.
            try
            {
                int numberOfDeclaredArguments = signature.inputSignature().size();
                if ( numberOfDeclaredArguments != input.length )
                {
                    throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                            "Function `%s` takes %d arguments but %d was provided.",
                            signature.name(),
                            numberOfDeclaredArguments, input.length );
                }

                Object cls = constructor.invoke();
                //API injection
                inject( ctx, cls );

                // Call the method
                Object[] args = args( numberOfDeclaredArguments, cls, input );

                Object rs = procedureMethod.invokeWithArguments( args );

                return valueConverter.toNeoValue( rs );
            }
            catch ( Throwable throwable )
            {
                if ( throwable instanceof Status.HasStatus )
                {
                    throw new ProcedureException( ((Status.HasStatus) throwable).status(), throwable,
                            throwable.getMessage() );
                }
                else
                {
                    throw new ProcedureException( Status.Procedure.ProcedureCallFailed, throwable,
                            "Failed to invoke function `%s`: %s", signature.name(), "Caused by: " + throwable );
                }
            }
        }
    }
}
