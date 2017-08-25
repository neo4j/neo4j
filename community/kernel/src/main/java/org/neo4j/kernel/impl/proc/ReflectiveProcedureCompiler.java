/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.ComponentInjectionException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.FailedLoadAggregatedFunction;
import org.neo4j.kernel.api.proc.FailedLoadFunction;
import org.neo4j.kernel.api.proc.FailedLoadProcedure;
import org.neo4j.kernel.api.proc.FieldSignature;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.kernel.api.proc.UserFunctionSignature;
import org.neo4j.kernel.impl.proc.OutputMappers.OutputMapper;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.procedure.UserFunction;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.procedure_unrestricted;
import static org.neo4j.helpers.collection.Iterators.asRawIterator;

/**
 * Handles converting a class into one or more callable {@link CallableProcedure}.
 */
class ReflectiveProcedureCompiler
{
    private final MethodHandles.Lookup lookup = MethodHandles.lookup();
    private final OutputMappers outputMappers;
    private final MethodSignatureCompiler inputSignatureDeterminer;
    private final FieldInjections safeFieldInjections;
    private final FieldInjections allFieldInjections;
    private final Log log;
    private final TypeMappers typeMappers;
    private final ProcedureConfig config;

    ReflectiveProcedureCompiler( TypeMappers typeMappers, ComponentRegistry safeComponents,
            ComponentRegistry allComponents, Log log, ProcedureConfig config )
    {
        inputSignatureDeterminer = new MethodSignatureCompiler( typeMappers );
        outputMappers = new OutputMappers( typeMappers );
        this.safeFieldInjections = new FieldInjections( safeComponents );
        this.allFieldInjections = new FieldInjections( allComponents );
        this.log = log;
        this.typeMappers = typeMappers;
        this.config = config;
    }

    List<CallableUserFunction> compileFunction( Class<?> fcnDefinition ) throws KernelException
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
                String valueName = method.getAnnotation( UserFunction.class ).value();
                String definedName = method.getAnnotation( UserFunction.class ).name();
                QualifiedName procName = extractName( fcnDefinition, method, valueName, definedName );
                if ( config.isWhitelisted( procName.toString() ) )
                {
                    out.add( compileFunction( fcnDefinition, constructor, method,procName ) );
                }
                else
                {
                    log.warn( String.format( "The function '%s' is not on the whitelist and won't be loaded.",
                            procName.toString() ) );
                }
            }
            out.sort( Comparator.comparing( a -> a.signature().name().toString() ) );
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

    List<CallableUserAggregationFunction> compileAggregationFunction( Class<?> fcnDefinition ) throws KernelException
    {
        try
        {
            List<Method> methods = Arrays.stream( fcnDefinition.getDeclaredMethods() )
                    .filter( m -> m.isAnnotationPresent( UserAggregationFunction.class ) )
                    .collect( Collectors.toList() );

            if ( methods.isEmpty() )
            {
                return emptyList();
            }

            MethodHandle constructor = constructor( fcnDefinition );

            ArrayList<CallableUserAggregationFunction> out = new ArrayList<>( methods.size() );
            for ( Method method : methods )
            {
                String valueName = method.getAnnotation( UserAggregationFunction.class ).value();
                String definedName = method.getAnnotation( UserAggregationFunction.class ).name();
                QualifiedName funcName = extractName( fcnDefinition, method, valueName, definedName );

                if ( config.isWhitelisted( funcName.toString() ) )
                {
                    out.add( compileAggregationFunction( fcnDefinition, constructor, method, funcName ) );
                }
                else
                {
                    log.warn( String.format( "The function '%s' is not on the whitelist and won't be loaded.",
                            funcName.toString() ) );
                }

            }
            out.sort( Comparator.comparing( a -> a.signature().name().toString() ) );
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

    List<CallableProcedure> compileProcedure( Class<?> procDefinition, Optional<String> warning,
            boolean fullAccess ) throws KernelException
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
                String valueName = method.getAnnotation( Procedure.class ).value();
                String definedName = method.getAnnotation( Procedure.class ).name();
                QualifiedName procName = extractName( procDefinition, method, valueName, definedName );

                if ( fullAccess || config.isWhitelisted( procName.toString() ) )
                {
                    out.add( compileProcedure( procDefinition, constructor, method, warning, fullAccess, procName ) );
                }
                else
                {
                    log.warn( String.format( "The procedure '%s' is not on the whitelist and won't be loaded.",
                            procName.toString() ) );
                }
            }
            out.sort( Comparator.comparing( a -> a.signature().name().toString() ) );
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

    private CallableProcedure compileProcedure( Class<?> procDefinition, MethodHandle constructor, Method method,
            Optional<String> warning, boolean fullAccess, QualifiedName procName  )
            throws ProcedureException, IllegalAccessException
    {
        MethodHandle procedureMethod = lookup.unreflect( method );

        List<FieldSignature> inputSignature = inputSignatureDeterminer.signatureFor( method );
        OutputMapper outputMapper = outputMappers.mapper( method );

        Optional<String> description = description( method );
        Procedure procedure = method.getAnnotation( Procedure.class );
        Mode mode = procedure.mode();
        if ( method.isAnnotationPresent( PerformsWrites.class ) )
        {
            if ( !procedure.mode().equals( org.neo4j.procedure.Mode.DEFAULT ) )
            {
                throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                        "Conflicting procedure annotation, cannot use PerformsWrites and mode" );
            }
            else
            {
                mode = Mode.WRITE;
            }
        }

        Optional<String> deprecated = deprecated( method, procedure::deprecatedBy,
                "Use of @Procedure(deprecatedBy) without @Deprecated in " + procName );

        List<FieldInjections.FieldSetter> setters = allFieldInjections.setters( procDefinition );
        if ( !fullAccess && !config.fullAccessFor( procName.toString() ) )
        {
            try
            {
                setters = safeFieldInjections.setters( procDefinition );
            }
            catch ( ComponentInjectionException e )
            {
                description = describeAndLogLoadFailure( procName );
                ProcedureSignature signature =
                        new ProcedureSignature( procName, inputSignature, outputMapper.signature(), Mode.DEFAULT,
                                Optional.empty(), new String[0], description, warning );
                return new FailedLoadProcedure( signature );
            }
        }

        ProcedureSignature signature =
                new ProcedureSignature( procName, inputSignature, outputMapper.signature(), mode, deprecated,
                        config.rolesFor( procName.toString() ), description, warning );
        return new ReflectiveProcedure( signature, constructor, procedureMethod, outputMapper, setters );
    }

    private Optional<String> describeAndLogLoadFailure( QualifiedName name )
    {
        String nameStr = name.toString();
        Optional<String> description = Optional.of(
                nameStr + " is unavailable because it is sandboxed and has dependencies outside of the sandbox. " +
                "Sandboxing is controlled by the " + procedure_unrestricted.name() + " setting. " +
                "Only unrestrict procedures you can trust with access to database internals." );
        log.warn( description.get() );
        return description;
    }

    private CallableUserFunction compileFunction( Class<?> procDefinition, MethodHandle constructor, Method method,
            QualifiedName procName )
            throws ProcedureException, IllegalAccessException
    {

        if ( procName.namespace() == null || procName.namespace().length == 0 )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "It is not allowed to define functions in the root namespace please use a namespace, " +
                            "e.g. `@UserFunction(\"org.example.com.%s\")", procName.name() );
        }

        List<FieldSignature> inputSignature = inputSignatureDeterminer.signatureFor( method );
        Class<?> returnType = method.getReturnType();
        TypeMappers.NeoValueConverter valueConverter = typeMappers.converterFor( returnType );
        MethodHandle procedureMethod = lookup.unreflect( method );
        Optional<String> description = description( method );
        UserFunction function = method.getAnnotation( UserFunction.class );
        Optional<String> deprecated = deprecated( method, function::deprecatedBy,
                "Use of @UserFunction(deprecatedBy) without @Deprecated in " + procName );

        List<FieldInjections.FieldSetter> setters = allFieldInjections.setters( procDefinition );
        if ( !config.fullAccessFor( procName.toString() ) )
        {
            try
            {
                setters = safeFieldInjections.setters( procDefinition );
            }
            catch ( ComponentInjectionException e )
            {
                description = describeAndLogLoadFailure( procName );
                UserFunctionSignature signature =
                        new UserFunctionSignature( procName, inputSignature, valueConverter.type(), deprecated,
                                config.rolesFor( procName.toString() ), description );
                return new FailedLoadFunction( signature );
            }
        }

        UserFunctionSignature signature =
                new UserFunctionSignature( procName, inputSignature, valueConverter.type(), deprecated,
                        config.rolesFor( procName.toString() ), description );

        return new ReflectiveUserFunction( signature, constructor, procedureMethod, valueConverter, setters );
    }

    private CallableUserAggregationFunction compileAggregationFunction( Class<?> definition, MethodHandle constructor,
            Method method, QualifiedName funcName ) throws ProcedureException, IllegalAccessException
    {
        if ( funcName.namespace() == null || funcName.namespace().length == 0 )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "It is not allowed to define functions in the root namespace please use a namespace, " +
                            "e.g. `@UserFunction(\"org.example.com.%s\")", funcName.name() );
        }

        //find update and result method
        Method update = null;
        Method result = null;
        Class<?> aggregator = method.getReturnType();
        for ( Method m : aggregator.getDeclaredMethods() )
        {
            if ( m.isAnnotationPresent( UserAggregationUpdate.class ) )
            {
                if ( update != null )
                {
                    throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                            "Class '%s' contains multiple methods annotated with '@%s'.", aggregator.getSimpleName(),
                            UserAggregationUpdate.class.getSimpleName() );
                }
                update = m;

            }
            if ( m.isAnnotationPresent( UserAggregationResult.class ) )
            {
                if ( result != null )
                {
                    throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                            "Class '%s' contains multiple methods annotated with '@%s'.", aggregator.getSimpleName(),
                            UserAggregationResult.class.getSimpleName() );
                }
                result = m;
            }
        }
        if ( result == null || update == null )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Class '%s' must contain methods annotated with both '@%s' as well as '@%s'.",
                    aggregator.getSimpleName(), UserAggregationResult.class.getSimpleName(),
                    UserAggregationUpdate.class.getSimpleName() );
        }
        if ( update.getReturnType() != void.class )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Update method '%s' in %s has type '%s' but must have return type 'void'.", update.getName(),
                    aggregator.getSimpleName(), update.getReturnType().getSimpleName() );

        }
        if ( !Modifier.isPublic( method.getModifiers() ) )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Aggregation method '%s' in %s must be public.", method.getName(), definition.getSimpleName() );
        }
        if ( !Modifier.isPublic( aggregator.getModifiers() ) )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Aggregation class '%s' must be public.", aggregator.getSimpleName() );
        }
        if ( !Modifier.isPublic( update.getModifiers() ) )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Aggregation update method '%s' in %s must be public.", update.getName(),
                    aggregator.getSimpleName() );
        }
        if ( !Modifier.isPublic( result.getModifiers() ) )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Aggregation result method '%s' in %s must be public.", result.getName(),
                    aggregator.getSimpleName() );
        }

        List<FieldSignature> inputSignature = inputSignatureDeterminer.signatureFor( update );
        Class<?> returnType = result.getReturnType();
        TypeMappers.NeoValueConverter valueConverter = typeMappers.converterFor( returnType );
        MethodHandle creator = lookup.unreflect( method );
        MethodHandle updateMethod = lookup.unreflect( update );
        MethodHandle resultMethod = lookup.unreflect( result );

        Optional<String> description = description( method );
        UserAggregationFunction function = method.getAnnotation( UserAggregationFunction.class );

        Optional<String> deprecated = deprecated( method, function::deprecatedBy,
                "Use of @UserAggregationFunction(deprecatedBy) without @Deprecated in " + funcName );

        List<FieldInjections.FieldSetter> setters = allFieldInjections.setters( definition );
        if ( !config.fullAccessFor( funcName.toString() ) )
        {
            try
            {
                setters = safeFieldInjections.setters( definition );
            }
            catch ( ComponentInjectionException e )
            {
                description = describeAndLogLoadFailure( funcName );
                UserFunctionSignature signature =
                        new UserFunctionSignature( funcName, inputSignature, valueConverter.type(), deprecated,
                                config.rolesFor( funcName.toString() ), description );

                return new FailedLoadAggregatedFunction( signature );
            }
        }

        UserFunctionSignature signature =
                new UserFunctionSignature( funcName, inputSignature, valueConverter.type(), deprecated,
                        config.rolesFor( funcName.toString() ), description );

        return new ReflectiveUserAggregationFunction( signature, constructor, creator, updateMethod, resultMethod,
                valueConverter, setters );
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
        String procName = definedName.trim().isEmpty() ? valueName : definedName;
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

        final List<FieldInjections.FieldSetter> fieldSetters;

        ReflectiveBase( List<FieldInjections.FieldSetter> fieldSetters )
        {
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
        private final ProcedureSignature signature;
        private final OutputMapper outputMapper;
        private final MethodHandle constructor;
        private final MethodHandle procedureMethod;

        ReflectiveProcedure( ProcedureSignature signature, MethodHandle constructor,
                MethodHandle procedureMethod, OutputMapper outputMapper,
                List<FieldInjections.FieldSetter> fieldSetters )
        {
            super( fieldSetters );
            this.constructor = constructor;
            this.procedureMethod = procedureMethod;
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
                    return new MappingIterator( ((Stream<?>) rs).iterator(), () -> ((Stream<?>) rs).close() );
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

        private class MappingIterator implements RawIterator<Object[],ProcedureException>, AutoCloseable
        {
            private final Iterator<?> out;
            private Closeable closeable;

            MappingIterator( Iterator<?> out, Closeable closeable )
            {
                this.out = out;
                this.closeable = closeable;
            }

            @Override
            public boolean hasNext() throws ProcedureException
            {
                try
                {
                    boolean hasNext = out.hasNext();
                    if ( !hasNext )
                    {
                        closeable.close();
                    }
                    return hasNext;
                }
                catch ( RuntimeException | IOException e )
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

            @Override
            public void close() throws Exception
            {
                closeable.close();
            }
        }
    }

    private static class ReflectiveUserFunction extends ReflectiveBase implements CallableUserFunction
    {

        private final TypeMappers.NeoValueConverter valueConverter;
        private final UserFunctionSignature signature;
        private final MethodHandle constructor;
        private final MethodHandle udfMethod;

        ReflectiveUserFunction( UserFunctionSignature signature, MethodHandle constructor,
                MethodHandle procedureMethod, TypeMappers.NeoValueConverter outputMapper,
                List<FieldInjections.FieldSetter> fieldSetters )
        {
            super( fieldSetters );
            this.constructor = constructor;
            this.udfMethod = procedureMethod;
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

                Object rs = udfMethod.invokeWithArguments( args );

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

    private static class ReflectiveUserAggregationFunction extends ReflectiveBase implements
            CallableUserAggregationFunction
    {

        private final TypeMappers.NeoValueConverter valueConverter;
        private final UserFunctionSignature signature;
        private final MethodHandle constructor;
        private final MethodHandle creator;
        private final MethodHandle updateMethod;
        private final MethodHandle resultMethod;

        ReflectiveUserAggregationFunction( UserFunctionSignature signature, MethodHandle constructor,
                MethodHandle creator, MethodHandle updateMethod, MethodHandle resultMethod,
                TypeMappers.NeoValueConverter outputMapper,
                List<FieldInjections.FieldSetter> fieldSetters )
        {
            super( fieldSetters );
            this.constructor = constructor;
            this.creator = creator;
            this.updateMethod = updateMethod;
            this.resultMethod = resultMethod;
            this.signature = signature;
            this.valueConverter = outputMapper;
        }

        @Override
        public UserFunctionSignature signature()
        {
            return signature;
        }

        @Override
        public Aggregator create( Context ctx ) throws ProcedureException
        {
            // For now, create a new instance of the class for each invocation. In the future, we'd like to keep
            // instances local to
            // at least the executing session, but we don't yet have good interfaces to the kernel to model that with.
            try
            {

                Object cls = constructor.invoke();
                //API injection
                inject( ctx, cls );
                Object aggregator = creator.invoke( cls );

                return new Aggregator()
                {
                    @Override
                    public void update( Object[] input ) throws ProcedureException
                    {
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
                            // Call the method
                            Object[] args = args( numberOfDeclaredArguments, aggregator, input );

                            updateMethod.invokeWithArguments( args );
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
                                        "Failed to invoke function `%s`: %s", signature.name(),
                                        "Caused by: " + throwable );
                            }
                        }
                    }

                    @Override
                    public Object result() throws ProcedureException
                    {
                        try
                        {
                            return valueConverter.toNeoValue( resultMethod.invoke(aggregator) );
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
                                        "Failed to invoke function `%s`: %s", signature.name(),
                                        "Caused by: " + throwable );
                            }
                        }

                    }

                };

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
                            "Failed to invoke function `%s`: %s", signature.name(),
                            "Caused by: " + throwable );
                }
            }
        }
    }
}
