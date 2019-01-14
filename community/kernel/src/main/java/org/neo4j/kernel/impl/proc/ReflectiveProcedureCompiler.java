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

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.exceptions.ComponentInjectionException;
import org.neo4j.kernel.api.exceptions.ResourceCloseFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.FailedLoadAggregatedFunction;
import org.neo4j.kernel.api.proc.FailedLoadFunction;
import org.neo4j.kernel.api.proc.FailedLoadProcedure;
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
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;

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
    private final NamingRestrictions restrictions;

    ReflectiveProcedureCompiler( TypeMappers typeMappers, ComponentRegistry safeComponents,
            ComponentRegistry allComponents, Log log, ProcedureConfig config )
    {
        this(
                new MethodSignatureCompiler( typeMappers ),
                new OutputMappers( typeMappers ),
                new FieldInjections( safeComponents ),
                new FieldInjections( allComponents ),
                log,
                typeMappers,
                config,
                ReflectiveProcedureCompiler::rejectEmptyNamespace );
    }

    private ReflectiveProcedureCompiler(
            MethodSignatureCompiler inputSignatureCompiler,
            OutputMappers outputMappers,
            FieldInjections safeFieldInjections,
            FieldInjections allFieldInjections,
            Log log,
            TypeMappers typeMappers,
            ProcedureConfig config,
            NamingRestrictions restrictions )
    {
        this.inputSignatureDeterminer = inputSignatureCompiler;
        this.outputMappers = outputMappers;
        this.safeFieldInjections = safeFieldInjections;
        this.allFieldInjections = allFieldInjections;
        this.log = log;
        this.typeMappers = typeMappers;
        this.config = config;
        this.restrictions = restrictions;
    }

    List<CallableUserFunction> compileFunction( Class<?> fcnDefinition ) throws KernelException
    {
        try
        {
            List<Method> functionMethods = Arrays.stream( fcnDefinition.getDeclaredMethods() )
                    .filter( m -> m.isAnnotationPresent( UserFunction.class ) )
                    .collect( Collectors.toList() );

            if ( functionMethods.isEmpty() )
            {
                return emptyList();
            }

            MethodHandle constructor = constructor( fcnDefinition );

            ArrayList<CallableUserFunction> out = new ArrayList<>( functionMethods.size() );
            for ( Method method : functionMethods )
            {
                String valueName = method.getAnnotation( UserFunction.class ).value();
                String definedName = method.getAnnotation( UserFunction.class ).name();
                QualifiedName funcName = extractName( fcnDefinition, method, valueName, definedName );
                if ( config.isWhitelisted( funcName.toString() ) )
                {
                    out.add( compileFunction( fcnDefinition, constructor, method, funcName ) );
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

    List<CallableProcedure> compileProcedure( Class<?> procDefinition, String warning, boolean fullAccess )
            throws KernelException
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
            String warning, boolean fullAccess, QualifiedName procName  )
            throws ProcedureException
    {
        List<FieldSignature> inputSignature = inputSignatureDeterminer.signatureFor( method );
        OutputMapper outputMapper = outputMappers.mapper( method );

        String description = description( method );
        Procedure procedure = method.getAnnotation( Procedure.class );
        Mode mode = procedure.mode();
        if ( method.isAnnotationPresent( PerformsWrites.class ) )
        {
            if ( procedure.mode() != org.neo4j.procedure.Mode.DEFAULT )
            {
                throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                        "Conflicting procedure annotation, cannot use PerformsWrites and mode" );
            }
            else
            {
                mode = Mode.WRITE;
            }
        }

        String deprecated = deprecated( method, procedure::deprecatedBy,
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
                                null, new String[0], description, warning, procedure.eager(), false );
                return new FailedLoadProcedure( signature );
            }
        }

        ProcedureSignature signature =
                new ProcedureSignature( procName, inputSignature, outputMapper.signature(), mode, deprecated,
                        config.rolesFor( procName.toString() ), description, warning, procedure.eager(), false );
        return new ReflectiveProcedure( signature, constructor, method, outputMapper, setters );
    }

    private String describeAndLogLoadFailure( QualifiedName name )
    {
        String nameStr = name.toString();
        String description =
                nameStr + " is unavailable because it is sandboxed and has dependencies outside of the sandbox. " +
                "Sandboxing is controlled by the " + procedure_unrestricted.name() + " setting. " +
                "Only unrestrict procedures you can trust with access to database internals.";
        log.warn( description );
        return description;
    }

    private CallableUserFunction compileFunction( Class<?> procDefinition, MethodHandle constructor, Method method,
            QualifiedName procName )
            throws ProcedureException, IllegalAccessException
    {
        restrictions.verify( procName );

        List<FieldSignature> inputSignature = inputSignatureDeterminer.signatureFor( method );
        Class<?> returnType = method.getReturnType();
        TypeMappers.TypeChecker typeChecker = typeMappers.checkerFor( returnType );
        String description = description( method );
        UserFunction function = method.getAnnotation( UserFunction.class );
        String deprecated = deprecated( method, function::deprecatedBy,
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
                        new UserFunctionSignature( procName, inputSignature, typeChecker.type(), deprecated,
                                config.rolesFor( procName.toString() ), description, false );
                return new FailedLoadFunction( signature );
            }
        }

        UserFunctionSignature signature =
                new UserFunctionSignature( procName, inputSignature, typeChecker.type(), deprecated,
                        config.rolesFor( procName.toString() ), description, false );

        return new ReflectiveUserFunction( signature, constructor, method, typeChecker, typeMappers, setters );
    }

    private CallableUserAggregationFunction compileAggregationFunction( Class<?> definition, MethodHandle constructor,
            Method method, QualifiedName funcName ) throws ProcedureException, IllegalAccessException
    {
        restrictions.verify( funcName );

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
        TypeMappers.TypeChecker valueConverter = typeMappers.checkerFor( returnType );
        MethodHandle creator = lookup.unreflect( method );
        MethodHandle resultMethod = lookup.unreflect( result );

        String description = description( method );
        UserAggregationFunction function = method.getAnnotation( UserAggregationFunction.class );

        String deprecated = deprecated( method, function::deprecatedBy,
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
                                config.rolesFor( funcName.toString() ), description, false );

                return new FailedLoadAggregatedFunction( signature );
            }
        }

        UserFunctionSignature signature =
                new UserFunctionSignature( funcName, inputSignature, valueConverter.type(), deprecated,
                        config.rolesFor( funcName.toString() ), description, false );

        return new ReflectiveUserAggregationFunction( signature, constructor, creator, update, resultMethod,
                valueConverter, setters );
    }

    private String deprecated( Method method, Supplier<String> supplier, String warning )
    {
        String deprecatedBy = supplier.get();
        String deprecated = null;
        if ( method.isAnnotationPresent( Deprecated.class ) )
        {
            deprecated = deprecatedBy ;
        }
        else if ( !deprecatedBy.isEmpty() )
        {
            log.warn( warning );
            deprecated = deprecatedBy;
        }

        return deprecated;
    }

    private String description( Method method )
    {
        if ( method.isAnnotationPresent( Description.class ) )
        {
           return method.getAnnotation( Description.class ).value();
        }
        else
        {
            return null;
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

    public ReflectiveProcedureCompiler withoutNamingRestrictions()
    {
        return new ReflectiveProcedureCompiler(
                inputSignatureDeterminer,
                outputMappers,
                safeFieldInjections,
                allFieldInjections,
                log,
                typeMappers,
                config,
                name ->
                {
                    // all ok
                } );
    }

    private abstract static class ReflectiveBase
    {

        final List<FieldInjections.FieldSetter> fieldSetters;
        private final ValueMapper<Object> mapper;

        ReflectiveBase( ValueMapper<Object> mapper, List<FieldInjections.FieldSetter> fieldSetters )
        {
            this.mapper = mapper;
            this.fieldSetters = fieldSetters;
        }

        protected void inject( Context ctx, Object object ) throws ProcedureException
        {
            for ( FieldInjections.FieldSetter setter : fieldSetters )
            {
                setter.apply( ctx, object );
            }
        }

        protected Object[] mapToObjects( String type, QualifiedName name, List<FieldSignature> inputSignature,
                AnyValue[] input ) throws ProcedureException
        {
            // Verify that the number of passed arguments matches the number expected in the mthod signature
            if ( inputSignature.size() != input.length )
            {
                throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                        "%s `%s` takes %d arguments but %d was provided.", type, name,
                        inputSignature.size(), input.length );
            }

            Object[] args = new Object[input.length];
            for ( int i = 0; i < input.length; i++ )
            {
                args[i] = inputSignature.get( i ).map( input[i], mapper );
            }
            return args;
        }
    }

    private static class ReflectiveProcedure extends ReflectiveBase implements CallableProcedure
    {
        private final ProcedureSignature signature;
        private final OutputMapper outputMapper;
        private final MethodHandle constructor;
        private final Method procedureMethod;
        private final int[] indexesToMap;

        ReflectiveProcedure( ProcedureSignature signature, MethodHandle constructor,
                Method procedureMethod, OutputMapper outputMapper,
                List<FieldInjections.FieldSetter> fieldSetters )
        {
            super( null, fieldSetters );
            this.constructor = constructor;
            this.procedureMethod = procedureMethod;
            this.signature = signature;
            this.outputMapper = outputMapper;
            this.indexesToMap = computeIndexesToMap( signature.inputSignature() );
        }

        @Override
        public ProcedureSignature signature()
        {
            return signature;
        }

        @Override
        public RawIterator<Object[],ProcedureException> apply( Context ctx, Object[] input,
                ResourceTracker resourceTracker ) throws ProcedureException
        {
            // For now, create a new instance of the class for each invocation. In the future, we'd like to keep
            // instances local to
            // at least the executing session, but we don't yet have good interfaces to the kernel to model that with.
            try
            {
                List<FieldSignature> inputSignature = signature.inputSignature();
                if ( inputSignature.size() != input.length )
                {
                    throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                            "Procedure `%s` takes %d arguments but %d was provided.",
                            signature.name(),
                            inputSignature.size(), input.length );
                }
                // Some input fields are not supported by Cypher and need to be mapped
                for ( int indexToMap : indexesToMap )
                {
                    input[indexToMap] = inputSignature.get( indexToMap ).map( input[indexToMap] );
                }

                Object cls = constructor.invoke();
                //API injection
                inject( ctx, cls );

                // Call the method
                Object rs = procedureMethod.invoke( cls, input );

                // This also handles VOID
                if ( rs == null )
                {
                    return asRawIterator( emptyIterator() );
                }
                else
                {
                    return new MappingIterator( ((Stream<?>) rs).iterator(), ((Stream<?>) rs)::close, resourceTracker );
                }
            }
            catch ( Throwable throwable )
            {
                throw newProcedureException( throwable );
            }
        }

        private class MappingIterator implements RawIterator<Object[],ProcedureException>, Resource
        {
            private final Iterator<?> out;
            private Resource closeableResource;
            private ResourceTracker resourceTracker;

            MappingIterator( Iterator<?> out, Resource closeableResource, ResourceTracker resourceTracker )
            {
                this.out = out;
                this.closeableResource = closeableResource;
                this.resourceTracker = resourceTracker;
                resourceTracker.registerCloseableResource( closeableResource );
            }

            @Override
            public boolean hasNext() throws ProcedureException
            {
                try
                {
                    boolean hasNext = out.hasNext();
                    if ( !hasNext )
                    {
                        close();
                    }
                    return hasNext;
                }
                catch ( Throwable throwable )
                {
                    throw closeAndCreateProcedureException( throwable );
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
                catch ( Throwable throwable )
                {
                    throw closeAndCreateProcedureException( throwable );
                }
            }

            @Override
            public void close()
            {
                if ( closeableResource != null )
                {
                    // Make sure we reset closeableResource before doing anything which may throw an exception that may
                    // result in a recursive call to this close-method
                    Resource resourceToClose = closeableResource;
                    closeableResource = null;

                    IOUtils.closeAll( ResourceCloseFailureException.class,
                            () -> resourceTracker.unregisterCloseableResource( resourceToClose ),
                            resourceToClose::close );
                }
            }

            private ProcedureException closeAndCreateProcedureException( Throwable t )
            {
                ProcedureException procedureException = newProcedureException( t );

                try
                {
                    close();
                }
                catch ( Exception exceptionDuringClose )
                {
                    try
                    {
                        procedureException.addSuppressed( exceptionDuringClose );
                    }
                    catch ( Throwable ignore )
                    {
                    }
                }
                return procedureException;
            }
        }

        private ProcedureException newProcedureException( Throwable throwable )
        {
            // Unwrap the wrapped exception we get from invocation by reflection
            if ( throwable instanceof InvocationTargetException )
            {
                throwable = throwable.getCause();
            }

            if ( throwable instanceof Status.HasStatus )
            {
                return new ProcedureException( ((Status.HasStatus) throwable).status(), throwable, throwable.getMessage() );
            }
            else
            {
                Throwable cause = ExceptionUtils.getRootCause( throwable );
                return new ProcedureException( Status.Procedure.ProcedureCallFailed, throwable,
                        "Failed to invoke procedure `%s`: %s", signature.name(),
                        "Caused by: " + (cause != null ? cause : throwable) );
            }
        }
    }

    private static class ReflectiveUserFunction extends ReflectiveBase implements CallableUserFunction
    {
        private final TypeMappers.TypeChecker typeChecker;
        private final UserFunctionSignature signature;
        private final MethodHandle constructor;
        private final Method udfMethod;
        private final int[] indexesToMap;

        ReflectiveUserFunction( UserFunctionSignature signature, MethodHandle constructor,
                Method udfMethod, TypeMappers.TypeChecker typeChecker,
                ValueMapper<Object> mapper, List<FieldInjections.FieldSetter> fieldSetters )
        {
            super( mapper, fieldSetters );
            this.constructor = constructor;
            this.udfMethod = udfMethod;
            this.signature = signature;
            this.typeChecker = typeChecker;
            indexesToMap = computeIndexesToMap( signature.inputSignature() );
        }

        @Override
        public UserFunctionSignature signature()
        {
            return signature;
        }

        @Override
        public AnyValue apply( Context ctx, AnyValue[] input ) throws ProcedureException
        {
            // For now, create a new instance of the class for each invocation. In the future, we'd like to keep
            // instances local to
            // at least the executing session, but we don't yet have good interfaces to the kernel to model that with.
            try
            {
                Object cls = constructor.invoke();
                //API injection
                inject( ctx, cls );

                // Call the method
                Object rs = udfMethod.invoke( cls, mapToObjects( "Function", signature.name(), signature.inputSignature(), input  ) );

                return typeChecker.toValue( rs );
            }
            catch ( Throwable throwable )
            {
                if ( throwable instanceof Status.HasStatus )
                {
                    throw new ProcedureException( ((Status.HasStatus) throwable).status(), throwable,
                            throwable.getMessage(), throwable );
                }
                else
                {
                    Throwable cause = ExceptionUtils.getRootCause( throwable );
                    throw new ProcedureException( Status.Procedure.ProcedureCallFailed, throwable,
                            "Failed to invoke function `%s`: %s", signature.name(),
                            "Caused by: " + (cause != null ? cause : throwable) );
                }
            }
        }
    }

    private static class ReflectiveUserAggregationFunction extends ReflectiveBase implements
            CallableUserAggregationFunction
    {

        private final TypeMappers.TypeChecker typeChecker;
        private final UserFunctionSignature signature;
        private final MethodHandle constructor;
        private final MethodHandle creator;
        private final Method updateMethod;
        private final MethodHandle resultMethod;
        private final int[] indexesToMap;

        ReflectiveUserAggregationFunction( UserFunctionSignature signature, MethodHandle constructor,
                MethodHandle creator, Method updateMethod, MethodHandle resultMethod,
                TypeMappers.TypeChecker typeChecker,
                List<FieldInjections.FieldSetter> fieldSetters )
        {
            super( null, fieldSetters );
            this.constructor = constructor;
            this.creator = creator;
            this.updateMethod = updateMethod;
            this.resultMethod = resultMethod;
            this.signature = signature;
            this.typeChecker = typeChecker;
            this.indexesToMap = computeIndexesToMap( signature.inputSignature() );
        }

        @Override
        public UserFunctionSignature signature()
        {
            return signature;
        }

        @Override
        public UserAggregator create( Context ctx ) throws ProcedureException
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
                List<FieldSignature> inputSignature = signature.inputSignature();
                int expectedNumberOfInputs = inputSignature.size();

                return new UserAggregator()
                {
                    @Override
                    public void update( Object[] input ) throws ProcedureException
                    {
                        try
                        {
                            if ( expectedNumberOfInputs != input.length )
                            {
                                throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                                        "Function `%s` takes %d arguments but %d was provided.",
                                        signature.name(),
                                        expectedNumberOfInputs, input.length );
                            }
                            // Some input fields are not supported by Cypher and need to be mapped
                            for ( int indexToMap : indexesToMap )
                            {
                                input[indexToMap] = inputSignature.get( indexToMap ).map( input[indexToMap] );
                            }

                            // Call the method
                            updateMethod.invoke( aggregator, input );
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
                                Throwable cause = ExceptionUtils.getRootCause( throwable );
                                throw new ProcedureException( Status.Procedure.ProcedureCallFailed, throwable,
                                        "Failed to invoke function `%s`: %s", signature.name(),
                                        "Caused by: " + (cause != null ? cause : throwable) );
                            }
                        }
                    }

                    @Override
                    public Object result() throws ProcedureException
                    {
                        try
                        {
                            return typeChecker.typeCheck( resultMethod.invoke(aggregator) );
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
                                Throwable cause = ExceptionUtils.getRootCause( throwable );
                                throw new ProcedureException( Status.Procedure.ProcedureCallFailed, throwable,
                                        "Failed to invoke function `%s`: %s", signature.name(),
                                        "Caused by: " + (cause != null ? cause : throwable) );
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
                    Throwable cause = ExceptionUtils.getRootCause( throwable );
                    throw new ProcedureException( Status.Procedure.ProcedureCallFailed, throwable,
                            "Failed to invoke function `%s`: %s", signature.name(),
                            "Caused by: " + (cause != null ? cause : throwable ) );
                }
            }
        }
    }

    private static void rejectEmptyNamespace( QualifiedName name ) throws ProcedureException
    {
        if ( name.namespace() == null || name.namespace().length == 0 )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "It is not allowed to define functions in the root namespace please use a namespace, " +
                    "e.g. `@UserFunction(\"org.example.com.%s\")", name.name() );
        }
    }

    private static int[] computeIndexesToMap( List<FieldSignature> inputSignature )
    {
        ArrayList<Integer> integers = new ArrayList<>();
        for ( int i = 0; i < inputSignature.size(); i++ )
        {
            if ( inputSignature.get( i ).needsMapping() )
            {
                integers.add( i );
            }
        }
        return integers.stream().mapToInt( i -> i ).toArray();
    }
}
