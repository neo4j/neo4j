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
package org.neo4j.procedure.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.exceptions.ComponentInjectionException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.FailedLoadAggregatedFunction;
import org.neo4j.kernel.api.procedure.FailedLoadFunction;
import org.neo4j.kernel.api.procedure.FailedLoadProcedure;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.procedure.UserFunction;

import static java.lang.reflect.Modifier.isPublic;
import static java.util.Collections.emptyList;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;

/**
 * Handles converting a class into one or more callable {@link CallableProcedure}.
 */
class ProcedureCompiler
{
    private final ProcedureOutputSignatureCompiler outputSignatureCompiler;
    private final MethodSignatureCompiler inputSignatureDeterminer;
    private final FieldInjections safeFieldInjections;
    private final FieldInjections allFieldInjections;
    private final Log log;
    private final TypeCheckers typeCheckers;
    private final ProcedureConfig config;
    private final NamingRestrictions restrictions;

    ProcedureCompiler( TypeCheckers typeCheckers, ComponentRegistry safeComponents,
            ComponentRegistry allComponents, Log log, ProcedureConfig config )
    {
        this(
                new MethodSignatureCompiler( typeCheckers ),
                new ProcedureOutputSignatureCompiler( typeCheckers ),
                new FieldInjections( safeComponents ),
                new FieldInjections( allComponents ),
                log,
                typeCheckers,
                config,
                ProcedureCompiler::rejectEmptyNamespace );
    }

    private ProcedureCompiler(
            MethodSignatureCompiler inputSignatureCompiler,
            ProcedureOutputSignatureCompiler outputSignatureCompiler,
            FieldInjections safeFieldInjections,
            FieldInjections allFieldInjections,
            Log log,
            TypeCheckers typeCheckers,
            ProcedureConfig config,
            NamingRestrictions restrictions )
    {
        this.inputSignatureDeterminer = inputSignatureCompiler;
        this.outputSignatureCompiler = outputSignatureCompiler;
        this.safeFieldInjections = safeFieldInjections;
        this.allFieldInjections = allFieldInjections;
        this.log = log;
        this.typeCheckers = typeCheckers;
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

            //used for proper error handling
            assertValidConstructor( fcnDefinition );

            ArrayList<CallableUserFunction> out = new ArrayList<>( functionMethods.size() );
            for ( Method method : functionMethods )
            {
                String valueName = method.getAnnotation( UserFunction.class ).value();
                String definedName = method.getAnnotation( UserFunction.class ).name();
                QualifiedName funcName = extractName( fcnDefinition, method, valueName, definedName );
                if ( config.isWhitelisted( funcName.toString() ) )
                {
                    out.add( compileFunction( fcnDefinition, method, funcName ) );
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

            assertValidConstructor( fcnDefinition );

            ArrayList<CallableUserAggregationFunction> out = new ArrayList<>( methods.size() );
            for ( Method method : methods )
            {
                String valueName = method.getAnnotation( UserAggregationFunction.class ).value();
                String definedName = method.getAnnotation( UserAggregationFunction.class ).name();
                QualifiedName funcName = extractName( fcnDefinition, method, valueName, definedName );

                if ( config.isWhitelisted( funcName.toString() ) )
                {
                    out.add( compileAggregationFunction( fcnDefinition, method, funcName ) );
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

            assertValidConstructor( procDefinition );
            ArrayList<CallableProcedure> out = new ArrayList<>( procedureMethods.size() );
            for ( Method method : procedureMethods )
            {
                String valueName = method.getAnnotation( Procedure.class ).value();
                String definedName = method.getAnnotation( Procedure.class ).name();
                QualifiedName procName = extractName( procDefinition, method, valueName, definedName );

                if ( fullAccess || config.isWhitelisted( procName.toString() ) )
                {
                    out.add( compileProcedure( procDefinition, method, warning, fullAccess, procName ) );
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

    private CallableProcedure compileProcedure( Class<?> procDefinition, Method method,
            String warning, boolean fullAccess, QualifiedName procName  )
            throws ProcedureException
    {
        List<FieldSignature> inputSignature = inputSignatureDeterminer.signatureFor( method );
        List<FieldSignature> outputSignature = outputSignatureCompiler.fieldSignatures( method );

        String description = description( method );
        Procedure procedure = method.getAnnotation( Procedure.class );
        Mode mode = procedure.mode();
        boolean admin = method.isAnnotationPresent( Admin.class );
        boolean systemProcedure = method.isAnnotationPresent( SystemProcedure.class );
        String deprecated = deprecated( method, procedure::deprecatedBy,
                "Use of @Procedure(deprecatedBy) without @Deprecated in " + procName );

        List<FieldSetter> setters = allFieldInjections.setters( procDefinition );
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
                        new ProcedureSignature( procName, inputSignature, outputSignature, Mode.DEFAULT,
                                admin, null, new String[0], description, warning, procedure.eager(), false, systemProcedure );
                return new FailedLoadProcedure( signature );
            }
        }

        ProcedureSignature signature =
                new ProcedureSignature( procName, inputSignature, outputSignature, mode, admin, deprecated,
                        config.rolesFor( procName.toString() ), description, warning, procedure.eager(), false, systemProcedure );

        return ProcedureCompilation.compileProcedure( signature, setters, method );
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

    private CallableUserFunction compileFunction( Class<?> procDefinition, Method method, QualifiedName procName )
            throws ProcedureException
    {
        restrictions.verify( procName );

        List<FieldSignature> inputSignature = inputSignatureDeterminer.signatureFor( method );
        Class<?> returnType = method.getReturnType();
        TypeCheckers.TypeChecker typeChecker = typeCheckers.checkerFor( returnType );
        String description = description( method );
        UserFunction function = method.getAnnotation( UserFunction.class );
        String deprecated = deprecated( method, function::deprecatedBy,
                "Use of @UserFunction(deprecatedBy) without @Deprecated in " + procName );

        List<FieldSetter> setters = allFieldInjections.setters( procDefinition );
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

        return ProcedureCompilation.compileFunction( signature, setters, method );
    }

    private CallableUserAggregationFunction compileAggregationFunction( Class<?> definition, Method create,
            QualifiedName funcName ) throws ProcedureException
    {
        restrictions.verify( funcName );

        //find update and result method
        Method update = null;
        Method result = null;
        Class<?> aggregator = create.getReturnType();
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
        if ( !isPublic( create.getModifiers() ) )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Aggregation method '%s' in %s must be public.", create.getName(), definition.getSimpleName() );
        }
        if ( !isPublic( aggregator.getModifiers() ) )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Aggregation class '%s' must be public.", aggregator.getSimpleName() );
        }
        if ( !isPublic( update.getModifiers() ) )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Aggregation update method '%s' in %s must be public.", update.getName(),
                    aggregator.getSimpleName() );
        }
        if ( !isPublic( result.getModifiers() ) )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Aggregation result method '%s' in %s must be public.", result.getName(),
                    aggregator.getSimpleName() );
        }

        List<FieldSignature> inputSignature = inputSignatureDeterminer.signatureFor( update );
        Class<?> returnType = result.getReturnType();
        TypeCheckers.TypeChecker valueConverter = typeCheckers.checkerFor( returnType );
        String description = description( create );
        UserAggregationFunction function = create.getAnnotation( UserAggregationFunction.class );

        String deprecated = deprecated( create, function::deprecatedBy,
                "Use of @UserAggregationFunction(deprecatedBy) without @Deprecated in " + funcName );

        List<FieldSetter> setters = allFieldInjections.setters( definition );
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

        return ProcedureCompilation.compileAggregation( signature, setters, create, update, result );
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

    private void assertValidConstructor( Class<?> procDefinition ) throws ProcedureException
    {
        boolean hasValidConstructor = false;
        for ( Constructor<?> constructor : procDefinition.getConstructors() )
        {
            if ( isPublic( constructor.getModifiers() ) && constructor.getParameterCount() == 0 )
            {
                hasValidConstructor = true;
                break;
            }
        }
        if ( !hasValidConstructor )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
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

    ProcedureCompiler withoutNamingRestrictions()
    {
        return new ProcedureCompiler(
                inputSignatureDeterminer,
                outputSignatureCompiler,
                safeFieldInjections,
                allFieldInjections,
                log,
                typeCheckers,
                config,
                name ->
                {
                    // all ok
                } );
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

}
