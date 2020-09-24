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
package org.neo4j.procedure.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.values.AnyValue;

import static java.lang.String.format;

public class ProcedureRegistry
{

    private final ProcedureHolder<CallableProcedure> procedures = new ProcedureHolder<>();
    private final ProcedureHolder<CallableUserFunction> functions = new ProcedureHolder<>();
    private final ProcedureHolder<CallableUserAggregationFunction> aggregationFunctions = new ProcedureHolder<>();
    private final Set<Integer> builtInFunctionIds = new HashSet<>();
    private final Set<Integer> builtInAggregatingFunctionIds = new HashSet<>();

    /**
     * Register a new procedure.
     *
     * @param proc the procedure.
     */
    public void register( CallableProcedure proc, boolean overrideCurrentImplementation ) throws ProcedureException
    {
        ProcedureSignature signature = proc.signature();
        QualifiedName name = signature.name();

        String descriptiveName = signature.toString();
        validateSignature( descriptiveName, signature.inputSignature(), "input" );
        validateSignature( descriptiveName, signature.outputSignature(), "output" );

        if ( !signature.isVoid() && signature.outputSignature().isEmpty() )
        {
            throw new ProcedureException(
                Status.Procedure.ProcedureRegistrationFailed,
                "Procedures with zero output fields must be declared as VOID"
            );
        }

        CallableProcedure oldImplementation = procedures.get( name );
        if ( oldImplementation == null )
        {
            procedures.put( name, proc, signature.caseInsensitive() );
        }
        else
        {
            if ( overrideCurrentImplementation )
            {
                procedures.put( name, proc, signature.caseInsensitive() );
            }
            else
            {
                throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                        "Unable to register procedure, because the name `%s` is already in use.", name );
            }
        }
    }

    /**
     * Register a new function.
     *
     * @param function the function.
     */
    public void register( CallableUserFunction function, boolean overrideCurrentImplementation, boolean builtIn ) throws ProcedureException
    {
        UserFunctionSignature signature = function.signature();
        QualifiedName name = signature.name();

        if ( aggregationFunctions.get( name ) != null )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Unable to register function, because the name `%s` is already in use as an aggregation function.", name );
        }
        CallableUserFunction oldImplementation = functions.get( name );
        if ( oldImplementation == null )
        {
            functions.put( name, function, signature.caseInsensitive() );
        }
        else
        {
            if ( overrideCurrentImplementation )
            {
                functions.put( name, function, signature.caseInsensitive() );
            }
            else
            {
                throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                        "Unable to register function, because the name `%s` is already in use.", name );
            }
        }
        if ( builtIn )
        {
            builtInFunctionIds.add( functions.idOf( name ) );
        }
    }

    /**
     * Register a new function.
     *
     * @param function the function.
     */
    public void register( CallableUserAggregationFunction function, boolean overrideCurrentImplementation, boolean builtIn ) throws ProcedureException
    {
        UserFunctionSignature signature = function.signature();
        QualifiedName name = signature.name();

        if ( functions.get( name ) != null )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Unable to register aggregation function, because the name `%s` is already in use as a function.", name );
        }
        CallableUserAggregationFunction oldImplementation = aggregationFunctions.get( name );
        if ( oldImplementation == null )
        {
            aggregationFunctions.put( name, function, signature.caseInsensitive() );
        }
        else
        {
            if ( overrideCurrentImplementation )
            {
                aggregationFunctions.put( name, function, signature.caseInsensitive() );
            }
            else
            {
                throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                        "Unable to register aggregation function, because the name `%s` is already in use.", name );
            }
        }
        if ( builtIn )
        {
            builtInAggregatingFunctionIds.add( aggregationFunctions.idOf( name ) );
        }
    }

    private void validateSignature( String descriptiveName, List<FieldSignature> fields, String fieldType )
            throws ProcedureException
    {
        Set<String> names = new HashSet<>();
        for ( FieldSignature field : fields )
        {
            if ( !names.add( field.name() ) )
            {
                throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                        "Procedure `%s` cannot be registered, because it contains a duplicated " + fieldType + " field, '%s'. " +
                        "You need to rename or remove one of the duplicate fields.", descriptiveName, field.name() );
            }
        }
    }

    public ProcedureHandle procedure( QualifiedName name ) throws ProcedureException
    {
        CallableProcedure proc = procedures.get( name );
        if ( proc == null )
        {
            throw noSuchProcedure( name );
        }
        return new ProcedureHandle( proc.signature(), procedures.idOf( name ) );
    }

    public UserFunctionHandle function( QualifiedName name )
    {
        CallableUserFunction func = functions.get( name );
        if ( func == null )
        {
            return null;
        }
        return new UserFunctionHandle( func.signature(), functions.idOf( name), func.threadSafe() );
    }

    public UserFunctionHandle aggregationFunction( QualifiedName name )
    {
        CallableUserAggregationFunction func = aggregationFunctions.get( name );
        if ( func == null )
        {
            return null;
        }
        return new UserFunctionHandle( func.signature(), aggregationFunctions.idOf( name), false );
    }

    public RawIterator<AnyValue[],ProcedureException> callProcedure( Context ctx, int id, AnyValue[] input, ResourceTracker resourceTracker )
            throws ProcedureException
    {
        CallableProcedure proc;
        try
        {
            proc = procedures.get( id );
            if ( proc.signature().admin() && !ctx.securityContext().allowExecuteAdminProcedure( id ) )
            {
                throw new AuthorizationViolationException( format( "Executing admin procedure is not allowed for %s.", ctx.securityContext().description() ) );
            }
        }
        catch ( IndexOutOfBoundsException e )
        {
            throw noSuchProcedure( id );
        }
        return proc.apply( ctx, input, resourceTracker );
    }

    public AnyValue callFunction( Context ctx, int functionId, AnyValue[] input )
            throws ProcedureException
    {
        CallableUserFunction func;
        try
        {
            func = functions.get( functionId );
        }
        catch ( IndexOutOfBoundsException e )
        {
            throw noSuchFunction( functionId );
        }
        return func.apply( ctx, input );
    }

    public UserAggregator createAggregationFunction( Context ctx, int id ) throws ProcedureException
    {
        try
        {
            CallableUserAggregationFunction func = aggregationFunctions.get( id );
            return func.create(ctx);
        }
        catch ( IndexOutOfBoundsException e )
        {
            throw noSuchFunction( id );
        }
    }

    private ProcedureException noSuchProcedure( QualifiedName name )
    {
        return new ProcedureException( Status.Procedure.ProcedureNotFound,
                "There is no procedure with the name `%s` registered for this database instance. " +
                "Please ensure you've spelled the procedure name correctly and that the " +
                "procedure is properly deployed.", name );
    }

    private ProcedureException noSuchProcedure( int id )
    {
        return new ProcedureException( Status.Procedure.ProcedureNotFound,
                "There is no procedure with the internal id `%d` registered for this database instance.", id );
    }

    private ProcedureException noSuchFunction( int id )
    {
        return new ProcedureException( Status.Procedure.ProcedureNotFound,
                "There is no function with the internal id `%d` registered for this database instance.", id );
    }

    public Set<ProcedureSignature> getAllProcedures()
    {
        return procedures.all().stream().map( CallableProcedure::signature ).collect( Collectors.toSet());
    }

    int[] getIdsOfProceduresMatching( Predicate<CallableProcedure> predicate )
    {
        return procedures.all().stream()
                         .filter( predicate )
                         .mapToInt( p -> procedures.idOf( p.signature().name() ) )
                         .toArray();
    }

    public Stream<UserFunctionSignature> getAllNonAggregatingFunctions()
    {
        return functions.all().stream().map( CallableUserFunction::signature );
    }

    int[] getIdsOfFunctionsMatching( Predicate<CallableUserFunction> predicate )
    {
        return functions.all().stream()
                        .filter( predicate )
                        .mapToInt( f -> functions.idOf( f.signature().name() ) )
                        .toArray();
    }

    boolean isBuiltInFunction( int id )
    {
        return builtInFunctionIds.contains( id );
    }

    public Stream<UserFunctionSignature> getAllAggregatingFunctions()
    {
        return aggregationFunctions.all().stream().map( CallableUserAggregationFunction::signature );
    }

    int[] getIdsOfAggregatingFunctionsMatching( Predicate<CallableUserAggregationFunction> predicate )
    {
        return aggregationFunctions.all().stream()
                                   .filter( predicate )
                                   .mapToInt( f -> aggregationFunctions.idOf( f.signature().name() ) )
                                   .toArray();
    }

    boolean isBuiltInAggregatingFunction( int id )
    {
        return builtInAggregatingFunctionIds.contains( id );
    }
}
