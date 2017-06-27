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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.FieldSignature;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.kernel.api.proc.UserFunctionSignature;

public class ProcedureRegistry
{
    private final Map<QualifiedName,CallableProcedure> procedures = new HashMap<>();
    private final Map<QualifiedName,CallableUserFunction> functions = new HashMap<>();
    private final Map<QualifiedName,CallableUserAggregationFunction> aggregationFunctions = new HashMap<>();

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

        if ( ! signature.isVoid() && signature.outputSignature().isEmpty() )
        {
            throw new ProcedureException(
                Status.Procedure.ProcedureRegistrationFailed,
                "Procedures with zero output fields must be declared as VOID"
            );
        }

        CallableProcedure oldImplementation = procedures.get( name );
        if ( oldImplementation == null )
        {
            procedures.put( name, proc );
        }
        else
        {
            if ( overrideCurrentImplementation )
            {
                procedures.put( name, proc );
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
    public void register( CallableUserFunction function, boolean overrideCurrentImplementation ) throws ProcedureException
    {
        UserFunctionSignature signature = function.signature();
        QualifiedName name = signature.name();

        CallableUserFunction oldImplementation = functions.get( name );
        if ( oldImplementation == null )
        {
            functions.put( name, function );
        }
        else
        {
            if ( overrideCurrentImplementation )
            {
                functions.put( name, function );
            }
            else
            {
                throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                        "Unable to register function, because the name `%s` is already in use.", name );
            }
        }
    }

    /**
     * Register a new function.
     *
     * @param function the function.
     */
    public void register( CallableUserAggregationFunction function, boolean overrideCurrentImplementation ) throws ProcedureException
    {
        UserFunctionSignature signature = function.signature();
        QualifiedName name = signature.name();

        CallableUserFunction oldImplementation = functions.get( name );
        if ( oldImplementation == null )
        {
            aggregationFunctions.put( name, function );
        }
        else
        {
            if ( overrideCurrentImplementation )
            {
                aggregationFunctions.put( name, function );
            }
            else
            {
                throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                        "Unable to register aggregation function, because the name `%s` is already in use.", name );
            }
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

    public ProcedureSignature procedure( QualifiedName name ) throws ProcedureException
    {
        CallableProcedure proc = procedures.get( name );
        if ( proc == null )
        {
            throw noSuchProcedure( name );
        }
        return proc.signature();
    }

    public Optional<UserFunctionSignature> function( QualifiedName name )
    {
        CallableUserFunction func = functions.get( name );
        if ( func == null )
        {
            return Optional.empty();
        }
        return Optional.of( func.signature() );
    }

    public Optional<UserFunctionSignature> aggregationFunction( QualifiedName name )
    {
        CallableUserAggregationFunction func = aggregationFunctions.get( name );
        if ( func == null )
        {
            return Optional.empty();
        }
        return Optional.of( func.signature() );
    }

    public RawIterator<Object[],ProcedureException> callProcedure( Context ctx, QualifiedName name, Object[] input )
            throws ProcedureException
    {
        CallableProcedure proc = procedures.get( name );
        if ( proc == null )
        {
            throw noSuchProcedure( name );
        }
        return proc.apply( ctx, input );
    }

    public Object callFunction( Context ctx, QualifiedName name, Object[] input )
            throws ProcedureException
    {
        CallableUserFunction func = functions.get( name );
        if ( func == null )
        {
            throw noSuchFunction( name );
        }
        return func.apply( ctx, input );
    }

    public CallableUserAggregationFunction.Aggregator createAggregationFunction( Context ctx, QualifiedName name )
            throws ProcedureException
    {
        CallableUserAggregationFunction func = aggregationFunctions.get( name );
        if ( func == null )
        {
            throw noSuchFunction( name );
        }
        return func.create(ctx);
    }

    private ProcedureException noSuchProcedure( QualifiedName name )
    {
        return new ProcedureException( Status.Procedure.ProcedureNotFound,
                "There is no procedure with the name `%s` registered for this database instance. " +
                "Please ensure you've spelled the procedure name correctly and that the " +
                "procedure is properly deployed.", name );
    }

    private ProcedureException noSuchFunction( QualifiedName name )
    {
        return new ProcedureException( Status.Procedure.ProcedureNotFound,
                "There is no function with the name `%s` registered for this database instance. " +
                "Please ensure you've spelled the function name correctly and that the " +
                "function is properly deployed.", name );
    }

    public Set<ProcedureSignature> getAllProcedures()
    {
        return procedures.values().stream().map( CallableProcedure::signature ).collect( Collectors.toSet());
    }

    public Set<UserFunctionSignature> getAllFunctions()
    {
        return Stream.concat(functions.values().stream().map( CallableUserFunction::signature ),
                aggregationFunctions.values().stream().map( CallableUserAggregationFunction::signature ))
                .collect( Collectors.toSet() );
    }
}
