/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.dbms.procedures;

import java.util.Arrays;
import java.util.UUID;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.values.storable.Values.stringValue;

public abstract class DatabaseStateProcedure extends CallableProcedure.BasicProcedure
{
    private static final String PROCEDURE_NAME = "state";
    private static final String[] PROCEDURE_NAMESPACE = {"dbms","database"};
    private static final String PARAMETER_NAME = "databaseId";
    protected final DatabaseIdRepository idRepository;

    DatabaseStateProcedure( DatabaseIdRepository idRepository )
    {
        super( procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME ) )
                .in( PARAMETER_NAME, Neo4jTypes.NTString )
                .out( "role", Neo4jTypes.NTString )
                .out( "address", Neo4jTypes.NTString )
                .out( "status", Neo4jTypes.NTString )
                .out( "error", Neo4jTypes.NTString )
                .description( "The actual status of the database with the provided id on this neo4j instance." )
                .systemProcedure()
                .build() );
        this.idRepository = idRepository;
    }

    DatabaseId extractDatabaseId( AnyValue[] input ) throws ProcedureException
    {
        if ( input.length != 1 )
        {
            throw new IllegalArgumentException( "Illegal input:" + Arrays.toString( input ) );
        }
        var rawId = input[0];
        if ( !( rawId instanceof TextValue) )
        {
            throw new IllegalArgumentException( format( "Parameter '%s' value should be a string representation of a UUID: %s", PARAMETER_NAME, rawId ) );
        }
        var uuid = UUID.fromString( ((TextValue) rawId).stringValue() );
        return idRepository.getByUuid( uuid )
                .orElseThrow( () -> new ProcedureException( DatabaseNotFound, format( "Unable to retrieve the status " +
                        "for database with id %s because no database with this id exists!", uuid ) ) );
    }

    AnyValue[] resultRowFactory( DatabaseId databaseId, String role, String address, DatabaseStateService stateService )
    {
        var status = stateService.stateOfDatabase( databaseId );
        var formattedStatus = stringValue( status.description() );
        var error = stateService.causeOfFailure( databaseId ).map( Throwable::getMessage );
        var formattedError = error.map( Values::stringValue ).orElse( stringValue( "" ) );
        return new AnyValue[]{ stringValue( role ), stringValue( address ), formattedStatus, formattedError };
    }
}
