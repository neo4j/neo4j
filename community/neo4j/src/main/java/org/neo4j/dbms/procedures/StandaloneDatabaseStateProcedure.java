/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.dbms.procedures;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.values.AnyValue;

public class StandaloneDatabaseStateProcedure extends DatabaseStateProcedure
{
    private final DatabaseStateService stateService;
    private final String address;

    public StandaloneDatabaseStateProcedure( DatabaseStateService stateService, DatabaseIdRepository idRepository, Config config )
    {
        super( idRepository );
        this.stateService = stateService;
        this.address = config.get( BoltConnector.advertised_address ).toString();
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> apply( Context ctx, AnyValue[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        var databaseId = extractDatabaseId( input );
        var resultRow = resultRowFactory( databaseId, "standalone", address, stateService );
        return RawIterator.<AnyValue[],ProcedureException>of( resultRow );
    }
}
