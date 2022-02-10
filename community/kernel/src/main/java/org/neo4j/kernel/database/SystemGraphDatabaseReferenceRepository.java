/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.database;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.systemgraph.CommunityTopologyGraphDbmsModel;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.graphdb.DatabaseShutdownException;

import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.NamedDatabaseId.SYSTEM_DATABASE_NAME;

public class SystemGraphDatabaseReferenceRepository implements DatabaseReferenceRepository
{
    private static final DatabaseReference SYSTEM_DATABASE_REFERENCE =
            new DatabaseReference.Internal( new NormalizedDatabaseName( SYSTEM_DATABASE_NAME ), NAMED_SYSTEM_DATABASE_ID );

    private final Supplier<DatabaseContext> systemDatabaseSupplier;

    public SystemGraphDatabaseReferenceRepository( Supplier<DatabaseContext> systemDatabaseSupplier )
    {
        this.systemDatabaseSupplier = systemDatabaseSupplier;
    }

    @Override
    public Optional<DatabaseReference> getByName( NormalizedDatabaseName databaseName )
    {
        if ( Objects.equals( SYSTEM_DATABASE_NAME, databaseName.name() ) )
        {
            return Optional.of( SYSTEM_DATABASE_REFERENCE );
        }
        return execute( model -> model.getDatabaseRefByAlias( databaseName.name() ) );
    }

    @Override
    public Set<DatabaseReference> getAllDatabaseReferences()
    {
        return execute( TopologyGraphDbmsModel::getAllDatabaseReferences );
    }

    @Override
    public Set<DatabaseReference.Internal> getInternalDatabaseReferences()
    {
        return execute( TopologyGraphDbmsModel::getAllInternalDatabaseReferences );
    }

    @Override
    public Set<DatabaseReference.External> getExternalDatabaseReferences()
    {
        return execute( TopologyGraphDbmsModel::getAllExternalDatabaseReferences );
    }

    private <T> T execute( Function<TopologyGraphDbmsModel,T> operation )
    {
        var databaseContext = systemDatabaseSupplier.get();
        var systemDb = databaseContext.databaseFacade();
        if ( !systemDb.isAvailable( 100 ) )
        {
            throw new DatabaseShutdownException( new DatabaseManagementException( "System database is not (yet) available" ) );
        }
        try ( var tx = systemDb.beginTx() )
        {
            var model = new CommunityTopologyGraphDbmsModel( tx );
            return operation.apply( model );
        }
    }
}
