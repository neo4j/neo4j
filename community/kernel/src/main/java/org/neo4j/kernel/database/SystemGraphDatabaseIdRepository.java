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

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.dbms.database.CommunityTopologyGraphDbmsModel;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.TopologyGraphDbmsModel;

public class SystemGraphDatabaseIdRepository implements DatabaseIdRepository
{
    private final Supplier<DatabaseContext> systemDatabaseSupplier;

    public SystemGraphDatabaseIdRepository( Supplier<DatabaseContext> systemDatabaseSupplier )
    {
        this.systemDatabaseSupplier = systemDatabaseSupplier;
    }

    @Override
    public Optional<NamedDatabaseId> getByName( NormalizedDatabaseName normalizedDatabaseName )
    {
        return execute( model -> model.getDatabaseIdByAlias( normalizedDatabaseName.name() ) );
    }

    @Override
    public Optional<NamedDatabaseId> getById( DatabaseId databaseId )
    {
        return execute( model -> model.getDatabaseIdByUUID( databaseId.uuid() ) );
    }

    @Override
    public Map<NormalizedDatabaseName,NamedDatabaseId> getAllDatabaseAliases()
    {
        var aliases = execute( TopologyGraphDbmsModel::getAllDatabaseAliases );
        return aliases.entrySet().stream()
                      .map( e -> Map.entry( new NormalizedDatabaseName( e.getKey() ), e.getValue() ) )
                      .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
    }

    private <T>  T execute( Function<TopologyGraphDbmsModel,T> operation )
    {
        var systemDb = systemDatabaseSupplier.get().databaseFacade();
        try ( var tx = systemDb.beginTx() )
        {
            var model = new CommunityTopologyGraphDbmsModel( tx );
            return operation.apply( model );
        }
    }
}
