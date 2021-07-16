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
package org.neo4j.dbms.database;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.storageengine.api.TransactionIdStore;

public class StandaloneDatabaseInfoService implements DatabaseInfoService
{
    private static final String ROLE_LABEL = "standalone";

    private final DatabaseIdRepository idRepository;
    private final ServerId serverId;
    private final SocketAddress address;
    private final DatabaseManager<?> databaseManager;
    private final DatabaseStateService stateService;

    public StandaloneDatabaseInfoService( ServerId serverId, SocketAddress address,
                                          DatabaseManager<?> databaseManager, DatabaseStateService stateService )
    {
        this.serverId = serverId;
        this.address = address;
        this.databaseManager = databaseManager;
        this.stateService = stateService;
        this.idRepository = databaseManager.databaseIdRepository();
    }

    @Override
    public List<DatabaseInfo> lookupCachedInfo( Set<String> databaseNames )
    {
        return createDatabaseInfoStream( databaseNames )
                .collect( Collectors.toList() );
    }

    @Override
    public List<ExtendedDatabaseInfo> requestDetailedInfo( Set<String> databaseNames )
    {
        return createDatabaseInfoStream( databaseNames )
                .map( databaseInfo -> databaseInfo.extendWith( getLastCommittedTransactionForDatabase( databaseInfo.namedDatabaseId() ) ) )
                .collect( Collectors.toList() );
    }

    private Stream<DatabaseInfo> createDatabaseInfoStream( Set<String> databaseNames )
    {
        return databaseNames.stream()
                            .map( idRepository::getByName )
                            .flatMap( Optional::stream )
                            .map( this::createInfoForDatabase );
    }

    private long getLastCommittedTransactionForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return databaseManager.getDatabaseContext( namedDatabaseId )
                              .map( DatabaseContext::dependencies )
                              .map( dependencies -> dependencies.resolveDependency( TransactionIdStore.class ) )
                              .map( TransactionIdStore::getLastCommittedTransactionId )
                              .orElse( ExtendedDatabaseInfo.COMMITTED_TX_ID_NOT_AVAILABLE );
    }

    private DatabaseInfo createInfoForDatabase( NamedDatabaseId namedDatabaseId )
    {
        var status = stateService.stateOfDatabase( namedDatabaseId ).operatorState().description();
        var error = stateService.causeOfFailure( namedDatabaseId ).map( Throwable::getMessage ).orElse( "" );
        return new DatabaseInfo( namedDatabaseId, serverId, address, ROLE_LABEL, status, error );
    }
}
