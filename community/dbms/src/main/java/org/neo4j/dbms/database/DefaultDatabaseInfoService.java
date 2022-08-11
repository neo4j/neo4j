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

import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DatabaseAccess.READ_ONLY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DatabaseAccess.READ_WRITE;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;

public class DefaultDatabaseInfoService implements DatabaseInfoService {
    @Deprecated
    private static final String OLD_ROLE_LABEL = "standalone";

    private final DatabaseIdRepository idRepository;
    private final ReadOnlyDatabases readOnlyDatabases;
    private final ServerId serverId;
    private final SocketAddress address;
    private final DatabaseStateService stateService;
    private final DetailedDbInfoProvider detailedDbInfoProvider;

    public DefaultDatabaseInfoService(
            ServerId serverId,
            SocketAddress address,
            DatabaseContextProvider<?> databaseContextProvider,
            DatabaseStateService stateService,
            ReadOnlyDatabases readOnlyDatabases,
            DetailedDbInfoProvider detailedDbInfoProvider) {
        this.serverId = serverId;
        this.address = address;
        this.stateService = stateService;
        this.idRepository = databaseContextProvider.databaseIdRepository();
        this.readOnlyDatabases = readOnlyDatabases;
        this.detailedDbInfoProvider = detailedDbInfoProvider;
    }

    @Override
    public List<DatabaseInfo> lookupCachedInfo(Set<String> databaseNames) {
        return createDatabaseInfoStream(databaseNames).collect(Collectors.toList());
    }

    @Override
    public List<ExtendedDatabaseInfo> requestDetailedInfo(Set<String> databaseNames) {
        return createDatabaseInfoStream(databaseNames)
                .map(databaseInfo -> {
                    var db = databaseInfo.namedDatabaseId.databaseId();
                    var lastCommittedTxId = detailedDbInfoProvider.lastCommittedTxId(db);
                    var storeId = detailedDbInfoProvider.storeId(db);
                    return databaseInfo.extendWith(new DetailedDatabaseInfo(lastCommittedTxId, storeId));
                })
                .collect(Collectors.toList());
    }

    private Stream<DatabaseInfo> createDatabaseInfoStream(Set<String> databaseNames) {
        return databaseNames.stream()
                .map(idRepository::getByName)
                .flatMap(Optional::stream)
                .map(this::createInfoForDatabase);
    }

    private DatabaseInfo createInfoForDatabase(NamedDatabaseId namedDatabaseId) {
        var status =
                stateService.stateOfDatabase(namedDatabaseId).operatorState().description();
        var statusMessage = stateService
                .causeOfFailure(namedDatabaseId)
                .map(Throwable::getMessage)
                .orElse("");
        var access = readOnlyDatabases.isReadOnly(namedDatabaseId) ? READ_ONLY : READ_WRITE;
        return new DatabaseInfo(
                namedDatabaseId,
                serverId,
                access,
                address,
                null,
                OLD_ROLE_LABEL,
                DatabaseInfo.ROLE_PRIMARY,
                true,
                status,
                statusMessage);
    }
}
