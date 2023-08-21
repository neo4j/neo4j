/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.database;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.systemgraph.CommunityTopologyGraphDbmsModel;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.CappedLogger;

public class SystemGraphDatabaseIdRepository implements DatabaseIdRepository {
    private final Supplier<Optional<? extends DatabaseContext>> systemDatabaseSupplier;
    private final CappedLogger cappedLogger;

    public SystemGraphDatabaseIdRepository(
            Supplier<Optional<? extends DatabaseContext>> systemDatabaseSupplier, InternalLogProvider logProvider) {
        this.systemDatabaseSupplier = systemDatabaseSupplier;
        this.cappedLogger = new CappedLogger(logProvider.getLog(getClass()), 10, TimeUnit.SECONDS, Clock.systemUTC());
    }

    @Override
    public Optional<NamedDatabaseId> getByName(NormalizedDatabaseName normalizedDatabaseName) {
        return execute(model -> model.getDatabaseIdByAlias(normalizedDatabaseName.name()));
    }

    @Override
    public Optional<NamedDatabaseId> getById(DatabaseId databaseId) {
        return execute(model -> model.getDatabaseIdByUUID(databaseId.uuid()));
    }

    private <T> Optional<T> execute(Function<TopologyGraphDbmsModel, Optional<T>> operation) {
        var databaseContext = systemDatabaseSupplier.get();
        if (databaseContext.isEmpty()) {
            cappedLogger.info("Could not retrieve the system database at this time.");
            return Optional.empty();
        }
        var systemDb = databaseContext.get().databaseFacade();
        if (!systemDb.isAvailable(100)) {
            cappedLogger.info("Currently, the system database is not available.");
            return Optional.empty();
        }
        try (var tx = systemDb.beginTx()) {
            var model = new CommunityTopologyGraphDbmsModel(tx);
            return operation.apply(model);
        }
    }
}
