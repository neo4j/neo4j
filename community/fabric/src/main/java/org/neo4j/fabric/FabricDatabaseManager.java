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
package org.neo4j.fabric;

import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.config.FabricSettings;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class FabricDatabaseManager {
    protected final FabricConfig fabricConfig;
    protected final DatabaseReferenceRepository databaseReferenceRepo;
    protected final DatabaseContextProvider<? extends DatabaseContext> databaseContextProvider;
    private final boolean multiGraphEverywhere;

    public FabricDatabaseManager(
            FabricConfig fabricConfig,
            DatabaseContextProvider<? extends DatabaseContext> databaseContextProvider,
            DatabaseReferenceRepository databaseReferenceRepo) {
        this.fabricConfig = fabricConfig;
        this.databaseContextProvider = databaseContextProvider;
        this.databaseReferenceRepo = databaseReferenceRepo;
        this.multiGraphEverywhere = fabricConfig.isEnabledByDefault();
    }

    public static boolean fabricByDefault(Config config) {
        return config.get(FabricSettings.enabled_by_default);
    }

    public DatabaseReferenceRepository databaseReferenceRepository() {
        return databaseReferenceRepo;
    }

    public boolean hasMultiGraphCapabilities(String databaseNameRaw) {
        return multiGraphCapabilitiesEnabledForAllDatabases() || isFabricDatabase(databaseNameRaw);
    }

    public boolean multiGraphCapabilitiesEnabledForAllDatabases() {
        return multiGraphEverywhere;
    }

    public DatabaseReference getDatabaseReference(String databaseNameRaw) throws UnavailableException {
        var ref = databaseReferenceRepo.getByAlias(databaseNameRaw).orElseThrow(databaseNotFound(databaseNameRaw));
        if (ref instanceof DatabaseReference.Internal internal) {
            assertInternalDatabaseAvailable(internal);
        }
        return ref;
    }

    public GraphDatabaseAPI getDatabaseFacade(String databaseNameRaw) throws UnavailableException {
        var databaseContext = getDatabaseContext(databaseNameRaw);
        databaseContext.database().getDatabaseAvailabilityGuard().assertDatabaseAvailable();
        return databaseContext.databaseFacade();
    }

    private DatabaseContext getDatabaseContext(String databaseNameRaw) {
        return databaseReferenceRepo
                .getInternalByAlias(databaseNameRaw)
                .map(DatabaseReference.Internal::databaseId)
                .flatMap(databaseContextProvider::getDatabaseContext)
                .orElseThrow(databaseNotFound(databaseNameRaw));
    }

    private static Supplier<DatabaseNotFoundException> databaseNotFound(String databaseNameRaw) {
        return () -> new DatabaseNotFoundException("Database " + databaseNameRaw + " not found");
    }

    private void assertInternalDatabaseAvailable(DatabaseReference.Internal databaseReference)
            throws UnavailableException {
        var ctx = databaseContextProvider
                .getDatabaseContext(databaseReference.databaseId())
                .orElseThrow(databaseNotFound(databaseReference.alias().name()));
        ctx.database().getDatabaseAvailabilityGuard().assertDatabaseAvailable();
    }

    public boolean isFabricDatabase(String databaseNameRaw) {
        // a "Fabric" database with special capabilities cannot exist in CE
        return false;
    }

    public boolean isFabricDatabasePresent() {
        return false;
    }

    public FabricConfig getFabricConfig() {
        return fabricConfig;
    }
}
;
