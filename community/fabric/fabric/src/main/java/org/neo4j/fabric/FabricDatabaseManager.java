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
package org.neo4j.fabric;

import static java.lang.String.format;

import java.util.Optional;
import java.util.function.Supplier;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class FabricDatabaseManager {
    protected final FabricConfig fabricConfig;
    protected final DatabaseReferenceRepository databaseReferenceRepo;
    protected final DatabaseContextProvider<? extends DatabaseContext> databaseContextProvider;

    public FabricDatabaseManager(
            FabricConfig fabricConfig,
            DatabaseContextProvider<? extends DatabaseContext> databaseContextProvider,
            DatabaseReferenceRepository databaseReferenceRepo) {
        this.fabricConfig = fabricConfig;
        this.databaseContextProvider = databaseContextProvider;
        this.databaseReferenceRepo = databaseReferenceRepo;
    }

    public DatabaseReferenceRepository databaseReferenceRepository() {
        return databaseReferenceRepo;
    }

    public DatabaseReference getDatabaseReference(String databaseNameRaw) {
        return databaseReferenceRepo.getByAlias(databaseNameRaw).orElseThrow(databaseNotFound(databaseNameRaw));
    }

    public GraphDatabaseAPI getDatabaseFacade(String databaseNameRaw) throws UnavailableException {
        var databaseContext = getDatabaseContext(databaseNameRaw);
        databaseContext.database().getDatabaseAvailabilityGuard().assertDatabaseAvailable();
        return databaseContext.databaseFacade();
    }

    private DatabaseContext getDatabaseContext(String databaseNameRaw) throws UnavailableException {
        var databaseReference =
                databaseReferenceRepo.getByAlias(databaseNameRaw).orElseThrow(databaseNotFound(databaseNameRaw));
        return getDatabaseContext(databaseReference)
                .orElseThrow(() -> new UnavailableException(format("Database '%s' is unavailable.", databaseNameRaw)));
    }

    private Optional<? extends DatabaseContext> getDatabaseContext(DatabaseReference databaseReference) {
        if (databaseReference instanceof DatabaseReferenceImpl.Composite) {
            return databaseContextProvider.getDatabaseContext(
                    ((DatabaseReferenceImpl.Composite) databaseReference).databaseId());
        } else if (databaseReference instanceof DatabaseReferenceImpl.Internal) {
            return databaseContextProvider.getDatabaseContext(
                    ((DatabaseReferenceImpl.Internal) databaseReference).databaseId());
        } else {
            return Optional.empty();
        }
    }

    private static Supplier<DatabaseNotFoundException> databaseNotFound(String databaseNameRaw) {
        return () -> new DatabaseNotFoundException("Database " + databaseNameRaw + " not found");
    }

    public boolean isFabricDatabase(NamedDatabaseId databaseId) {
        // a "Fabric" database with special capabilities cannot exist in CE
        return false;
    }

    public FabricConfig getFabricConfig() {
        return fabricConfig;
    }
}
