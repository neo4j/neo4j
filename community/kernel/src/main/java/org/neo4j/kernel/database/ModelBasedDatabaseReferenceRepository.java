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

import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.NamedDatabaseId.SYSTEM_DATABASE_NAME;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;

public class ModelBasedDatabaseReferenceRepository implements DatabaseReferenceRepository {
    private static final DatabaseReference SYSTEM_DATABASE_REFERENCE = new DatabaseReferenceImpl.Internal(
            new NormalizedDatabaseName(SYSTEM_DATABASE_NAME), NAMED_SYSTEM_DATABASE_ID, true);

    private final DatabaseObjectRepositoryModelProvider modelProvider;

    public ModelBasedDatabaseReferenceRepository(DatabaseObjectRepositoryModelProvider modelProvider) {
        this.modelProvider = modelProvider;
    }

    @Override
    public Optional<DatabaseReference> getByAlias(NormalizedCatalogEntry catalogEntry) {
        if (catalogEntry.compositeDb().isEmpty() && catalogEntry.databaseAlias().equals(SYSTEM_DATABASE_NAME)) {
            return Optional.of(SYSTEM_DATABASE_REFERENCE);
        }
        return modelProvider.withModel(model -> model.getDatabaseRefByAlias(catalogEntry));
    }

    @Override
    public Optional<DatabaseReference> getByAlias(NormalizedDatabaseName databaseAlias) {
        return getByAlias(new NormalizedCatalogEntry(normalizeCatalogName(databaseAlias.name())));
    }

    @Override
    public Optional<DatabaseReference> getByDisplayName(NormalizedDatabaseName databaseAlias) {
        if (databaseAlias.name().equals(SYSTEM_DATABASE_NAME)) {
            return Optional.of(SYSTEM_DATABASE_REFERENCE);
        }
        return modelProvider.withModel(model -> model.getDatabaseRefByDisplayName(databaseAlias));
    }

    @Override
    public Optional<DatabaseReference> getByUuid(UUID databaseId) {
        if (Objects.equals(SYSTEM_DATABASE_REFERENCE.id(), databaseId)) {
            return Optional.of(SYSTEM_DATABASE_REFERENCE);
        }

        return modelProvider.withModel(model -> model.getDatabaseIdByUUID(databaseId)
                .flatMap(id -> model.getDatabaseRefByAlias(new NormalizedCatalogEntry(id.name()))));
    }

    private String normalizeCatalogName(String databaseAlias) {
        // catalog has been quoted due to a .
        // TODO: quoted composite name literals need to keep quotation
        if (databaseAlias.matches("`.*\\..*`")) {
            String unquoted = databaseAlias.substring(1, databaseAlias.length() - 1);
            return unquoted.replace("``", "`");
        }
        return databaseAlias;
    }

    @Override
    public Set<DatabaseReference> getAllDatabaseReferences() {
        return modelProvider.withModel(TopologyGraphDbmsModel::getAllDatabaseReferences);
    }

    @Override
    public Set<DatabaseReferenceImpl.Composite> getCompositeDatabaseReferences() {
        return modelProvider.withModel(TopologyGraphDbmsModel::getAllCompositeDatabaseReferences);
    }
}
