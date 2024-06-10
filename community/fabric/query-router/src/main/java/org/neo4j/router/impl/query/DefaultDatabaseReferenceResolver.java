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
package org.neo4j.router.impl.query;

import java.util.Optional;
import java.util.function.Supplier;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.router.query.DatabaseReferenceResolver;

public class DefaultDatabaseReferenceResolver implements DatabaseReferenceResolver {
    private final DatabaseReferenceRepository repository;

    public DefaultDatabaseReferenceResolver(DatabaseReferenceRepository repository) {
        this.repository = repository;
    }

    @Override
    public DatabaseReference resolve(String name) {
        return resolve(new NormalizedDatabaseName(name));
    }

    @Override
    public DatabaseReference resolve(NormalizedDatabaseName name) {
        return repository
                .getByAlias(name)
                .or(() -> getCompositeConstituentAlias(name))
                .orElseThrow(databaseNotFound(name));
    }

    @Override
    public boolean isPropertyShardDatabase(NormalizedDatabaseName name) {
        var maybeSpd = repository.getSpdDatabaseReference();
        return maybeSpd.map(spd -> spd.entityDetailStores().values().stream()
                        .anyMatch(shardReference -> shardReference.fullName().equals(name)))
                .orElse(false);
    }

    private Optional<DatabaseReference> getCompositeConstituentAlias(NormalizedDatabaseName name) {
        return repository.getCompositeDatabaseReferences().stream()
                .flatMap(comp -> comp.constituents().stream())
                .filter(constituent -> constituent.fullName().equals(name))
                .findFirst();
    }

    private static Supplier<DatabaseNotFoundException> databaseNotFound(NormalizedDatabaseName databaseNameRaw) {
        return () -> new DatabaseNotFoundException("Graph not found: " + databaseNameRaw.name());
    }
}
