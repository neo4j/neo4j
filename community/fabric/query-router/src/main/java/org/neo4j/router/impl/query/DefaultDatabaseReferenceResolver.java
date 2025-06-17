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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.neo4j.cypher.internal.ast.CatalogName;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.database.NormalizedCatalogEntry;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.router.query.DatabaseReferenceResolver;

public class DefaultDatabaseReferenceResolver implements DatabaseReferenceResolver {
    private final DatabaseReferenceRepository repository;

    public DefaultDatabaseReferenceResolver(DatabaseReferenceRepository repository) {
        this.repository = repository;
    }

    @Override
    public QueryTarget resolve(DatabaseReference sessionDatabase, CatalogName catalogName) { // boolean: resolveStrictly
        var containsQuotedNameParts = !catalogName.names().stream()
                .filter(name -> name.contains("."))
                .toList()
                .isEmpty();
        if (catalogName.names().size() > 2) {
            if (containsQuotedNameParts) {
                throw databaseNotFound(catalogName).get();
            } else {
                List<NormalizedCatalogEntry> allNameCombinations = getAllNameCombinations(catalogName.names());
                // find first matching reference and add notification
                return allNameCombinations.stream()
                        .flatMap(name -> queryTarget(name, catalogName, true).stream())
                        .findFirst()
                        .orElseThrow(databaseNotFound(catalogName));
            }
        } else if (catalogName.names().size() == 2) {
            if (catalogName.names().get(0).contains("."))
                throw databaseNotFound(catalogName).get();
            var constituentEntry = NormalizedCatalogEntry.fromList(catalogName.names());
            var aliasEntry = new NormalizedCatalogEntry(namePartsToName(catalogName.names()));
            if (containsQuotedNameParts) {
                // consider only user given choice
                return queryTarget(constituentEntry, catalogName, false).orElseThrow(databaseNotFound(catalogName));
            } else {
                if (sessionDatabase.isComposite()) {
                    // prefer constituent over alias
                    return queryTarget(constituentEntry, catalogName, true)
                            .or(() -> queryTarget(aliasEntry, catalogName, true))
                            .orElseThrow(databaseNotFound(catalogName));
                } else {
                    // prefer alias over constituent
                    return queryTarget(aliasEntry, catalogName, true)
                            .or(() -> queryTarget(constituentEntry, catalogName, true))
                            .orElseThrow(databaseNotFound(catalogName));
                }
            }
        } else {
            return queryTarget(NormalizedCatalogEntry.fromList(catalogName.names()), catalogName, false)
                    .orElseThrow(databaseNotFound(catalogName));
        }
    }

    private Optional<QueryTarget> queryTarget(
            NormalizedCatalogEntry catalogEntry, CatalogName catalogName, boolean addNotification) {
        var databaseReference = repository.getByAlias(catalogEntry);
        return databaseReference.map(QueryTarget::new);
    }

    static List<NormalizedCatalogEntry> getAllNameCombinations(List<String> nameParts) {
        List<NormalizedCatalogEntry> combinations = new ArrayList<>();
        combinations.add(new NormalizedCatalogEntry(namePartsToName(nameParts)));
        for (int i = 1; i < nameParts.size(); i++) {
            String composite = namePartsToName(nameParts.subList(0, i));
            String constituent = namePartsToName(nameParts.subList(i, nameParts.size()));
            combinations.add(new NormalizedCatalogEntry(composite, constituent));
        }
        return combinations;
    }

    private static String namePartsToName(List<String> nameParts) {
        return String.join(".", nameParts);
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

    private Optional<DatabaseReference> getCompositeConstituentAlias(NormalizedDatabaseName name) {
        return repository.getCompositeDatabaseReferences().stream()
                .flatMap(comp -> comp.constituents().stream())
                .filter(constituent -> constituent.fullName().equals(name))
                .findFirst();
    }

    private static Supplier<DatabaseNotFoundException> databaseNotFound(NormalizedDatabaseName databaseNameRaw) {
        return () -> new DatabaseNotFoundException("Graph not found: " + databaseNameRaw.name());
    }

    private static Supplier<DatabaseNotFoundException> databaseNotFound(CatalogName databaseNameRaw) {
        return () -> new DatabaseNotFoundException("Graph not found: " + databaseNameRaw.qualifiedNameString());
    }
}
