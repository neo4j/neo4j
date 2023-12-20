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
import org.neo4j.cypher.internal.ast.CatalogName;
import org.neo4j.cypher.messages.MessageUtilProvider;
import org.neo4j.exceptions.InvalidSemanticsException;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.router.query.DatabaseReferenceResolver;
import org.neo4j.router.query.TargetService;

public class StandardTargetService implements TargetService {

    private final DatabaseReference sessionDatabase;
    private final DatabaseReferenceResolver databaseReferenceResolver;

    public StandardTargetService(
            DatabaseReference sessionDatabase, DatabaseReferenceResolver databaseReferenceResolver) {
        this.sessionDatabase = sessionDatabase;
        this.databaseReferenceResolver = databaseReferenceResolver;
    }

    @Override
    public DatabaseReference target(CatalogInfo catalogInfo) {
        var parsedTarget = toCatalogName(catalogInfo)
                .map(CatalogName::qualifiedNameString)
                .map(databaseReferenceResolver::resolve);
        if (parsedTarget
                .filter(target -> target.isComposite()
                        || (!target.isPrimary() && target.namespace().isPresent()))
                .isPresent()) {
            var message = "Accessing a composite database and its constituents is only allowed when connected to it. "
                    + "Attempted to access '%s' while connected to '%s'";
            throw new InvalidSemanticsException(
                    String.format(message, parsedTarget.get().toPrettyString(), sessionDatabase.toPrettyString()));
        }
        return parsedTarget.orElse(sessionDatabase);
    }

    private Optional<CatalogName> toCatalogName(CatalogInfo catalogInfo) {
        if (catalogInfo instanceof SingleQueryCatalogInfo singleQueryCatalogInfo) {
            return singleQueryCatalogInfo.catalogName();
        }

        if (catalogInfo instanceof UnionQueryCatalogInfo unionQueryCatalogInfo) {
            var catalogName = unionQueryCatalogInfo.catalogNames().get(0);
            // We have to check for one specific combination of an ambient and explicit graph:

            // USE foo
            // MATCH (n) RETURN n
            // UNION
            // MATCH (n) RETURN n

            // The reason is that the meaning of what is the ambient graph changes when routing is performed.
            // The example query would become valid after being routed to database foo, which is incorrect.
            // Any other invalid combinations are either caught in semantic analysis or in the target database.

            if (catalogName.isPresent()
                    && unionQueryCatalogInfo.catalogNames().stream().anyMatch(Optional::isEmpty)) {
                var normalizedDatabaseName =
                        new NormalizedDatabaseName(catalogName.get().qualifiedNameString());
                if (!sessionDatabase.fullName().name().equals(normalizedDatabaseName.name())) {
                    throw new InvalidSemanticsException(MessageUtilProvider.createMultipleGraphReferencesError(
                            normalizedDatabaseName.name(), false));
                }
            }
            return catalogName;
        }

        throw new IllegalArgumentException("Unexpected catalog info " + catalogInfo);
    }
}
