package org.neo4j.router.query;

import java.util.Optional;
import java.util.function.Supplier;
import org.neo4j.cypher.internal.ast.CatalogName;

/**
 * Parse a query into a target database override
 *
 * A query can override the target database if it:
 * - contains a USE-clause, or
 * - contains a system admin command
 */
public interface QueryTargetParser {

    Optional<CatalogName> parseQueryTarget(Query query);

    interface Cache {
        Optional<CatalogName> computeIfAbsent(String query, Supplier<Optional<CatalogName>> supplier);
    }

    Cache NO_CACHE = new Cache() {
        @Override
        public Optional<CatalogName> computeIfAbsent(String query, Supplier<Optional<CatalogName>> supplier) {
            return supplier.get();
        }
    };
}
