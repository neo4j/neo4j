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
package org.neo4j.router.query;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.neo4j.cypher.internal.ast.CatalogName;
import org.neo4j.cypher.internal.options.CypherExecutionMode;
import org.neo4j.cypher.internal.util.ObfuscationMetadata;
import org.neo4j.router.impl.query.StatementType;

/**
 * Parse a query into a target database override
 *
 * A query can override the target database if it:
 * - contains a USE-clause, or
 * - contains a system admin command
 */
public interface QueryPreParsedInfoParser {

    record PreParsedInfo(
            CatalogInfo catalogInfo,
            Optional<ObfuscationMetadata> obfuscationMetadata,
            StatementType statementType,
            CypherExecutionMode cypherExecutionMode) {}

    sealed interface CatalogInfo permits SingleQueryCatalogInfo, UnionQueryCatalogInfo {}

    record SingleQueryCatalogInfo(Optional<CatalogName> catalogName) implements CatalogInfo {}

    record UnionQueryCatalogInfo(List<Optional<CatalogName>> catalogNames) implements CatalogInfo {}

    PreParsedInfo parseQuery(Query query);

    long clearQueryCachesForDatabase(String databaseName);

    interface Cache {
        PreParsedInfo computeIfAbsent(String query, Supplier<PreParsedInfo> supplier);

        long clearQueryCachesForDatabase(String databaseName);
    }
}
