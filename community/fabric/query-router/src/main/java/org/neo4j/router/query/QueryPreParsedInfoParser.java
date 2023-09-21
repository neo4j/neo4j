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

import java.util.Optional;
import java.util.function.Supplier;
import org.neo4j.cypher.internal.ast.CatalogName;
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
            Optional<CatalogName> catalogName,
            Optional<ObfuscationMetadata> obfuscationMetadata,
            StatementType statementType) {}

    PreParsedInfo parseQuery(Query query);

    interface Cache {
        PreParsedInfo computeIfAbsent(String query, Supplier<PreParsedInfo> supplier);
    }

    Cache NO_CACHE = new Cache() {
        @Override
        public PreParsedInfo computeIfAbsent(String query, Supplier<PreParsedInfo> supplier) {
            return supplier.get();
        }
    };
}
