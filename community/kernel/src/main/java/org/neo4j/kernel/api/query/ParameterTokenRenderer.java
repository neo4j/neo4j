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
package org.neo4j.kernel.api.query;

import java.util.HashMap;
import java.util.Map;

/**
 * Renders each redacted literal as {@code $`OBFUSCATED <TYPE> <n>`}, numbered per type in query-text order.
 * The obfuscator calls this once per distinct (type, source-text) literal,
 * so identical literals get identical parameter tokens.
 *
 * <p>The renderer holds per-type counters in instance state, so a fresh instance must be used per query
 * ({@link #create()}) to restart numbering.
 */
public final class ParameterTokenRenderer implements QueryObfuscator.ObfuscatedLiteralRenderer {
    private final Map<String, Integer> countersByType = new HashMap<>();

    @Override
    public String render(String typeName) {
        int n = countersByType.merge(typeName, 1, Integer::sum);
        return "$`OBFUSCATED " + typeName + " " + n + "`";
    }

    /** Fresh renderer per call so per-type numbering restarts for each query. */
    public static ParameterTokenRenderer create() {
        return new ParameterTokenRenderer();
    }
}
