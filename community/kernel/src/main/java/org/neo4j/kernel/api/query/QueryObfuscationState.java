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

import java.util.function.Supplier;
import org.neo4j.kernel.api.query.QueryObfuscator.ObfuscatedQuery;
import org.neo4j.values.virtual.MapValue;

/**
 * Immutable, query-bound inputs plus a per-query memo of the two obfuscated views. Shared by reference between
 * the live {@link ExecutingQuery} and all its snapshots, so each view is computed at most once. Thread-safe via
 * idempotent recompute: the inputs are immutable and {@link #computeSafely} always returns a non-null bundle, so a
 * benign race just recomputes an equal value (no lock).
 */
final class QueryObfuscationState {
    private final QueryObfuscator obfuscator;
    private final String rawText;
    private final MapValue rawParameters;
    private final int preparserOffset;

    private volatile ObfuscatedQuery all;
    private volatile ObfuscatedQuery defaultView;

    QueryObfuscationState(QueryObfuscator obfuscator, String rawText, MapValue rawParameters, int preparserOffset) {
        this.obfuscator = obfuscator;
        this.rawText = rawText;
        this.rawParameters = rawParameters;
        this.preparserOffset = preparserOffset;
    }

    ObfuscatedQuery all() {
        ObfuscatedQuery a = all;
        if (a == null) {
            a = computeSafely(() -> obfuscator.fullyObfuscatedQuery(rawText, rawParameters, preparserOffset));
            all = a;
        }
        return a;
    }

    ObfuscatedQuery defaultView() {
        ObfuscatedQuery d = defaultView;
        if (d == null) {
            d = computeSafely(() -> obfuscator.defaultObfuscatedQuery(rawText, rawParameters, preparserOffset));
            defaultView = d;
        }
        return d;
    }

    private ObfuscatedQuery computeSafely(Supplier<ObfuscatedQuery> view) {
        try {
            return view.get();
        } catch (Throwable e) {
            // Any failure -> the absent view, so consumers fall back to raw; never leak the raw text here
            return ObfuscatedQuery.absent();
        }
    }
}
