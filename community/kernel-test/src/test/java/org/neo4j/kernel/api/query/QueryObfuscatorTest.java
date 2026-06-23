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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.kernel.api.query.QueryObfuscator.ObfuscatedQuery;
import org.neo4j.values.virtual.MapValue;

class QueryObfuscatorTest {
    private static final String RAW_TEXT = "RETURN 'literal'";

    @Test
    void fullViewIsAbsentUnlessDeliberatelyProduced() {
        // An obfuscator that only knows how to redact sensitive literals (the per-piece methods) must not pass its
        // sensitive view off as the all-literals view: the interface default fails closed.
        QueryObfuscator sensitiveOnly = new QueryObfuscator() {
            @Override
            public String obfuscateText(String rawQueryText, int preparserOffset) {
                return "******";
            }

            @Override
            public Function<InputPosition, InputPosition> obfuscatePosition(String rawQueryText, int preparserOffset) {
                return Function.identity();
            }

            @Override
            public MapValue obfuscateParameters(MapValue rawQueryParameters) {
                return rawQueryParameters;
            }
        };

        var view = sensitiveOnly.fullyObfuscatedQuery(RAW_TEXT, MapValue.EMPTY, 0);

        assertThat(ObfuscatedQuery.optional(view)).isEmpty();
    }

    @Test
    void passthroughExposesRawTextAsTheFullView() {
        // PASSTHROUGH is only selected when the query has no literals at all and the full view may be
        // exposed, so its full view is legitimately the raw text.
        var view = QueryObfuscator.PASSTHROUGH.fullyObfuscatedQuery(RAW_TEXT, MapValue.EMPTY, 0);

        assertThat(view.text()).isEqualTo(RAW_TEXT);
        assertThat(view.parameters()).isEqualTo(MapValue.EMPTY);
    }
}
