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

import java.util.Optional;
import java.util.function.Function;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.values.virtual.MapValue;

public interface QueryObfuscator {

    /**
     * One obfuscated view of a query. {@code text} is {@code null} when obfuscation failed; obtain views via
     * {@link #optional(ObfuscatedQuery)} so a failure surfaces as {@link Optional#empty()} rather than a null-text bundle.
     */
    record ObfuscatedQuery(String text, MapValue parameters, Function<InputPosition, InputPosition> positionMap) {

        /** Empty when the view was not produced or obfuscation failed (null text); present otherwise. */
        public static Optional<ObfuscatedQuery> optional(ObfuscatedQuery view) {
            return view == null || view.text == null ? Optional.empty() : Optional.of(view);
        }

        /** The absent view: not produced (or production failed), surfaced as empty by {@link #optional}. */
        public static ObfuscatedQuery absent() {
            return new ObfuscatedQuery(null, null, null);
        }
    }

    String obfuscateText(String rawQueryText, int preparserOffset);

    Function<InputPosition, InputPosition> obfuscatePosition(String rawQueryText, int preparserOffset);

    MapValue obfuscateParameters(MapValue rawQueryParameters);

    default ObfuscatedQuery sensitiveObfuscatedQuery(
            String rawQueryText, MapValue rawQueryParameters, int preparserOffset) {
        return new ObfuscatedQuery(
                obfuscateText(rawQueryText, preparserOffset),
                obfuscateParameters(rawQueryParameters),
                obfuscatePosition(rawQueryText, preparserOffset));
    }

    /**
     * The all-literals view: every literal redacted, independent of {@code obfuscate_literals}. Fails closed — the
     * default is the absent view, so an implementation never passes its sensitive view off as fully obfuscated.
     */
    default ObfuscatedQuery fullyObfuscatedQuery(
            String rawQueryText, MapValue rawQueryParameters, int preparserOffset) {
        return ObfuscatedQuery.absent();
    }

    /**
     * The default view: all-literals when {@code obfuscate_literals} is enabled, sensitive-only otherwise — the
     * level is chosen where the obfuscator is built.
     */
    default ObfuscatedQuery defaultObfuscatedQuery(
            String rawQueryText, MapValue rawQueryParameters, int preparserOffset) {
        return sensitiveObfuscatedQuery(rawQueryText, rawQueryParameters, preparserOffset);
    }

    QueryObfuscator PASSTHROUGH = new QueryObfuscator() {

        @Override
        public String obfuscateText(String rawQueryText, int preparserOffset) {
            return rawQueryText;
        }

        @Override
        public Function<InputPosition, InputPosition> obfuscatePosition(String rawQueryText, int preparserOffset) {
            return Function.identity();
        }

        @Override
        public MapValue obfuscateParameters(MapValue rawQueryParameters) {
            return rawQueryParameters;
        }

        @Override
        public ObfuscatedQuery fullyObfuscatedQuery(
                String rawQueryText, MapValue rawQueryParameters, int preparserOffset) {
            return new ObfuscatedQuery(rawQueryText, rawQueryParameters, Function.identity());
        }
    };
}
