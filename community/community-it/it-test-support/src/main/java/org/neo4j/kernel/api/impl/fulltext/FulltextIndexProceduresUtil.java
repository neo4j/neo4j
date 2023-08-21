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
package org.neo4j.kernel.api.impl.fulltext;

import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.ANALYZER;

import java.util.Arrays;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public final class FulltextIndexProceduresUtil {
    public static final String SHOW_FULLTEXT_INDEXES = "SHOW FULLTEXT INDEXES";
    public static final String LIST_AVAILABLE_ANALYZERS = "CALL db.index.fulltext.listAvailableAnalyzers()";
    public static final String DB_AWAIT_INDEX = "CALL db.awaitIndex(\"%s\")";
    public static final String QUERY_NODES = "CALL db.index.fulltext.queryNodes(\"%s\", \"%s\")";
    public static final String QUERY_RELS = "CALL db.index.fulltext.queryRelationships(\"%s\", \"%s\")";
    public static final String AWAIT_REFRESH = "CALL db.index.fulltext.awaitEventuallyConsistentIndexRefresh()";
    public static final String FULLTEXT_CREATE = "CREATE FULLTEXT INDEX `%s` FOR %s ON EACH %s";
    public static final String FULLTEXT_CREATE_WITH_CONFIG =
            "CREATE FULLTEXT INDEX `%s` FOR %s ON EACH %s OPTIONS {indexConfig: %s}";

    private FulltextIndexProceduresUtil() {}

    public static String asNodeLabelStr(String... args) {
        if (args.length == 0) {
            return "(e)";
        }
        return Arrays.stream(args).collect(Collectors.joining("|", "(e:", ")"));
    }

    public static String asRelationshipTypeStr(String... args) {
        if (args.length == 0) {
            return "()-[e]-()";
        }
        return Arrays.stream(args).collect(Collectors.joining("|", "()-[e:", "]-()"));
    }

    public static String asPropertiesStrList(String... args) {
        if (args.length == 0) {
            return "[]";
        }
        return Arrays.stream(args).collect(Collectors.joining(", e.", "[e.", "]"));
    }

    public static Map<String, Value> asConfigMap(String analyzer, boolean eventuallyConsistent) {
        return Map.of(
                ANALYZER,
                Values.stringValue(analyzer),
                FulltextIndexSettingsKeys.EVENTUALLY_CONSISTENT,
                Values.booleanValue(eventuallyConsistent));
    }

    public static String asConfigMapString(String analyzer, boolean eventuallyConsistent) {
        StringJoiner joiner = new StringJoiner(", ", "{", "}");

        joiner.add("`" + ANALYZER + "`: \"" + analyzer + "\"");
        joiner.add("`" + FulltextIndexSettingsKeys.EVENTUALLY_CONSISTENT + "`: " + eventuallyConsistent);

        return joiner.toString();
    }
}
