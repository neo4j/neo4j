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
package org.neo4j.commandline.dbms;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.neo4j.kernel.diagnostics.DiagnosticsQueryResult;

/**
 * Renders a {@link DiagnosticsQueryResult} as pretty-printed JSON for inclusion in a diagnostics report. The result is
 * written as an array of objects, one per record, keyed by column name (in column order). Nested maps and lists - such
 * as the structures returned by {@code db.stats.retrieve('GRAPH COUNTS')} - are rendered as nested JSON rather than
 * being flattened into an opaque string.
 * <p>
 * Self-contained so that {@code neo4j-dbms} does not need a JSON library dependency. Values originate from the driver
 * ({@code Record.asMap()}): {@code null}, {@link Boolean}, {@link Number}, {@link String}, {@link Map} and
 * {@link Iterable} are rendered structurally; anything else falls back to its string representation.
 */
final class DiagnosticsJson {
    private static final String INDENT = "  ";

    private DiagnosticsJson() {}

    static String toJson(DiagnosticsQueryResult result) {
        StringBuilder sb = new StringBuilder();
        Iterator<Map<String, Object>> rows = result.rows().iterator();
        if (!rows.hasNext()) {
            return "[]\n";
        }
        sb.append("[\n");
        while (rows.hasNext()) {
            // Re-key by column order so the output is stable and matches the query's projection.
            Map<String, Object> ordered = new LinkedHashMap<>();
            Map<String, Object> row = rows.next();
            for (String column : result.columns()) {
                ordered.put(column, row.get(column));
            }
            indent(sb, 1);
            writeValue(sb, ordered, 1);
            sb.append(rows.hasNext() ? ",\n" : "\n");
        }
        sb.append("]\n");
        return sb.toString();
    }

    private static void writeValue(StringBuilder sb, Object value, int depth) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof Map<?, ?> map) {
            writeObject(sb, map, depth);
        } else if (value instanceof Iterable<?> iterable) {
            writeArray(sb, iterable, depth);
        } else if (value instanceof Boolean || value instanceof Number) {
            sb.append(value);
        } else {
            writeString(sb, String.valueOf(value));
        }
    }

    private static void writeObject(StringBuilder sb, Map<?, ?> map, int depth) {
        if (map.isEmpty()) {
            sb.append("{}");
            return;
        }
        sb.append("{\n");
        Iterator<? extends Map.Entry<?, ?>> entries = map.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<?, ?> entry = entries.next();
            indent(sb, depth + 1);
            writeString(sb, String.valueOf(entry.getKey()));
            sb.append(": ");
            writeValue(sb, entry.getValue(), depth + 1);
            sb.append(entries.hasNext() ? ",\n" : "\n");
        }
        indent(sb, depth);
        sb.append('}');
    }

    private static void writeArray(StringBuilder sb, Iterable<?> iterable, int depth) {
        Iterator<?> elements = iterable.iterator();
        if (!elements.hasNext()) {
            sb.append("[]");
            return;
        }
        sb.append("[\n");
        while (elements.hasNext()) {
            indent(sb, depth + 1);
            writeValue(sb, elements.next(), depth + 1);
            sb.append(elements.hasNext() ? ",\n" : "\n");
        }
        indent(sb, depth);
        sb.append(']');
    }

    private static void writeString(StringBuilder sb, String value) {
        sb.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    private static void indent(StringBuilder sb, int depth) {
        sb.append(INDENT.repeat(depth));
    }
}
