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
package org.neo4j.cypher.internal.preparser.javacc;

import java.util.List;
import java.util.Objects;
import org.neo4j.cypher.internal.PreParserOption;
import org.neo4j.cypher.internal.util.InputPosition;

public class PreParserResult {
    private final List<PreParserOption> options;
    private final InputPosition position;
    private static final String EMPTY_QUERY_PARSER_EXCEPTION_MSG =
            "Unexpected end of input: expected CYPHER, EXPLAIN, PROFILE or Query";

    public PreParserResult(List<PreParserOption> options, InputPosition position) {
        this.options = options;
        this.position = position;
    }

    public List<PreParserOption> options() {
        return options;
    }

    public InputPosition position() {
        return position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PreParserResult that = (PreParserResult) o;
        return Objects.equals(options, that.options) && Objects.equals(position, that.position);
    }

    @Override
    public int hashCode() {
        return Objects.hash(options, position);
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append("PreParserResult(List(");
        options.forEach((o) -> sb.append(o.toString()).append(','));
        sb.append("))");
        return sb.toString();
    }

    public static String getEmptyQueryExceptionMsg() {
        return EMPTY_QUERY_PARSER_EXCEPTION_MSG;
    }
}
