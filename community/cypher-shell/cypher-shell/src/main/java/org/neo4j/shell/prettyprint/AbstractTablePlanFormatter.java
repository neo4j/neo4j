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
package org.neo4j.shell.prettyprint;

import static java.util.stream.Collectors.toMap;
import static org.neo4j.shell.prettyprint.OutputFormatter.NEWLINE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.driver.summary.Plan;

abstract class AbstractTablePlanFormatter {
    protected abstract String operatorHeader();

    protected abstract List<String> headers();

    private static void pad(int width, char chr, StringBuilder result) {
        result.repeat(chr, width);
    }

    private static int width(String header, Map<String, Integer> columns) {
        return 2 + Math.max(header.length(), columns.get(header));
    }

    private void divider(
            List<String> headers, TableRow tableRow /*= null*/, StringBuilder result, Map<String, Integer> columns) {
        for (String header : headers) {
            if (tableRow != null && header.equals(operatorHeader()) && tableRow.connection.isPresent()) {
                result.append("|");
                String connection = tableRow.connection.get();
                result.append(" ").append(connection);
                pad(width(header, columns) - connection.length() - 1, ' ', result);
            } else {
                result.append("+");
                pad(width(header, columns), '-', result);
            }
        }
        result.append("+").append(NEWLINE);
    }

    protected abstract Level rootLevel();

    String formatPlan(Plan plan) {
        Map<String, Integer> columns = new HashMap<>();
        List<TableRow> tableRows = accumulate(plan, rootLevel(), columns);

        // Remove Identifiers column if we have a Details column
        List<String> headers = headers().stream().filter(columns::containsKey).collect(Collectors.toList());

        StringBuilder result = new StringBuilder((2
                        + NEWLINE.length()
                        + headers.stream().mapToInt(h -> width(h, columns)).sum())
                * (tableRows.size() * 2 + 3));

        List<TableRow> allTableRows = new ArrayList<>();
        Map<String, Cell> headerMap = headers.stream()
                .map(header -> Pair.of(header, new LeftJustifiedCell(header)))
                .collect(toMap(p -> p._1, p -> p._2));
        allTableRows.add(new TableRow(operatorHeader(), headerMap, Optional.empty()));
        allTableRows.addAll(tableRows);
        for (int rowIndex = 0; rowIndex < allTableRows.size(); rowIndex++) {
            TableRow tableRow = allTableRows.get(rowIndex);
            divider(headers, tableRow, result, columns);
            for (int rowLineIndex = 0; rowLineIndex < tableRow.height; rowLineIndex++) {
                for (String header : headers) {
                    Cell cell = tableRow.get(header);
                    String defaultText = "";
                    if (header.equals(operatorHeader()) && rowIndex + 1 < allTableRows.size()) {
                        defaultText = allTableRows
                                .get(rowIndex + 1)
                                .connection
                                .orElse("")
                                .replace('\\', ' ');
                    }
                    result.append("| ");
                    int columnWidth = width(header, columns);
                    cell.writePaddedLine(rowLineIndex, defaultText, columnWidth, result);
                    result.append(" ");
                }
                result.append("|").append(NEWLINE);
            }
        }
        divider(headers, null, result, columns);

        return result.toString();
    }

    protected abstract Stream<List<TableRow>> children(Plan plan, Level level, Map<String, Integer> columns);

    protected abstract String operatorType(Plan plan, Level level);

    protected List<TableRow> accumulate(Plan plan, Level level, Map<String, Integer> columns) {
        String line = level.line() + operatorType(plan, level);
        mapping(operatorHeader(), new LeftJustifiedCell(line), columns);

        return Stream.concat(
                        Stream.of(new TableRow(line, details(plan, columns), level.connector())),
                        children(plan, level, columns).flatMap(Collection::stream))
                .collect(Collectors.toList());
    }

    protected abstract Map<String, Cell> details(Plan plan, Map<String, Integer> columns);

    private static Optional<Pair<String, Cell>> mapping(String key, Cell value, Map<String, Integer> columns) {
        update(columns, key, value.length);
        return Optional.of(Pair.of(key, value));
    }

    private static void update(Map<String, Integer> columns, String key, int length) {
        columns.put(key, Math.max(columns.getOrDefault(key, 0), length));
    }

    protected class TableRow {
        private final String tree;
        private final Map<String, Cell> cells;
        private final Optional<String> connection;
        private final int height;

        TableRow(String tree, Map<String, Cell> cells, Optional<String> connection) {
            this.tree = tree;
            this.cells = cells;
            this.connection = connection == null ? Optional.empty() : connection;
            this.height =
                    cells.values().stream().mapToInt(v -> v.lines.length).max().orElse(0);
        }

        Cell get(String key) {
            if (key.equals(operatorHeader())) {
                return new LeftJustifiedCell(tree);
            } else {
                return cells.getOrDefault(key, new LeftJustifiedCell(""));
            }
        }
    }

    abstract static class Cell {
        final int length;
        final String[] lines;

        Cell(String[] lines) {
            this.length = Stream.of(lines).mapToInt(String::length).max().orElse(0);
            this.lines = lines;
        }

        abstract void writePaddedLine(int lineIndex, String orElseValue, int columnWidth, StringBuilder result);

        protected static int paddingWidth(int columnWidth, String line) {
            return columnWidth - line.length() - 2;
        }

        protected String getLineOrElse(int lineIndex, String orElseValue) {
            if (lineIndex < lines.length) {
                return lines[lineIndex];
            } else {
                return orElseValue;
            }
        }
    }

    static class LeftJustifiedCell extends Cell {
        LeftJustifiedCell(String... lines) {
            super(lines);
        }

        @Override
        void writePaddedLine(int lineIndex, String orElseValue, int columnWidth, StringBuilder result) {
            String line = getLineOrElse(lineIndex, orElseValue);
            result.append(line);
            pad(paddingWidth(columnWidth, line), ' ', result);
        }
    }

    static class RightJustifiedCell extends Cell {
        RightJustifiedCell(String... lines) {
            super(lines);
        }

        @Override
        void writePaddedLine(int lineIndex, String orElseValue, int columnWidth, StringBuilder result) {
            String line = getLineOrElse(lineIndex, orElseValue);
            pad(paddingWidth(columnWidth, line), ' ', result);
            result.append(line);
        }
    }

    protected abstract static class Level {
        abstract Level sibling();

        abstract Level lastSibling();

        abstract Level firstChild();

        abstract Level onlyChild();

        abstract String line();

        abstract Optional<String> connector();
    }

    static final class Pair<T1, T2> {
        final T1 _1;
        final T2 _2;

        private Pair(T1 _1, T2 _2) {
            this._1 = _1;
            this._2 = _2;
        }

        public static <T1, T2> Pair<T1, T2> of(T1 _1, T2 _2) {
            return new Pair<>(_1, _2);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Pair<?, ?> pair = (Pair<?, ?>) o;
            return _1.equals(pair._1) && _2.equals(pair._2);
        }

        @Override
        public int hashCode() {
            return 31 * _1.hashCode() + _2.hashCode();
        }
    }
}
