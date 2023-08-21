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

import static org.neo4j.shell.prettyprint.OutputFormatter.Capabilities.INFO;
import static org.neo4j.shell.prettyprint.OutputFormatter.Capabilities.RESULT;
import static org.neo4j.shell.prettyprint.OutputFormatter.Capabilities.STATISTICS;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.shell.state.BoltResult;

public class SimpleOutputFormatter implements OutputFormatter {

    @Override
    public int formatAndCount(BoltResult result, LinePrinter output) {
        Iterator<Record> records = result.iterate();
        int numberOfRows = 0;
        if (records.hasNext()) {
            Record firstRow = records.next();
            output.printOut(String.join(COMMA_SEPARATOR, firstRow.keys()));
            output.printOut(formatRecord(firstRow));
            numberOfRows++;
            while (records.hasNext()) {
                output.printOut(formatRecord(records.next()));
                numberOfRows++;
            }
        }
        return numberOfRows;
    }

    private String formatRecord(final Record record) {
        return record.values().stream().map(this::formatValue).collect(Collectors.joining(COMMA_SEPARATOR));
    }

    @Override
    public String formatInfo(ResultSummary summary) {
        if (!summary.hasPlan()) {
            return "";
        }
        Map<String, Value> info = OutputFormatter.info(summary);
        return info.entrySet().stream()
                .map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
                .collect(Collectors.joining(NEWLINE));
    }

    @Override
    public Set<Capabilities> capabilities() {
        return EnumSet.of(INFO, STATISTICS, RESULT);
    }
}
