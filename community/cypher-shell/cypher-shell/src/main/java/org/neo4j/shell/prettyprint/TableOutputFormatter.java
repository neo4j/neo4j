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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyIterator;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.shell.prettyprint.OutputFormatter.repeat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.value.NumberValueAdapter;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.shell.state.BoltResult;

public class TableOutputFormatter implements OutputFormatter {

    public static final String STRING_REPRESENTATION = "string-representation";
    private final boolean wrap;
    private final int numSampleRows;

    public TableOutputFormatter(boolean wrap, int numSampleRows) {
        this.wrap = wrap;
        this.numSampleRows = numSampleRows;
    }

    @Override
    public int formatAndCount(BoltResult result, LinePrinter output) {
        String[] columns = result.getKeys().toArray(new String[0]);
        if (columns.length == 0) {
            return 0;
        }

        Iterator<Record> records = result.iterate();
        return formatResultAndCountRows(columns, records, output);
    }

    /**
     * Prints bolt result with a heading. Not optimised for large result sets.
     */
    public void formatWithHeading(BoltResult result, LinePrinter output, String heading) {
        final String[] columns = result.getKeys().toArray(new String[0]);
        printTableAndCountRows(columns, emptyIterator(), output, result.getRecords(), true, heading);
    }

    private static void take(Iterator<Record> records, ArrayList<Record> topRecords, int count) {
        while (records.hasNext() && topRecords.size() < count) {
            topRecords.add(records.next());
        }
    }

    private int formatResultAndCountRows(String[] columns, Iterator<Record> records, LinePrinter output) {

        ArrayList<Record> topRecords = new ArrayList<>(numSampleRows);
        try {
            take(records, topRecords, numSampleRows);
        } catch (RuntimeException e) {
            printTableAndCountRows(columns, records, output, topRecords, false, null);
            throw e;
        }
        return printTableAndCountRows(columns, records, output, topRecords, true, null);
    }

    private int printTableAndCountRows(
            String[] columns,
            Iterator<Record> records,
            LinePrinter output,
            List<Record> topRecords,
            boolean printFooter,
            String heading) {
        int[] columnSizes = calculateColumnSizes(columns, topRecords, records.hasNext(), heading);

        int totalWidth = 1;
        for (int columnSize : columnSizes) {
            totalWidth += columnSize + 3;
        }

        StringBuilder builder = new StringBuilder(totalWidth);
        int lineWidth = totalWidth - 2;
        String dashes = "+" + String.valueOf(repeat('-', lineWidth)) + "+";

        if (heading != null && !heading.isBlank()) {
            output.printOut(dashes);
            output.printOut(
                    formatRow(builder, new int[] {lineWidth - 2}, new String[] {heading}, new boolean[] {false}));
            builder.setLength(0);
        }

        output.printOut(dashes);
        output.printOut(formatRow(builder, columnSizes, columns, new boolean[columnSizes.length]));
        output.printOut(dashes);

        int numberOfRows = 0;

        for (Record record : topRecords) {
            output.printOut(formatRecord(builder, columnSizes, record));
            numberOfRows++;
        }

        while (records.hasNext()) {
            output.printOut(formatRecord(builder, columnSizes, records.next()));
            numberOfRows++;
        }

        if (printFooter) output.printOut(String.format("%s%n", dashes));

        return numberOfRows;
    }

    /**
     * Calculate the size of the columns for table formatting
     *
     * @param columns              the column names
     * @param data                 (sample) data
     * @param moreDataAfterSamples if there is more data that should be written into the table after `data`
     * @return the column sizes
     */
    private int[] calculateColumnSizes(
            String[] columns, List<Record> data, boolean moreDataAfterSamples, String heading) {
        int[] columnSizes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            columnSizes[i] = columns[i].length();
        }
        for (Record record : data) {
            for (int i = 0; i < columns.length; i++) {
                int len = columnLengthForValue(record.get(i), moreDataAfterSamples);
                if (columnSizes[i] < len) {
                    columnSizes[i] = len;
                }
            }
        }
        if (heading != null) {
            final var totalSize = Arrays.stream(columnSizes).sum();
            if (heading.length() > totalSize) {
                columnSizes[0] = columnSizes[0] + (heading.length() - totalSize);
            }
        }
        return columnSizes;
    }

    /**
     * The length of a column, where Numbers are always getting enough space to fit the highest number possible.
     *
     * @param value                the value to calculate the length for
     * @param moreDataAfterSamples if there is more data that should be written into the table after `data`
     * @return the column size for this value.
     */
    private int columnLengthForValue(Value value, boolean moreDataAfterSamples) {
        if (value instanceof NumberValueAdapter && moreDataAfterSamples) {
            return 19; // The number of digits of Long.Max
        } else {
            return formatValue(value).length();
        }
    }

    private String formatRecord(StringBuilder sb, int[] columnSizes, Record record) {
        sb.setLength(0);
        return formatRow(sb, columnSizes, formatValues(record), new boolean[columnSizes.length]);
    }

    private String[] formatValues(Record record) {
        String[] row = new String[record.size()];
        for (int i = 0; i < row.length; i++) {
            row[i] = formatValue(record.get(i));
        }
        return row;
    }

    /**
     * Format one row of data.
     *
     * @param sb           the StringBuilder to use (will reset)
     * @param columnSizes  the size of all columns
     * @param row          the data
     * @param continuation for each column whether it holds the remainder of data that did not fit in the column
     * @return the String result
     */
    private String formatRow(StringBuilder sb, int[] columnSizes, String[] row, boolean[] continuation) {
        if (!continuation[0]) {
            sb.append("|");
        } else {
            sb.append("\\");
        }
        boolean remainder = false;
        for (int i = 0; i < row.length; i++) {
            sb.append(" ");
            int length = columnSizes[i];
            String txt = row[i];
            if (txt != null) {
                int offset = 0; // char offset in the string
                int codePointCount = 0; // UTF code point counter (one code point can be multiple chars)

                /*
                 * Copy content of cell to output, UTF codepoint by codepoint,
                 * until cell width is reached, string ends or line breaks.
                 *
                 * The reason we copy by codepoint (and not by char) is to
                 * avoid breaking the string in the middle of a code point
                 * which can lead to invalid characters in output when
                 * wrapping.
                 */
                while (codePointCount < length && offset < txt.length()) {
                    final int codepoint = txt.codePointAt(offset);

                    // Stop at line breaks. Note that we skip the line break later in nextLineStart.
                    if (codepoint == '\n' || codepoint == '\r') {
                        break;
                    }

                    sb.appendCodePoint(codepoint);
                    offset = txt.offsetByCodePoints(offset, 1); // Move offset to next code point
                    ++codePointCount;
                }

                if (offset < txt.length())
                // Content did not fit column
                {
                    if (wrap) {
                        row[i] = txt.substring(nextLineStart(txt, offset));
                        continuation[i] = true;
                        remainder = true;
                    } else if (codePointCount < length) {
                        sb.append("…");
                        ++codePointCount;
                    } else {
                        int lastCodePoint = sb.codePointBefore(sb.length());
                        int lastLength = Character.charCount(lastCodePoint);
                        sb.replace(sb.length() - lastLength, sb.length(), "…");
                    }
                } else
                // Content did fit column
                {
                    row[i] = null;
                }

                // Insert padding
                if (codePointCount < length) {
                    sb.append(repeat(' ', length - codePointCount));
                }
            } else {
                sb.append(repeat(' ', length));
            }
            if (i == row.length - 1 || !continuation[i + 1]) {
                sb.append(" |");
            } else {
                sb.append(" \\");
            }
        }
        if (wrap && remainder) {
            sb.append(OutputFormatter.NEWLINE);
            formatRow(sb, columnSizes, row, continuation);
        }
        return sb.toString();
    }

    private static int nextLineStart(String txt, int start) {
        if (start < txt.length()) {
            final char firstChar = txt.charAt(start);
            if (firstChar == '\n') {
                return start + 1;
            } else if (firstChar == '\r') {
                int next = start + 1;
                if (next < txt.length() && txt.charAt(next) == '\n') {
                    return next + 1;
                } else {
                    return start + 1;
                }
            } else {
                return start;
            }
        }

        return txt.length();
    }

    @Override
    public String formatFooter(BoltResult result, int numberOfRows) {
        ResultSummary summary = result.getSummary();
        return String.format(
                "%d row%s" + OutputFormatter.NEWLINE + "ready to start consuming query after %d ms, "
                        + "results consumed after another %d ms",
                numberOfRows,
                numberOfRows != 1 ? "s" : "",
                summary.resultAvailableAfter(MILLISECONDS),
                summary.resultConsumedAfter(MILLISECONDS));
    }

    @Override
    public String formatNotifications(List<Notification> notifications) {
        if (notifications.isEmpty()) {
            return "";
        } else {
            final var messages = new HashSet<String>();
            final var builder = new StringBuilder();
            for (final var notification : notifications) {
                final var message = formatNotification(notification);
                if (messages.add(message)) {
                    builder.append('\n').append(message).append('\n');
                }
            }
            return builder.toString();
        }
    }

    private String formatNotification(Notification notification) {
        final var severity = severityString(notification);
        return String.format("%s: %s (%s)", severity, notification.description(), notification.code());
    }

    private static String severityString(Notification notification) {
        final var rawSeverity =
                notification.rawSeverityLevel().orElse("information").toLowerCase(Locale.ROOT);
        return switch (rawSeverity) {
            case "information" -> "info";
            case "warning" -> "warn";
            default -> rawSeverity;
        };
    }

    @Override
    public String formatInfo(ResultSummary summary) {
        Map<String, Value> info = OutputFormatter.info(summary);
        if (info.isEmpty()) {
            return "";
        }
        String[] columns = info.keySet().toArray(new String[0]);
        StringBuilder sb = new StringBuilder();
        Record record = new InternalRecord(asList(columns), info.values().toArray(new Value[0]));
        formatResultAndCountRows(columns, Collections.singletonList(record).iterator(), line -> sb.append(line)
                .append(OutputFormatter.NEWLINE));
        return sb.toString();
    }

    @Override
    public String formatPlan(ResultSummary summary) {
        if (summary == null || !summary.hasPlan()) {
            return "";
        }

        Plan plan = summary.plan();
        if (plan.arguments().containsKey(STRING_REPRESENTATION)) {
            return plan.arguments().get(STRING_REPRESENTATION).asString();
        } else {
            return new TablePlanFormatter().formatPlan(plan);
        }
    }

    @Override
    public Set<Capabilities> capabilities() {
        return EnumSet.allOf(Capabilities.class);
    }
}
