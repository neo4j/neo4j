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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.value.NumberValueAdapter;
import org.neo4j.driver.summary.GqlNotification;
import org.neo4j.driver.summary.GqlStatusObject;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.shell.state.BoltResult;

public class TableOutputFormatter implements OutputFormatter {

    public static final String INCOMING_VARIABLES = "incoming variables";
    public static final String STRING_REPRESENTATION = "string-representation";
    private static final String INFO_SEVERITY_LEVEL = "info";
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

        boolean[] rawColumns = new boolean[columns.length];
        List<String> rawKeys = result.getRawKeys();
        for (int i = 0; i < columns.length; i++) {
            rawColumns[i] = rawKeys.contains(columns[i]);
        }

        Iterator<Record> records = result.iterate();
        return formatResultAndCountRows(columns, rawColumns, records, output);
    }

    /**
     * Prints bolt result with a heading. Not optimised for large result sets.
     */
    public void formatWithHeading(BoltResult result, LinePrinter output, String heading) {
        final String[] columns = result.getKeys().toArray(new String[0]);
        boolean[] rawColumns = new boolean[columns.length];
        List<String> rawKeys = result.getRawKeys();
        for (int i = 0; i < columns.length; i++) {
            rawColumns[i] = rawKeys.contains(columns[i]);
        }
        printTableAndCountRows(columns, rawColumns, emptyIterator(), output, result.getRecords(), true, heading);
    }

    private static void take(Iterator<Record> records, ArrayList<Record> topRecords, int count) {
        while (records.hasNext() && topRecords.size() < count) {
            topRecords.add(records.next());
        }
    }

    private int formatResultAndCountRows(
            String[] columns, boolean[] rawColumns, Iterator<Record> records, LinePrinter output) {

        ArrayList<Record> topRecords = new ArrayList<>(numSampleRows);
        try {
            take(records, topRecords, numSampleRows);
        } catch (RuntimeException e) {
            printTableAndCountRows(columns, rawColumns, records, output, topRecords, false, null);
            throw e;
        }
        return printTableAndCountRows(columns, rawColumns, records, output, topRecords, true, null);
    }

    private int printTableAndCountRows(
            String[] columns,
            boolean[] rawColumns,
            Iterator<Record> records,
            LinePrinter output,
            List<Record> topRecords,
            boolean printFooter,
            String heading) {
        int[] columnSizes = calculateColumnSizes(columns, rawColumns, topRecords, records.hasNext(), heading);

        int totalWidth = 1;
        for (int columnSize : columnSizes) {
            totalWidth += columnSize + 3;
        }

        StringBuilder builder = new StringBuilder(totalWidth);
        int lineWidth = totalWidth - 2;
        String dashes = "+" + "-".repeat(lineWidth) + "+";

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
            output.printOut(formatRecord(builder, columnSizes, rawColumns, record));
            numberOfRows++;
        }

        while (records.hasNext()) {
            output.printOut(formatRecord(builder, columnSizes, rawColumns, records.next()));
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
            String[] columns, boolean[] rawColumns, List<Record> data, boolean moreDataAfterSamples, String heading) {
        int[] columnSizes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            columnSizes[i] = displayWidth(columns[i]);
        }
        for (Record record : data) {
            for (int i = 0; i < columns.length; i++) {
                final var currentColumnSize = columnSizes[i];
                final int len =
                        columnLengthForValue(record.get(i), rawColumns[i], moreDataAfterSamples, currentColumnSize);
                if (currentColumnSize < len) {
                    columnSizes[i] = len;
                }
            }
        }
        if (heading != null) {
            final var totalSize = Arrays.stream(columnSizes).sum();
            final var headingWidth = displayWidth(heading);
            if (headingWidth > totalSize) {
                columnSizes[0] = columnSizes[0] + (headingWidth - totalSize);
            }
        }
        return columnSizes;
    }

    /**
     * The length of a column, where Numbers are always getting enough space to fit the highest number possible.
     *
     * @param value                the value to calculate the length for
     * @param raw                  whether to format the value rawly (without quotations)
     * @param moreDataAfterSamples if there is more data that should be written into the table after `data`
     * @param currentColSize       current size of this column
     * @return the column size for this value.
     */
    private int columnLengthForValue(Value value, boolean raw, boolean moreDataAfterSamples, int currentColSize) {
        if (value instanceof NumberValueAdapter && moreDataAfterSamples) {
            return 19; // The number of digits of Long.Max
        } else {
            final var formatted = raw ? formatValueRaw(value) : formatValue(value);
            final boolean hasLineBreak = StringUtils.containsAny(formatted, '\n', '\r');
            if (!hasLineBreak) {
                return displayWidth(formatted);
            } else if (wrap) {
                // With wrapping we need the max line length.
                // Not optimised, but not expected to come here that often.
                return formatted
                        .lines()
                        .mapToInt(TableOutputFormatter::displayWidth)
                        .max()
                        .orElse(0);
            } else {
                // With no wrapping we only display the first line.
                final int lineBreakIndex = StringUtils.indexOfAny(formatted, '\n', '\r');
                return displayWidth(formatted, 0, lineBreakIndex);
            }
        }
    }

    private String formatRecord(StringBuilder sb, int[] columnSizes, boolean[] rawColumns, Record record) {
        sb.setLength(0);
        return formatRow(sb, columnSizes, formatValues(record, rawColumns), new boolean[columnSizes.length]);
    }

    private String[] formatValues(Record record, boolean[] rawColumns) {
        String[] row = new String[record.size()];
        for (int i = 0; i < row.length; i++) {
            row[i] = rawColumns[i] ? formatValueRaw(record.get(i)) : formatValue(record.get(i));
        }
        return row;
    }

    private String formatValueRaw(Value value) {
        if (value == null || value.isNull()) {
            return "";
        }
        if (value.type().name().equals("STRING")) {
            return value.asString();
        }
        return formatValue(value);
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
            final int length = columnSizes[i];
            final var txt = row[i];
            if (txt != null) {
                final var txtLength = txt.length();
                int offset = 0; // char offset in the string
                int displayWidthCount = 0; // Terminal column width counter

                /*
                 * Copy content of cell to output, UTF codepoint by codepoint,
                 * until cell width is reached, string ends or line breaks.
                 *
                 * The reason we copy by codepoint (and not by char) is to
                 * avoid breaking the string in the middle of a code point
                 * which can lead to invalid characters in output when
                 * wrapping.
                 */
                while (displayWidthCount < length && offset < txtLength) {
                    final int codepoint = txt.codePointAt(offset);

                    // Stop at line breaks. Note that we skip the line break later in nextLineStart.
                    if (codepoint == '\n' || codepoint == '\r') {
                        break;
                    }

                    final int width = codePointDisplayWidth(codepoint);
                    if (width > 0 && displayWidthCount + width > length) {
                        break;
                    }

                    sb.appendCodePoint(codepoint);
                    offset = txt.offsetByCodePoints(offset, 1); // Move offset to next code point
                    displayWidthCount += width;
                }

                if (offset < txtLength)
                // Content did not fit column
                {
                    if (wrap) {
                        row[i] = txt.substring(nextLineStart(txt, offset));
                        continuation[i] = true;
                        remainder = true;
                    } else if (displayWidthCount < length) {
                        sb.append("…");
                        displayWidthCount += codePointDisplayWidth('…');
                    } else {
                        int lastCodePoint = sb.codePointBefore(sb.length());
                        int lastLength = Character.charCount(lastCodePoint);
                        int lastWidth = codePointDisplayWidth(lastCodePoint);
                        sb.replace(sb.length() - lastLength, sb.length(), "…");
                        displayWidthCount = displayWidthCount - lastWidth + codePointDisplayWidth('…');
                    }
                } else
                // Content did fit column
                {
                    row[i] = null;
                }

                // Insert padding
                if (displayWidthCount < length) {
                    sb.repeat(' ', length - displayWidthCount);
                }
            } else {
                sb.repeat(' ', length);
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

    private static int displayWidth(String text) {
        return displayWidth(text, 0, text.length());
    }

    private static int displayWidth(String text, int start, int end) {
        int width = 0;
        int offset = start;
        while (offset < end) {
            int codepoint = text.codePointAt(offset);
            if (codepoint == '\n' || codepoint == '\r') {
                break;
            }
            width += codePointDisplayWidth(codepoint);
            offset = text.offsetByCodePoints(offset, 1);
        }
        return width;
    }

    private static int codePointDisplayWidth(int codepoint) {
        if (codepoint == 0) {
            return 0;
        }
        if (codepoint < 32 || (codepoint >= 0x7F && codepoint < 0xA0)) {
            return 0;
        }
        int type = Character.getType(codepoint);
        if (type == Character.NON_SPACING_MARK
                || type == Character.ENCLOSING_MARK
                || type == Character.COMBINING_SPACING_MARK) {
            return 0;
        }
        return isWide(codepoint) ? 2 : 1;
    }

    // Approximate terminal wcwidth rules for wide characters.
    private static boolean isWide(int codepoint) {
        return codepoint >= 0x1100
                && (codepoint <= 0x115F
                        || codepoint == 0x2329
                        || codepoint == 0x232A
                        || (codepoint >= 0x2E80 && codepoint <= 0xA4CF && codepoint != 0x303F)
                        || (codepoint >= 0xAC00 && codepoint <= 0xD7A3)
                        || (codepoint >= 0xF900 && codepoint <= 0xFAFF)
                        || (codepoint >= 0xFE10 && codepoint <= 0xFE19)
                        || (codepoint >= 0xFE30 && codepoint <= 0xFE6F)
                        || (codepoint >= 0xFF00 && codepoint <= 0xFF60)
                        || (codepoint >= 0xFFE0 && codepoint <= 0xFFE6)
                        || (codepoint >= 0x1F300 && codepoint <= 0x1F64F)
                        || (codepoint >= 0x1F900 && codepoint <= 0x1F9FF)
                        || (codepoint >= 0x20000 && codepoint <= 0x2FFFD)
                        || (codepoint >= 0x30000 && codepoint <= 0x3FFFD));
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
    public String formatNotifications(ResultSummary summary) {
        Set<String> messages;
        // These GQLSTATUS codes are added by the driver when the server is too old to support GQLSTATUS
        List<String> incompatibilityGqlstatuses = List.of("01N42", "02N42", "03N42");

        var actualGqlStatusObjects = summary.gqlStatusObjects().stream()
                .filter(gso -> !incompatibilityGqlstatuses.contains(gso.gqlStatus()))
                .toList();

        if (actualGqlStatusObjects.size() > 1) {
            // GQL Status Objects are available, use them
            messages = summary.gqlStatusObjects().stream()
                    .map(gqlStatusObject -> switch (gqlStatusObject) {
                        case GqlNotification gqlNotification ->
                            String.format("%s (%s)", gqlNotification.statusDescription(), gqlNotification.gqlStatus());
                        case GqlStatusObject ignored -> null;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        } else {
            // use legacy notifications
            messages = summary.notifications().stream()
                    .map(notification -> {
                        var severity = notification
                                .rawSeverityLevel()
                                .map(rawSeverity -> rawSeverity.toLowerCase(Locale.ROOT))
                                .map(this::severity)
                                .orElse(INFO_SEVERITY_LEVEL);
                        return String.format("%s: %s (%s)", severity, notification.description(), notification.code());
                    })
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
        var builder = new StringBuilder();
        messages.forEach(message -> builder.append('\n').append(message).append('\n'));
        return builder.toString();
    }

    private String severity(String rawSeverity) {
        return switch (rawSeverity) {
            case "information" -> INFO_SEVERITY_LEVEL;
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
        Record record =
                new InternalRecord(asList(columns), info.values().stream().toList());
        formatResultAndCountRows(
                columns,
                new boolean[columns.length],
                Collections.singletonList(record).iterator(),
                line -> sb.append(line).append(OutputFormatter.NEWLINE));
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
        } else if (plan.arguments().containsKey(INCOMING_VARIABLES)) {
            return new TableScopePlanFormatter().formatPlan(plan);
        } else {
            return new TablePlanFormatter().formatPlan(plan);
        }
    }

    @Override
    public Set<Capabilities> capabilities() {
        return EnumSet.allOf(Capabilities.class);
    }
}
