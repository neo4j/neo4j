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
package org.neo4j.internal.helpers;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class Format {
    /**
     * Default time zone is UTC (+00:00) so that comparing timestamped logs from different
     * sources is an easier task.
     */
    static final ZoneId DEFAULT_TIME_ZONE = ZoneOffset.UTC;

    private static final String[] COUNT_SIZES = {"", "k", "M", "G", "T"};

    static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSZ";
    static final String TIME_FORMAT = "HH:mm:ss.SSS";

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern(DATE_FORMAT).withZone(DEFAULT_TIME_ZONE);
    private static final DateTimeFormatter LOCAL_DATE_FORMATTER =
            DateTimeFormatter.ofPattern(DATE_FORMAT).withZone(ZoneId.systemDefault());
    private static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern(TIME_FORMAT).withZone(DEFAULT_TIME_ZONE);
    private static final HexFormat HEX_FORMAT = HexFormat.of().withUpperCase();

    public static String localDate() {
        return LOCAL_DATE_FORMATTER.format(Instant.now());
    }

    public static String date() {
        return date(Instant.now());
    }

    public static String date(long millis) {
        return date(Instant.ofEpochMilli(millis));
    }

    public static String date(Instant instant) {
        return DATE_FORMATTER.format(instant);
    }

    public static String hexString(byte[] bytes) {
        return HEX_FORMAT.formatHex(bytes);
    }

    public static byte[] parseHexString(String hexString) {
        return HEX_FORMAT.parseHex(hexString);
    }

    public static String time() {
        return time(Instant.now());
    }

    public static String time(long millis) {
        return time(Instant.ofEpochMilli(millis));
    }

    public static String time(Instant instant) {
        return TIME_FORMATTER.format(instant);
    }

    public static String count(long count) {
        return suffixCount(count, COUNT_SIZES, 1_000);
    }

    private static String suffixCount(long value, String[] sizes, int stride) {
        double size = value;
        for (String suffix : sizes) {
            if (size < stride) {
                return String.format(Locale.ROOT, "%.2f %s", size, suffix);
            }
            size /= stride;
        }
        return String.format(Locale.ROOT, "%.2f TB", size);
    }

    public static String duration(long durationMillis) {
        return duration(durationMillis, TimeUnit.DAYS, TimeUnit.MILLISECONDS);
    }

    public static String duration(long durationMillis, TimeUnit highestGranularity, TimeUnit lowestGranularity) {
        return duration(durationMillis, highestGranularity, lowestGranularity, Format::shortName);
    }

    public static String duration(
            long durationMillis,
            TimeUnit highestGranularity,
            TimeUnit lowestGranularity,
            Function<TimeUnit, String> unitFormat) {
        StringBuilder builder = new StringBuilder();

        TimeUnit[] units = TimeUnit.values();
        ArrayUtil.reverse(units);
        boolean use = false;
        for (TimeUnit unit : units) {
            if (unit == highestGranularity) {
                use = true;
            }

            if (use) {
                durationMillis = extractFromDuration(durationMillis, unit, unitFormat, builder);
                if (unit == lowestGranularity) {
                    break;
                }
            }
        }

        if (builder.isEmpty()) {
            // The value is too low to extract any meaningful numbers with the given unit brackets.
            // So we append a zero of the lowest unit.
            builder.append('0').append(unitFormat.apply(lowestGranularity));
        }

        return builder.toString();
    }

    private static String shortName(TimeUnit unit) {
        return switch (unit) {
            case NANOSECONDS -> "ns";
            case MICROSECONDS -> "μs";
            case MILLISECONDS -> "ms";
            default -> unit.name().substring(0, 1).toLowerCase(Locale.ROOT);
        };
    }

    private static long extractFromDuration(
            long durationMillis, TimeUnit unit, Function<TimeUnit, String> unitFormat, StringBuilder target) {
        int count = 0;
        long millisPerUnit = unit.toMillis(1);
        while (durationMillis >= millisPerUnit) {
            count++;
            durationMillis -= millisPerUnit;
        }
        if (count > 0) {
            target.append(!target.isEmpty() ? " " : "").append(count).append(unitFormat.apply(unit));
        }
        return durationMillis;
    }

    /**
     * Converts a number into a string with the number grouped by three digits, like 10,000 or 1,234,567 for a more readable display of a number.
     * @param number the number to convert to a string.
     * @param groupSeparatorChar the character to separate the groups with.
     * @return the number as a string, grouped three and three.
     */
    public static String numberToStringWithGroups(long number, char groupSeparatorChar) {
        DecimalFormatSymbols symbols = new DecimalFormatSymbols();
        symbols.setGroupingSeparator(groupSeparatorChar);
        DecimalFormat df = new DecimalFormat();
        df.setDecimalFormatSymbols(symbols);
        df.setGroupingSize(3);
        df.setMaximumFractionDigits(0);
        return df.format(number);
    }

    private Format() {
        // No instances
    }
}
