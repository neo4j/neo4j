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
package org.neo4j.configuration;

import static java.lang.String.format;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;

import org.neo4j.io.ByteUnit;

public class DenseRootCacheSizeSettingValueParser implements SettingValueParser<Long> {
    private static final long UNKNOWN_MAX_HEAP_DEFAULT = mebiBytes(10);
    private static final long MIN = (int) kibiBytes(10);
    private static final long MAX = Math.max(MIN, percentageOfHeap(10, Long.MAX_VALUE));

    @Override
    public Long parse(String value) {
        value = value.trim();
        if (value.equalsIgnoreCase("auto")) {
            return clampedPercentageOfHeap(1);
        } else if (value.endsWith("%")) {
            return clampedPercentageOfHeap(Double.parseDouble(value.substring(0, value.length() - 1)));
        }
        return Math.clamp(BYTES.parse(value), MIN, MAX);
    }

    @Override
    public String getDescription() {
        return format(
                "a byte size (valid multipliers are %s) or %% of heap (e.g. 2%% or 0.1%%), or 'auto'",
                ByteUnit.VALID_MULTIPLIERS);
    }

    @Override
    public Class<Long> getType() {
        return Long.class;
    }

    @Override
    public String valueToString(Long value) {
        return BYTES.valueToString(value);
    }

    private static long percentageOfHeap(double percentage, long valueOnUnknownMaxHeap) {
        long maxMemory = Runtime.getRuntime().maxMemory();
        if (maxMemory == Long.MAX_VALUE) {
            return valueOnUnknownMaxHeap;
        }
        return (long) (maxMemory * percentage / 100D);
    }

    static long clampedPercentageOfHeap(double percentage) {
        return Math.clamp(percentageOfHeap(percentage, UNKNOWN_MAX_HEAP_DEFAULT), MIN, MAX);
    }
}
