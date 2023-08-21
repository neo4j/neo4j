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
package org.neo4j.kernel.impl.transaction.log.pruning;

import static org.neo4j.configuration.SettingValueParsers.parseLongWithUnit;

public class ThresholdConfigParser {
    public record ThresholdConfigValue(String type, long value, long additionalRestriction) {
        private static final long NO_ADDITIONAL_RESTRICTION = -1;
        static final ThresholdConfigValue NO_PRUNING = new ThresholdConfigValue("false", -1, NO_ADDITIONAL_RESTRICTION);
        static final ThresholdConfigValue KEEP_LAST_FILE =
                new ThresholdConfigValue("entries", 1, NO_ADDITIONAL_RESTRICTION);

        ThresholdConfigValue(String type, long value) {
            this(type, value, NO_ADDITIONAL_RESTRICTION);
        }

        boolean hasAdditionalRestriction() {
            return additionalRestriction != NO_ADDITIONAL_RESTRICTION;
        }
    }

    private ThresholdConfigParser() {}

    public static ThresholdConfigValue parse(String configValue) {
        String[] tokens = configValue.split(" ");
        if (tokens.length == 0) {
            throw new IllegalArgumentException("Invalid log pruning configuration value '" + configValue + "'");
        }

        final String boolOrNumber = tokens[0];

        if (tokens.length == 1) {
            return switch (boolOrNumber) {
                case "keep_all", "true" -> ThresholdConfigValue.NO_PRUNING;
                case "keep_none", "false" -> ThresholdConfigValue.KEEP_LAST_FILE;
                default -> throw new IllegalArgumentException("Invalid log pruning configuration value '" + configValue
                        + "'. The form is 'true', 'false' or '<number><unit> <type>'. For example, '100k txs' "
                        + "will keep the 100 000 latest transactions.");
            };
        } else {
            long thresholdValue = parseLongWithUnit(boolOrNumber);
            String thresholdType = tokens[1];
            long maxSizeRestriction =
                    tokens.length > 2 ? parseLongWithUnit(tokens[2]) : ThresholdConfigValue.NO_ADDITIONAL_RESTRICTION;
            return new ThresholdConfigValue(thresholdType, thresholdValue, maxSizeRestriction);
        }
    }
}
