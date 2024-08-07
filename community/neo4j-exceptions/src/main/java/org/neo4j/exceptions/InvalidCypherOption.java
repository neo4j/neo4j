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
package org.neo4j.exceptions;

import static java.lang.String.format;

public class InvalidCypherOption extends InvalidArgumentException {

    public InvalidCypherOption(String message) {
        super(message);
    }

    public static InvalidCypherOption invalidCombination(
            String optionName1, String option1, String optionName2, String option2) {
        return new InvalidCypherOption(
                format("Cannot combine %s '%s' with %s '%s'", optionName1, option1, optionName2, option2));
    }

    public static InvalidCypherOption parallelRuntimeIsDisabled() {
        return new InvalidCypherOption(
                "Parallel runtime has been disabled, please enable it or upgrade to a bigger Aura instance.");
    }

    public static InvalidCypherOption invalidOption(String input, String name, String... validOptions) {
        return new InvalidCypherOption(format(
                "%s is not a valid option for %s. Valid options are: %s",
                input, name, String.join(", ", validOptions)));
    }

    public static InvalidCypherOption conflictingOptionForName(String name) {
        return new InvalidCypherOption("Can't specify multiple conflicting values for " + name);
    }

    public static InvalidCypherOption unsupportedOptions(String... keys) {
        return new InvalidCypherOption(format("Unsupported options: %s", String.join(", ", keys)));
    }

    public static InvalidCypherOption irEagerAnalyzerUnsupported(String operation) {
        return new InvalidCypherOption(format(
                "The Cypher option `eagerAnalyzer=ir` is not supported while %s. Use `eagerAnalyzer=lp` instead.",
                operation));
    }

    // NOTE: this is an internal error and should probably not have any GQL code
    public static InvalidCypherOption sourceGenerationDisabled() {
        return new InvalidCypherOption("In order to use source generation you need to enable "
                + "`internal.cypher.pipelined.allow_source_generation`");
    }
}
