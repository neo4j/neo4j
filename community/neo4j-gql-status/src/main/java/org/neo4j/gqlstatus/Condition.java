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
package org.neo4j.gqlstatus;

// The first four entries of the enum must be in this specific order for GQL-status objects to be sorted in severity
// order
public enum Condition {
    NO_DATA,
    WARNING,
    SUCCESSFUL_COMPLETION,
    INFORMATIONAL,
    CONNECTION_EXCEPTION,
    DATA_EXCEPTION,
    SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
    GENERAL_PROCESSING_EXCEPTION,
    SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
    PROCEDURE_EXCEPTION,
    DEPENDENT_OBJECT_ERROR,
    GRAPH_TYPE_VIOLATION,
    INVALID_TRANSACTION_STATE,
    INVALID_TRANSACTION_TERMINATION,
    TRANSACTION_ROLLBACK;

    public static String createStandardDescription(Condition condition, String subcondition) {
        return switch (condition) {
            case WARNING -> "warn: " + subcondition;
            case INFORMATIONAL -> "info: " + subcondition;
            case SUCCESSFUL_COMPLETION -> {
                String successBaseMessage = "note: successful completion";
                if (subcondition.isEmpty()) {
                    yield successBaseMessage;
                } else {
                    yield successBaseMessage + " - " + subcondition;
                }
            }
            case NO_DATA -> "note: no data";
            default -> {
                String exceptionBaseMessage = "error: " + condition.createConditionString();
                if (subcondition.isEmpty()) {
                    yield exceptionBaseMessage;
                } else {
                    yield exceptionBaseMessage + " - " + subcondition;
                }
            }
        };
    }

    private String createConditionString() {
        return this.name().toLowerCase().replace('_', ' ');
    }
}
