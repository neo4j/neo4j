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

/**
 * GQLSTATUS is a code that identifies what notable or problematic condition arose during a query execution.
 * This includes successful completion, warnings, no data, information and exceptions.
 * GQLSTATUS is 5 character long and alphanumeric,
 * where the two first characters forms the class code and the three last the subclass code.
 */
public record GqlStatus(String gqlStatusString) {
    public GqlStatus(String gqlStatusString) {
        this.gqlStatusString = validate(gqlStatusString);
    }

    private String validate(String input) {
        if (input.isEmpty()) {
            throw new IllegalArgumentException("GQLSTATUS must be 5 characters and alphanumeric, got an empty string.");
        }
        if (!input.matches("[A-Za-z0-9]{5}")) {
            throw new IllegalArgumentException(
                    String.format("GQLSTATUS must be 5 characters and alphanumeric, got: %s.", input));
        }
        return input.toUpperCase();
    }

    @Override
    public boolean equals(Object that) {
        if (that instanceof GqlStatus otherGqlStatus) {
            return this.gqlStatusString.equals(otherGqlStatus.gqlStatusString);
        }
        return false;
    }
}
