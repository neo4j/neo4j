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
package org.neo4j.configuration.helpers;

import static org.neo4j.configuration.helpers.DatabaseNameValidator.validateDatabaseNamePattern;

import java.util.Optional;
import java.util.regex.Pattern;

public class DatabaseNamePattern {
    private final Optional<Pattern> regexPattern;
    private final String databaseName;

    public DatabaseNamePattern(String name) {
        validateDatabaseNamePattern(name);
        this.regexPattern =
                ConfigPatternBuilder.optionalPatternFromConfigString(name.toLowerCase(), Pattern.CASE_INSENSITIVE);
        this.databaseName = name;
    }

    public boolean matches(String value) {
        return regexPattern.map(p -> p.matcher(value).matches()).orElse(databaseName.equals(value));
    }

    public boolean containsPattern() {
        return regexPattern.isPresent();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String toString() {
        if (containsPattern()) {
            return "Database name pattern=" + databaseName;
        } else {
            return "Database name=" + databaseName;
        }
    }
}
