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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.neo4j.kernel.database.NormalizedDatabaseName;

public class DatabaseNamePattern {
    private final Optional<Pattern> regexPattern;
    private final String databaseName;
    private final String normalizedDatabaseName;

    public DatabaseNamePattern(String name) {
        validateDatabaseNamePattern(name);
        this.regexPattern =
                ConfigPatternBuilder.optionalPatternFromConfigString(name.toLowerCase(), Pattern.CASE_INSENSITIVE);
        this.databaseName = name;
        this.normalizedDatabaseName = new NormalizedDatabaseName(name).name();
    }

    public boolean matches(String value) {
        return regexPattern.map(p -> p.matcher(value).matches()).orElse(normalizedDatabaseName.equals(value));
    }

    public boolean containsPattern() {
        return regexPattern.isPresent();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getNormalizedDatabaseName() {
        return normalizedDatabaseName;
    }

    @Override
    public String toString() {
        if (containsPattern()) {
            return "Database name pattern=" + databaseName;
        } else {
            return "Database name=" + databaseName;
        }
    }

    public static Optional<Set<String>> exactNames(List<DatabaseNamePattern> patterns) {
        Set<String> exact = new HashSet<>();
        for (var pattern : patterns) {
            if (pattern.containsPattern()) {
                return Optional.empty();
            }
            exact.add(pattern.getDatabaseName());
        }
        return Optional.of(exact);
    }

    public static Predicate<String> matchAny(List<DatabaseNamePattern> patterns) {
        return (string) -> patterns.stream().anyMatch(pattern -> pattern.matches(string));
    }

    public static List<DatabaseNamePattern> patternsOf(String... patterns) {
        return Arrays.stream(patterns).map(DatabaseNamePattern::new).toList();
    }
}
