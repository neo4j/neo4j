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
package org.neo4j.capabilities;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.contains;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Used for hierarchical naming of capabilities, each namespace component separated by '.'.
 */
public final class Name {
    private static final Predicate<String> NAME_VALIDATOR =
            Pattern.compile("^\\w+(\\.\\w+)*$").asMatchPredicate();
    private static final String SINGLE_ENTRY_PATTERN = "\\w{0,}";
    private static final String MULTIPLE_ENTRY_PATTERN = "(\\w{0,}(\\.)?){0,}";
    private static final String SEPARATOR = ".";

    private final String fullName;

    private Name(String fullName) {
        this.fullName = validateName(Objects.requireNonNull(fullName));
    }

    /**
     * Returns the full name.
     *
     * @return full name.
     */
    public String fullName() {
        return fullName;
    }

    /**
     * Creates a child name from this name instance.
     *
     * @param name child name.
     * @return new name instance.
     * @throws IllegalArgumentException if name is empty or contains '.'.
     */
    public Name child(String name) {
        if (isBlank(name) || contains(name, SEPARATOR)) {
            throw new IllegalArgumentException(String.format("'%s' is not a valid name", name));
        }

        if (isBlank(this.fullName)) {
            return new Name(name);
        }

        return new Name(this.fullName + SEPARATOR + Objects.requireNonNull(name));
    }

    /**
     * Checks if this name instance is in the given namespace.
     *
     * @param namespace namespace to check
     * @return true if this name lies in the given namespace, false otherwise.
     */
    public boolean isIn(String namespace) {
        var validated = validateName(namespace);

        if (isBlank(validated) || validated.equals(fullName)) {
            return true;
        }

        return fullName.startsWith(validated + SEPARATOR);
    }

    /**
     * Checks if this name instance is in the given name's scope.
     *
     * @param name name to check
     * @return true if this name lies in the given name's scope, false otherwise.
     */
    public boolean isIn(Name name) {
        return isIn(name.fullName);
    }

    public boolean matches(String pattern) {
        var transformed = pattern.replace(".", "\\.") // escape dots
                .replace("**", MULTIPLE_ENTRY_PATTERN) // convert ** into multiple entry pattern
                .replace("*", SINGLE_ENTRY_PATTERN); // convert * into single entry pattern

        return Pattern.matches(transformed, fullName);
    }

    public boolean matches(List<String> patterns) {
        return patterns.stream().anyMatch(this::matches);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Name other = (Name) o;
        return this.fullName.equals(other.fullName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fullName);
    }

    @Override
    public String toString() {
        return fullName;
    }

    private static String validateName(String fullName) {
        if (isEmpty(fullName)) {
            return fullName;
        }

        var valid = NAME_VALIDATOR.test(fullName);
        if (!valid) {
            throw new IllegalArgumentException(format("'%s' is not a valid name.", fullName));
        }
        return fullName;
    }

    /**
     * Creates a name from the given string.
     *
     * @param name name.
     * @return a new name instance.
     */
    public static Name of(String name) {
        return new Name(name);
    }
}
