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
package org.neo4j.genai.util;

import java.util.function.Predicate;
import java.util.regex.Pattern;

public final class UrlPath {
    private static final Predicate<String> URI_SAFE =
            Pattern.compile("^[a-zA-Z0-9-_@.]+$").asMatchPredicate();

    private UrlPath() {}

    /**
     * Validates a value used as a URL path part and returns it if safe.
     *
     * @param pathPart the value to validate
     * @param name the semantic name of the value (used in exception message)
     * @return the original value when valid
     * @throws IllegalArgumentException when the value contains unsafe characters
     */
    public static String pathSafe(String pathPart, String name) {
        if (URI_SAFE.test(pathPart)) {
            return pathPart;
        }
        throw new IllegalArgumentException("Not a valid '%s': %s".formatted(name, pathPart));
    }

    public static String pathSafe(String pathPart) {
        return pathSafe(pathPart, "resource");
    }
}
