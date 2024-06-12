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
package org.neo4j.server.security;

import java.util.Map;
import org.neo4j.exceptions.InvalidArgumentException;

public abstract class SecureHasherConfigurations {
    static final String CURRENT_VERSION = "1";

    // add new configurations here
    static final Map<String, SecureHasherConfiguration> configurations = Map.of(
            // <version-number>, Pair.of(<algorithm>, <algorithm-iterations>)
            "0", new SecureHasherConfiguration("SHA-256", 1),
            "1", new SecureHasherConfiguration("SHA-256", 1024));

    public static String getVersionForConfiguration(String algorithm, int iterations) {
        var entry = configurations.entrySet().stream()
                .filter(configuration -> configuration.getValue().algorithm.equals(algorithm)
                        && configuration.getValue().iterations == iterations)
                .findFirst();

        if (entry.isPresent()) {
            return entry.get().getKey();
        } else {
            throw new InvalidArgumentException(String.format(
                    "There exists no version with this combination of algorithm:'%s' and iterations:'%d'",
                    algorithm, iterations));
        }
    }
}
