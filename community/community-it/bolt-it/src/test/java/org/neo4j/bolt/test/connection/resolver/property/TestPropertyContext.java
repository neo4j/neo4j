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
package org.neo4j.bolt.test.connection.resolver.property;

import java.nio.file.Path;
import java.util.Optional;

/**
 * Provides access to properties generated during the automatic startup of Bolt integration tests
 * to tests.
 * <p />
 * Instances of this interface may be injected as test parameters where needed.
 */
public interface TestPropertyContext {

    /**
     * Identifies the directory in which metrics will be stored when automatically configured.
     */
    Key<Path> METRICS_FOLDER = Key.of("metricsFolder", Path.class);

    <T> Optional<T> get(Key<T> key);

    default <T> T require(Key<T> key) {
        return this.get(key)
                .orElseThrow(
                        () -> new IllegalStateException(
                                "Required test property " + key.name() + " of type " + key.type()
                                        + " has not been initialized - Please verify your test configuration to ensure the appropriate initializers are present"));
    }

    final class Key<T> {
        private final String name;
        private final Class<T> type;

        private Key(String name, Class<T> type) {
            this.name = name;
            this.type = type;
        }

        public static <T> Key<T> of(String name, Class<T> type) {
            return new Key<>(name, type);
        }

        public String name() {
            return this.name;
        }

        public Class<T> type() {
            return this.type;
        }
    }
}
