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
package org.neo4j.bolt.protocol.common.connection;

import java.util.function.Function;
import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValueBuilder;

@FunctionalInterface
public interface ConnectionHintProvider {

    /**
     * Provides a NOOP connection hint provider.
     *
     * @return a noop provider.
     */
    static ConnectionHintProvider noop() {
        return hints -> {};
    }

    /**
     * Creates a connection hint provider for a given key and dynamically retrieved value.
     *
     * @param key      a key.
     * @param supplier a value supplier.
     * @return a connection hint provider.
     */
    static ConnectionHintProvider forKey(String key, Supplier<AnyValue> supplier) {
        return hints -> hints.add(key, supplier.get());
    }

    /**
     * Creates a connection hint provider for a given key and configuration setting.
     *
     * @param key       a key.
     * @param config    a configuration source.
     * @param setting   a setting.
     * @param converter a converter function.
     * @param <I>       a setting value type.
     * @return a connection hint provider.
     */
    static <I> ConnectionHintProvider forConfig(
            String key, Config config, Setting<I> setting, Function<I, AnyValue> converter) {
        return forKey(key, () -> converter.apply(config.get(setting)));
    }

    /**
     * Appends a set of connection hints to the given map.
     *
     * @param hints a map consisting of zero or more hints.
     */
    void append(MapValueBuilder hints);

    /**
     * Combines this connection hint provider with another provider.
     *
     * @param next the following provider.
     * @return a connection hint provider.
     */
    default ConnectionHintProvider and(ConnectionHintProvider next) {
        final var curr = this;

        return hints -> {
            curr.append(hints);
            next.append(hints);
        };
    }
}
