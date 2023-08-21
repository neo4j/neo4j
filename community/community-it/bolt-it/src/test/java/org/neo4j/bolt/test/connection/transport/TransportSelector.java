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
package org.neo4j.bolt.test.connection.transport;

import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.bolt.test.annotation.connection.SelectTransport;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.util.AnnotationUtil;

/**
 * Selects the set of transports for which a given test template shall be instantiated.
 * <p />
 * This interface is typically implemented in conjunction with a {@link SelectTransport} annotation or a custom
 * annotation bearing the {@link SelectTransport} annotation.
 * <p />
 * Implementations of this interface are expected to provide a publicly accessible no-args construction via which they
 * are resolved when referenced using the {@link TransportSelector} annotation or one of its children.
 * <p />
 * Refer to the {@link org.neo4j.bolt.test.annotation.connection.transport} package for examples on how to utilize this
 * interface in the most optimal fashion.
 */
public interface TransportSelector {

    /**
     * Selects a list of transports for which the targeted test template shall be instantiated.
     *
     * @param context an extension context.
     * @return a stream of transports.
     */
    Stream<TransportType> select(ExtensionContext context);

    static Optional<TransportSelector> findSelector(ExtensionContext context) {
        return AnnotationUtil.selectProvider(context, SelectTransport.class, SelectTransport::value);
    }
}
