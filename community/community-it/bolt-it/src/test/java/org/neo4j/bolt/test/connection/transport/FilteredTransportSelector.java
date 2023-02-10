/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.test.connection.transport;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.bolt.test.annotation.connection.transport.ExcludeTransport;
import org.neo4j.bolt.test.annotation.connection.transport.IncludeTransport;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.util.AnnotationUtil;

/**
 * Provides a transport selector for use with the {@link IncludeTransport} and {@link ExcludeTransport} annotations.
 * <p />
 * This selector implementation will return a filtered list of all available transport implementations within the test
 * framework.
 */
public class FilteredTransportSelector implements TransportSelector {

    @Override
    public Stream<TransportType> select(ExtensionContext context) {
        var explicitIncludes = AnnotationUtil.findAnnotation(context, IncludeTransport.class)
                .map(annotation -> Stream.of(annotation.value()))
                .orElseGet(() -> Stream.of(TransportType.values()));
        var explicitExcludes = AnnotationUtil.findAnnotation(context, ExcludeTransport.class)
                .map(annotation -> List.of(annotation.value()))
                .orElseGet(Collections::emptyList);

        return explicitIncludes.distinct().filter(transport -> !explicitExcludes.contains(transport));
    }
}
