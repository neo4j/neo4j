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
package org.neo4j.bolt.test.wire.selector;

import java.util.Collections;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.test.annotation.wire.selector.ExcludeWire;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.testing.util.AnnotationUtil;

/**
 * Provides a wire selector implementation which selects the tested wires based on a predefined set of in- and
 * exclusions.
 */
public class FilteredBoltWireSelector implements BoltWireSelector {

    @Override
    public Stream<BoltWire> select(ExtensionContext context) {
        var explicitIncludes = AnnotationUtil.findAnnotation(context, IncludeWire.class)
                .map(annotation ->
                        Stream.of(annotation.value()).map(this::decodeVersion).toList())
                .orElseGet(Collections::emptyList);
        var explicitExcludes = AnnotationUtil.findAnnotation(context, ExcludeWire.class)
                .map(annotation ->
                        Stream.of(annotation.value()).map(this::decodeVersion).toList())
                .orElseGet(Collections::emptyList);

        return BoltWire.versions()
                .filter(wire -> (explicitIncludes.isEmpty()
                        || explicitIncludes.stream().anyMatch(range -> range.matches(wire.getProtocolVersion()))))
                .filter(wire -> explicitExcludes.stream().noneMatch(range -> range.matches(wire.getProtocolVersion())));
    }

    private ProtocolVersion decodeVersion(Version annotation) {
        if (annotation.minor() == -1) {
            return new ProtocolVersion(
                    annotation.major(), ProtocolVersion.MAX_MINOR_BIT, ProtocolVersion.MAX_MINOR_BIT);
        }

        return new ProtocolVersion(annotation.major(), annotation.minor(), annotation.range());
    }
}
