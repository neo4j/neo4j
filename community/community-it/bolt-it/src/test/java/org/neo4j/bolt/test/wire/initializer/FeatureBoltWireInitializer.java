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
package org.neo4j.bolt.test.wire.initializer;

import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.test.annotation.wire.initializer.EnableFeature;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.testing.util.AnnotationUtil;

/**
 * Enables a desired set of features on a given Bolt wire.
 */
public class FeatureBoltWireInitializer implements BoltWireInitializer {

    @Override
    public void initialize(ExtensionContext context, BoltWire wire) {
        var features = AnnotationUtil.findAnnotations(context, EnableFeature.class).stream()
                .flatMap(annotation -> Stream.of(annotation.value()))
                .distinct()
                .filter(wire::isOptionalFeature)
                .toArray(Feature[]::new);

        wire.enable(features);
    }
}
