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

package org.neo4j.bolt.test.connection.initializer;

import java.util.Map;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.testing.messages.BoltWire;

public abstract class AbstractNegotiatingConnectionInitializer implements ConnectionInitializer {

    protected void assertNegotiatedFeatures(BoltWire wire, Map<String, Object> meta) {
        if (wire.getEnabledFeatures().isEmpty()) {
            Assertions.assertThat(meta).doesNotContainKey("patch_bolt");
            return;
        }

        Assertions.assertThat(meta).hasEntrySatisfying("patch_bolt", features -> Assertions.assertThat(features)
                .asInstanceOf(InstanceOfAssertFactories.list(String.class))
                .containsAll(
                        wire.getEnabledFeatures().stream().map(Feature::getId).collect(Collectors.toSet())));
    }
}
