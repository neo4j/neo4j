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
package org.neo4j.bolt.fsm;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.testing.fsm.StateMachineProvider;

public class StateMachineProviderTest {

    /**
     * Ensures that all protocol versions are also covered by finite state machine tests.
     * <p>
     * If this test is failing for you, you likely forgot to include an implementation of
     * {@link StateMachineProvider} and/or did not register it with
     * {@link StateMachineProvider#versions()}.
     */
    @TestFactory
    Stream<DynamicTest> shouldIncludeStateMachineProvider() {
        var fsmVersions = StateMachineProvider.versions()
                .map(StateMachineProvider::version)
                .collect(Collectors.toSet());

        return BoltProtocol.installed().stream()
                .map(BoltProtocol::version)
                .map(version -> DynamicTest.dynamicTest(
                        version.toString(),
                        () -> Assertions.assertThat(fsmVersions).contains(version)));
    }
}
