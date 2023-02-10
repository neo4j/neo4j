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
package org.neo4j.bolt.testing.extension.initializer;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.fsm.response.NoopResponseHandler;
import org.neo4j.bolt.protocol.v40.fsm.FailedState;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.testing.assertions.StateMachineAssertions;
import org.neo4j.bolt.testing.extension.dependency.StateMachineDependencyProvider;
import org.neo4j.bolt.testing.fsm.StateMachineProvider;

public class FailedStateMachineInitializer implements StateMachineInitializer {

    @Override
    public void initialize(
            ExtensionContext extensionContext,
            ParameterContext parameterContext,
            StateMachineDependencyProvider dependencyProvider,
            StateMachineProvider provider,
            StateMachine fsm)
            throws BoltConnectionFatality {
        try {
            fsm.process(provider.messages().run("✨✨✨ MAGICAL CRASH STRING ✨✨✨"), NoopResponseHandler.getInstance());
        } catch (BoltConnectionFatality ignore) {
        }

        StateMachineAssertions.assertThat(fsm).isInState(FailedState.class);
    }
}
