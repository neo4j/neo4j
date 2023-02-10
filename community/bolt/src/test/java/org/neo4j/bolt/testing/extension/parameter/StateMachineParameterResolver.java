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
package org.neo4j.bolt.testing.extension.parameter;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.testing.extension.dependency.StateMachineDependencyProvider;
import org.neo4j.bolt.testing.extension.initializer.StateMachineInitializer;
import org.neo4j.bolt.testing.extension.provider.StateMachineConnectionRegistry;
import org.neo4j.bolt.testing.fsm.StateMachineProvider;
import org.neo4j.logging.internal.NullLogService;

public class StateMachineParameterResolver implements ParameterResolver {
    private final StateMachineDependencyProvider dependencyProvider;
    private final StateMachineProvider fsmProvider;
    private final StateMachineConnectionRegistry connectionRegistry;

    private final List<StateMachine> instances = new ArrayList<>();

    public StateMachineParameterResolver(
            StateMachineDependencyProvider dependencyProvider,
            StateMachineProvider fsmProvider,
            StateMachineConnectionRegistry connectionRegistry) {
        this.dependencyProvider = dependencyProvider;
        this.fsmProvider = fsmProvider;
        this.connectionRegistry = connectionRegistry;
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return StateMachine.class.isAssignableFrom(
                parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        var protocol = this.fsmProvider.protocol(
                NullLogService.getInstance(),
                this.dependencyProvider.spi(extensionContext),
                this.dependencyProvider.clock(extensionContext));
        var connection = this.dependencyProvider.connection(extensionContext);

        var fsm = protocol.createStateMachine(connection);
        this.connectionRegistry.register(fsm, connection);

        var initializers = StateMachineInitializer.listProviders(parameterContext.getParameter());
        try {
            for (var initializer : initializers) {
                initializer.initialize(
                        extensionContext, parameterContext, this.dependencyProvider, this.fsmProvider, fsm);
            }
        } catch (BoltConnectionFatality ex) {
            throw new ParameterResolutionException("Failed to initialize state machine", ex);
        }

        this.instances.add(fsm);
        return fsm;
    }
}
