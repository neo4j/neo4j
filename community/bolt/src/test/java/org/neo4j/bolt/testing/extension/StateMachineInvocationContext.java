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
package org.neo4j.bolt.testing.extension;

import java.time.Clock;
import java.util.List;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.testing.extension.dependency.StateMachineDependencyProvider;
import org.neo4j.bolt.testing.extension.lifecycle.StateMachineDependencyProviderLifecycleListener;
import org.neo4j.bolt.testing.extension.parameter.ConnectionParameterResolver;
import org.neo4j.bolt.testing.extension.parameter.StateMachineParameterResolver;
import org.neo4j.bolt.testing.extension.parameter.StaticParameterResolver;
import org.neo4j.bolt.testing.extension.parameter.SupplierParameterResolver;
import org.neo4j.bolt.testing.extension.provider.ConnectionProvider;
import org.neo4j.bolt.testing.extension.provider.StateMachineConnectionRegistry;
import org.neo4j.bolt.testing.extension.provider.TransactionIdProvider;
import org.neo4j.bolt.testing.fsm.StateMachineProvider;
import org.neo4j.bolt.testing.messages.BoltMessages;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.bolt.tx.TransactionManager;

public class StateMachineInvocationContext implements TestTemplateInvocationContext {
    private final StateMachineDependencyProvider dependencyProvider;
    private final StateMachineProvider fsmProvider;
    private final StateMachineConnectionRegistry connectionRegistry = new StateMachineConnectionRegistry();

    public StateMachineInvocationContext(
            StateMachineDependencyProvider dependencyProvider, StateMachineProvider fsmProvider) {
        this.dependencyProvider = dependencyProvider;
        this.fsmProvider = fsmProvider;
    }

    @Override
    public String getDisplayName(int invocationIndex) {
        return this.fsmProvider.version().toString();
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
        return List.of(
                new StateMachineDependencyProviderLifecycleListener(this.dependencyProvider, this.connectionRegistry),
                new SupplierParameterResolver<>(
                        BoltGraphDatabaseManagementServiceSPI.class, this.dependencyProvider::spi),
                new SupplierParameterResolver<>(Clock.class, this.dependencyProvider::clock),
                new SupplierParameterResolver<>(ProtocolVersion.class, this.fsmProvider::version),
                new SupplierParameterResolver<>(BoltMessages.class, this.fsmProvider::messages),
                new StateMachineParameterResolver(this.dependencyProvider, this.fsmProvider, this.connectionRegistry),
                new ConnectionParameterResolver(this.connectionRegistry),
                new StaticParameterResolver<>(ConnectionProvider.class, this.connectionRegistry),
                new SupplierParameterResolver<>(ResponseRecorder.class, () -> new ResponseRecorder()),
                new SupplierParameterResolver<>(
                        TransactionIdProvider.class, ctx -> new TransactionIdProvider(ctx, this.dependencyProvider)),
                new SupplierParameterResolver(TransactionManager.class, ctx -> this.dependencyProvider
                        .transactionManager()
                        .orElseThrow(() -> new ParameterResolutionException(
                                "TransactionManager is not exposed by this dependency provider"))));
    }
}
