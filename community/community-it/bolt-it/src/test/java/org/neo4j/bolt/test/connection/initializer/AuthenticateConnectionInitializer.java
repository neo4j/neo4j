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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;

/**
 * Performs Bolt authentication upon connection creation.
 * <p />
 * This initializer may be registered via the {@link Authenticated} annotation or any of its children.
 */
public final class AuthenticateConnectionInitializer implements ConnectionInitializer {

    @Override
    public void initialize(
            ExtensionContext extensionContext, ParameterContext context, BoltWire wire, TransportConnection connection)
            throws ParameterResolutionException {
        var command = this.getCommand(context, wire);

        try {
            // If using new method hello will not have auth and needs to be sent to get the authentication state
            if (wire.supportsLogonMessage()) {
                connection.send(wire.hello());
            } else {
                connection.send(command);
            }

            if (wire.getEnabledFeatures().isEmpty()) {
                BoltConnectionAssertions.assertThat(connection).receivesSuccess();
            } else {
                BoltConnectionAssertions.assertThat(connection).receivesSuccess(meta -> {
                    Assertions.assertThat(meta)
                            .hasEntrySatisfying("patch_bolt", features -> Assertions.assertThat(features)
                                    .asInstanceOf(InstanceOfAssertFactories.list(String.class))
                                    .containsAll(wire.getEnabledFeatures().stream()
                                            .map(Feature::getId)
                                            .collect(Collectors.toSet())));
                });
            }

            // Then in new auth you auth after hello
            if (wire.supportsLogonMessage()) {
                connection.send(command);
                BoltConnectionAssertions.assertThat(connection).receivesSuccess();
            }
        } catch (IOException | AssertionError ex) {
            throw new ParameterResolutionException("Failed to authenticate connection", ex);
        }
    }

    private ByteBuf getCommand(ParameterContext context, BoltWire wire) {
        return context.findAnnotation(Authenticated.class)
                .flatMap(annotation -> this.getCommand(annotation, wire))
                .orElseGet(() -> wire.supportsLogonMessage() ? wire.logon() : wire.hello());
    }

    private Optional<ByteBuf> getCommand(Authenticated annotation, BoltWire wire) {
        if (annotation.principal().isEmpty()) {
            return Optional.empty();
        }

        if (wire.supportsLogonMessage()) {
            return Optional.of(wire.logon(annotation.principal(), annotation.credentials()));
        }
        return Optional.of(wire.hello(x -> x.withBasicAuth(annotation.principal(), annotation.credentials())));
    }
}
