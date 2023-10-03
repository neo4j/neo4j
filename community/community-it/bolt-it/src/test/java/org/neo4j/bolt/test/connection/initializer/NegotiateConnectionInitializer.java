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

import java.io.IOException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.connection.initializer.Negotiated;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;

/**
 * Performs the Bolt protocol negotiation upon connection creation.
 * <p />
 * This initializer may be registered via the {@link Negotiated} annotation or any of its children.
 */
public final class NegotiateConnectionInitializer extends AbstractNegotiatingConnectionInitializer {

    @Override
    public void initialize(
            ExtensionContext extensionContext, ParameterContext context, BoltWire wire, TransportConnection connection)
            throws ParameterResolutionException {
        if (!wire.supportsLogonMessage()) {
            // if the selected protocol version does not support explicit authentication, we will make
            // sure that the test has been set up correctly (e.g. requires authentication)
            if (!context.isAnnotated(Authenticated.class)) {
                // if no authentication has been requested, the test was targeted incorrectly as it
                // will implicitly complete authentication as part of the negotiation phase
                throw new UnsupportedOperationException(
                        "Cannot enter AUTHENTICATION stage via negotiation message in protocol version "
                                + wire.getProtocolVersion());
            }
            return;
        }

        try {
            connection.send(wire.hello());

            BoltConnectionAssertions.assertThat(connection)
                    .receivesSuccess(meta -> this.assertNegotiatedFeatures(wire, meta));
        } catch (IOException | AssertionError ex) {
            throw new ParameterResolutionException("Failed to authenticate connection", ex);
        }
    }
}
