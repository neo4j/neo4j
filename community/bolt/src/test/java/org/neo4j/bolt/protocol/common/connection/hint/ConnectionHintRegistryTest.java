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
package org.neo4j.bolt.protocol.common.connection.hint;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.values.virtual.MapValueBuilder;

class ConnectionHintRegistryTest {

    @Test
    void shouldInvokeProviders() {
        var provider1 = Mockito.mock(ConnectionHintProvider.class, Mockito.RETURNS_MOCKS);
        var provider2 = Mockito.mock(ConnectionHintProvider.class, Mockito.RETURNS_MOCKS);
        var provider3 = Mockito.mock(ConnectionHintProvider.class, Mockito.RETURNS_MOCKS);

        Mockito.doReturn(new ProtocolVersion(1, 0)).when(provider1).supportedSince();
        Mockito.doReturn(new ProtocolVersion(1, 0)).when(provider2).supportedSince();
        Mockito.doReturn(new ProtocolVersion(1, 0)).when(provider3).supportedSince();

        Mockito.doReturn(ProtocolVersion.MAX).when(provider1).supportedUntil();
        Mockito.doReturn(ProtocolVersion.MAX).when(provider2).supportedUntil();
        Mockito.doReturn(ProtocolVersion.MAX).when(provider3).supportedUntil();

        Mockito.doReturn(true).when(provider1).isApplicable();
        Mockito.doReturn(true).when(provider2).isApplicable();
        Mockito.doReturn(true).when(provider3).isApplicable();

        var builder = new MapValueBuilder();

        var registry = ConnectionHintRegistry.newBuilder()
                .withProviders(provider1, provider2, provider3)
                .build();

        registry.applyTo(new ProtocolVersion(24, 3), builder);

        Mockito.verify(provider1).supportedSince();
        Mockito.verify(provider1).supportedUntil();
        Mockito.verify(provider1).isApplicable();
        Mockito.verify(provider1).append(builder);

        Mockito.verify(provider2).supportedSince();
        Mockito.verify(provider2).supportedUntil();
        Mockito.verify(provider2).isApplicable();
        Mockito.verify(provider2).append(builder);

        Mockito.verify(provider3).supportedSince();
        Mockito.verify(provider3).supportedUntil();
        Mockito.verify(provider3).isApplicable();
        Mockito.verify(provider3).append(builder);
    }

    @Test
    void shouldFilterProviders() {
        var provider1 = Mockito.mock(ConnectionHintProvider.class, Mockito.RETURNS_MOCKS);
        var provider2 = Mockito.mock(ConnectionHintProvider.class, Mockito.RETURNS_MOCKS);
        var provider3 = Mockito.mock(ConnectionHintProvider.class, Mockito.RETURNS_MOCKS);

        var actual = new ProtocolVersion(5, 2, 0);
        var supported = new ProtocolVersion(5, 1, 0);
        var unsupported = new ProtocolVersion(5, 3, 0);

        Mockito.doReturn(supported).when(provider1).supportedSince();
        Mockito.doCallRealMethod().when(provider1).supportedUntil();
        Mockito.doReturn(true).when(provider1).isApplicable();

        Mockito.doReturn(unsupported).when(provider2).supportedSince();
        Mockito.doCallRealMethod().when(provider2).supportedUntil();
        Mockito.doReturn(true).when(provider2).isApplicable();

        Mockito.doCallRealMethod().when(provider3).supportedSince();
        Mockito.doCallRealMethod().when(provider3).supportedUntil();
        Mockito.doReturn(false).when(provider3).isApplicable();

        var builder = new MapValueBuilder();

        var registry = ConnectionHintRegistry.newBuilder()
                .withProviders(provider1, provider2, provider3)
                .build();

        registry.applyTo(actual, builder);

        Mockito.verify(provider1).supportedSince();
        Mockito.verify(provider1).supportedUntil();
        Mockito.verify(provider1).isApplicable();
        Mockito.verify(provider1).append(builder);

        Mockito.verify(provider2).supportedSince();

        Mockito.verify(provider3).supportedSince();
        Mockito.verify(provider3).supportedUntil();
        Mockito.verify(provider3).isApplicable();
    }
}
