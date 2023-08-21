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
package org.neo4j.bolt;

class BoltChannelTest {
    //    private final Channel channel = mock(Channel.class);
    //
    //    @Test
    //    void shouldCloseUnderlyingChannelWhenItIsOpen() {
    //        var channel = channelMock(true);
    //        var boltChannel = new BoltChannel(
    //                "bolt-1",
    //                "bolt",
    //                channel,
    //                mock(Authentication.class),
    //                ConnectionListener.NULL,
    //                mock(ConnectionHintProvider.class),
    //                mock(MemoryTracker.class));
    //
    //        boltChannel.close();
    //
    //        verify(channel).close();
    //    }
    //
    //    @Test
    //    void shouldNotCloseUnderlyingChannelWhenItIsClosed() {
    //        var channel = channelMock(false);
    //        var boltChannel = new BoltChannel(
    //                "bolt-1",
    //                "bolt",
    //                channel,
    //                mock(Authentication.class),
    //                ConnectionListener.NULL,
    //                mock(ConnectionHintProvider.class),
    //                mock(MemoryTracker.class));
    //
    //        boltChannel.close();
    //
    //        verify(channel, never()).close();
    //    }
    //
    //    @Test
    //    void shouldHaveId() {
    //        var boltChannel = new BoltChannel(
    //                "bolt-42",
    //                "bolt",
    //                channel,
    //                mock(Authentication.class),
    //                ConnectionListener.NULL,
    //                mock(ConnectionHintProvider.class),
    //                mock(MemoryTracker.class));
    //
    //        assertEquals("bolt-42", boltChannel.id());
    //    }
    //
    //    @Test
    //    void shouldHaveConnector() {
    //        var boltChannel = new BoltChannel(
    //                "bolt-1",
    //                "my-bolt",
    //                channel,
    //                mock(Authentication.class),
    //                ConnectionListener.NULL,
    //                mock(ConnectionHintProvider.class),
    //                mock(MemoryTracker.class));
    //
    //        assertEquals("my-bolt", boltChannel.connectorId());
    //    }
    //
    //    @Test
    //    void shouldHaveConnectTime() {
    //        var boltChannel = new BoltChannel(
    //                "bolt-1",
    //                "my-bolt",
    //                channel,
    //                mock(Authentication.class),
    //                ConnectionListener.NULL,
    //                mock(ConnectionHintProvider.class),
    //                mock(MemoryTracker.class));
    //
    //        assertThat(boltChannel.connectTime()).isGreaterThan(0L);
    //    }
    //
    //    @Test
    //    void shouldHaveUsernameAndUserAgent() {
    //        var boltChannel = new BoltChannel(
    //                "bolt-1",
    //                "my-bolt",
    //                channel,
    //                mock(Authentication.class),
    //                ConnectionListener.NULL,
    //                mock(ConnectionHintProvider.class),
    //                mock(MemoryTracker.class));
    //
    //        assertNull(boltChannel.username());
    //        boltChannel.updateUser("hello", "my-bolt-driver/1.2.3");
    //        assertEquals("hello", boltChannel.username());
    //        assertEquals("my-bolt-driver/1.2.3", boltChannel.userAgent());
    //    }
    //
    //    @Test
    //    void shouldExposeClientConnectionInfo() {
    //        var channel = new EmbeddedChannel();
    //        var boltChannel = new BoltChannel(
    //                "bolt-42",
    //                "my-bolt",
    //                channel,
    //                mock(Authentication.class),
    //                ConnectionListener.NULL,
    //                mock(ConnectionHintProvider.class),
    //                mock(MemoryTracker.class));
    //
    //        ClientConnectionInfo info1 = boltChannel.info();
    //        assertEquals("bolt-42", info1.connectionId());
    //        assertEquals("bolt", info1.protocol());
    //        assertEquals(SocketAddress.format(channel.remoteAddress()), info1.clientAddress());
    //
    //        boltChannel.updateUser("Tom", "my-driver");
    //
    //        ClientConnectionInfo info2 = boltChannel.info();
    //        assertEquals("bolt-42", info2.connectionId());
    //        assertEquals("bolt", info2.protocol());
    //        assertEquals(SocketAddress.format(channel.remoteAddress()), info2.clientAddress());
    //        assertThat(info2.asConnectionDetails()).contains("my-driver");
    //    }
    //
    //    @Test
    //    void shouldDisableChannelProtectorAfterUpdateUser() throws Throwable {
    //        // Given
    //        var protector = mock(ConnectionListener.class);
    //        BoltChannel boltChannel = new BoltChannel(
    //                "bolt-1",
    //                "bolt",
    //                channel,
    //                mock(Authentication.class),
    //                protector,
    //                mock(ConnectionHintProvider.class),
    //                mock(MemoryTracker.class));
    //
    //        // When
    //        boltChannel.updateUser("hello", "my-bolt-driver/1.2.3");
    //
    //        // Then
    //        verify(protector).onAuthenticated();
    //    }
    //
    //    private static Channel channelMock(boolean open) {
    //        Channel channel = mock(Channel.class);
    //        when(channel.isOpen()).thenReturn(open);
    //        ChannelFuture channelFuture = mock(ChannelFuture.class);
    //        when(channel.close()).thenReturn(channelFuture);
    //        return channel;
    //    }
}
