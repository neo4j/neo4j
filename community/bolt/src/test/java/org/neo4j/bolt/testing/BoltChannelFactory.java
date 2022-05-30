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
package org.neo4j.bolt.testing;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.protocol.common.connection.ConnectionHintProvider;
import org.neo4j.bolt.protocol.common.protector.ChannelProtector;
import org.neo4j.bolt.security.basic.BasicAuthentication;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.memory.EmptyMemoryTracker;

public final class BoltChannelFactory {
    private BoltChannelFactory() {}

    public static BoltChannel newTestBoltChannel() {
        return newTestBoltChannel("bolt-1");
    }

    public static BoltChannel newTestBoltChannel(String id) {
        return new BoltChannel(
                id,
                "bolt",
                new EmbeddedChannel(),
                new BasicAuthentication(AuthManager.NO_AUTH),
                ChannelProtector.NULL,
                ConnectionHintProvider.noop(),
                EmptyMemoryTracker.INSTANCE);
    }

    public static BoltChannel newTestBoltChannel(Channel ch) {
        return new BoltChannel(
                "bolt-1",
                "bolt",
                ch,
                new BasicAuthentication(AuthManager.NO_AUTH),
                ChannelProtector.NULL,
                ConnectionHintProvider.noop(),
                EmptyMemoryTracker.INSTANCE);
    }
}
