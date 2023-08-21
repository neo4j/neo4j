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
package org.neo4j.bolt.protocol.v51.message.decoder.authentication;

import java.util.Collections;
import java.util.Map;
import org.neo4j.bolt.protocol.common.message.decoder.authentication.DefaultHelloMessageDecoder;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;

public final class HelloMessageDecoderV51 extends DefaultHelloMessageDecoder {
    private static final HelloMessageDecoderV51 INSTANCE = new HelloMessageDecoderV51();

    private HelloMessageDecoderV51() {}

    public static HelloMessageDecoderV51 getInstance() {
        return INSTANCE;
    }

    @Override
    protected NotificationsConfig readNotificationsConfig(Map<String, Object> meta) {
        return null;
    }

    @Override
    protected Map<String, String> readBoltAgent(Map<String, Object> meta) {
        return Collections.emptyMap();
    }
}
