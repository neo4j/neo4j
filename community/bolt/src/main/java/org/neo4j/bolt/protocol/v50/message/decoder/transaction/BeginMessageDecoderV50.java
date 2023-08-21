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
package org.neo4j.bolt.protocol.v50.message.decoder.transaction;

import org.neo4j.bolt.protocol.common.message.decoder.transaction.DefaultBeginMessageDecoder;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;
import org.neo4j.bolt.protocol.common.message.request.transaction.BeginMessage;
import org.neo4j.values.virtual.MapValue;

public final class BeginMessageDecoderV50 extends DefaultBeginMessageDecoder {
    private static final BeginMessageDecoderV50 INSTANCE = new BeginMessageDecoderV50();

    private BeginMessageDecoderV50() {}

    public static BeginMessageDecoderV50 getInstance() {
        return INSTANCE;
    }

    @Override
    protected NotificationsConfig readNotificationsConfig(MapValue meta) {
        return null;
    }

    public short getTag() {
        return BeginMessage.SIGNATURE;
    }
}
