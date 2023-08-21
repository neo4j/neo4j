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
package org.neo4j.bolt.protocol.v44.message.decoder.transaction;

import org.neo4j.bolt.protocol.common.message.decoder.transaction.DefaultBeginMessageDecoder;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;
import org.neo4j.bolt.protocol.common.message.request.transaction.BeginMessage;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.values.virtual.MapValue;

public final class BeginMessageDecoderV44 extends DefaultBeginMessageDecoder {
    private static final BeginMessageDecoderV44 INSTANCE = new BeginMessageDecoderV44();

    private BeginMessageDecoderV44() {}

    public static BeginMessageDecoderV44 getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return BeginMessage.SIGNATURE;
    }

    @Override
    protected TransactionType readType(MapValue meta) throws PackstreamReaderException {
        return null;
    }

    @Override
    protected NotificationsConfig readNotificationsConfig(MapValue metadata) throws IllegalStructArgumentException {
        return null;
    }
}
