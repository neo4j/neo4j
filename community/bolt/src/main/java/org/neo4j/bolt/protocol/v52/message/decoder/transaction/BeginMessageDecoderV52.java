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
package org.neo4j.bolt.protocol.v52.message.decoder.transaction;

import org.neo4j.bolt.protocol.common.message.decoder.transaction.DefaultBeginMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.util.NotificationsConfigMetadataReader;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.values.virtual.MapValue;

public class BeginMessageDecoderV52 extends DefaultBeginMessageDecoder {
    private static final BeginMessageDecoderV52 INSTANCE = new BeginMessageDecoderV52();

    private BeginMessageDecoderV52() {}

    public static BeginMessageDecoderV52 getInstance() {
        return INSTANCE;
    }

    @Override
    protected NotificationsConfig readNotificationsConfig(MapValue meta) throws IllegalStructArgumentException {
        return NotificationsConfigMetadataReader.readLegacyFromMapValue(meta);
    }
}
