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

import org.neo4j.bolt.protocol.common.message.decoder.transaction.DefaultRunMessageDecoder;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;
import org.neo4j.values.virtual.MapValue;

public final class RunMessageDecoderV44 extends DefaultRunMessageDecoder {
    private static final RunMessageDecoderV44 INSTANCE = new RunMessageDecoderV44();

    private RunMessageDecoderV44() {}

    public static RunMessageDecoderV44 getInstance() {
        return INSTANCE;
    }

    @Override
    protected NotificationsConfig readNotificationsConfig(MapValue ignore) {
        return null;
    }
}
