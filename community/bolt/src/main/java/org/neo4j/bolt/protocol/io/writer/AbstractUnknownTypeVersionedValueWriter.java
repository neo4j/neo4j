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
package org.neo4j.bolt.protocol.io.writer;

import java.util.Map;
import org.neo4j.bolt.protocol.io.pipeline.WriterContext;
import org.neo4j.notifications.NotificationCodeWithDescription;

abstract class AbstractUnknownTypeVersionedValueWriter implements VersionedValueWriter {

    protected abstract String typeName();

    protected void reportUnknownType(WriterContext ctx, String originalTypeDescription) {
        var typeName = this.typeName();
        var notificationManager = ctx.connection().fsm().connection().notificationManager();

        notificationManager.addNotification(NotificationCodeWithDescription.clientDoesNotSupportType(typeName));
        notificationManager.addGqlStatus(NotificationCodeWithDescription.clientDoesNotSupportType(typeName));

        ctx.buffer().writeMap(Map.of("originalType", originalTypeDescription, "reason", "UNKNOWN_TYPE"));
    }

    protected void reportUnknownType(WriterContext ctx) {
        this.reportUnknownType(ctx, this.typeName());
    }
}
