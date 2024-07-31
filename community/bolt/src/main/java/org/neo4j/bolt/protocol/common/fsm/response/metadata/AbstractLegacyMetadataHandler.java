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
package org.neo4j.bolt.protocol.common.fsm.response.metadata;

import static org.neo4j.values.storable.Values.intValue;

import org.neo4j.bolt.protocol.common.fsm.response.MetadataConsumer;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

public abstract class AbstractLegacyMetadataHandler extends DefaultMetadataHandler {

    @Override
    public void onNotifications(
            MetadataConsumer handler, Iterable<Notification> notifications, Iterable<GqlStatusObject> statuses) {
        var it = notifications.iterator();
        if (!it.hasNext()) {
            return;
        }

        var children = ListValueBuilder.newListBuilder();
        while (it.hasNext()) {
            var notification = it.next();

            var pos = notification.getPosition(); // position is optional
            var includePosition = !pos.equals(InputPosition.empty);
            int size = includePosition ? 5 : 4;
            var builder = new MapValueBuilder(size);

            builder.add("code", Values.utf8Value(notification.getCode()));
            builder.add("title", Values.utf8Value(notification.getTitle()));
            builder.add("description", Values.utf8Value(notification.getDescription()));
            builder.add("severity", Values.utf8Value(notification.getSeverity().toString()));
            builder.add(
                    "category", Values.stringValue(notification.getCategory().toString()));

            if (includePosition) {
                // only add the position if it is not empty
                builder.add("position", VirtualValues.map(new String[] {"offset", "line", "column"}, new AnyValue[] {
                    intValue(pos.getOffset()), intValue(pos.getLine()), intValue(pos.getColumn())
                }));
            }

            children.add(builder.build());
        }

        handler.onMetadata("notifications", children.build());
    }
}
