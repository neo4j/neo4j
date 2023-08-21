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
package org.neo4j.server.http.cypher.entity;

import java.util.ArrayList;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.NotificationCategory;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

public class HttpNotification implements Notification {
    private final String code;
    private final String title;
    private final String description;
    private final SeverityLevel severity;
    private final InputPosition inputPosition;

    private final NotificationCategory category;

    private HttpNotification(
            String code,
            String title,
            String description,
            String severity,
            InputPosition inputPosition,
            String category) {
        this.code = code;
        this.title = title;
        this.description = description;
        this.severity = SeverityLevel.valueOf(severity);
        this.inputPosition = inputPosition;
        this.category = NotificationCategory.valueOf(category);
    }

    public static Iterable<Notification> iterableFromAnyValue(AnyValue anyValue) {
        if (anyValue == null) {
            return new ArrayList<>();
        }
        ArrayList<Notification> notifications = new ArrayList<>();

        ListValue listValue = (ListValue) anyValue;

        listValue.forEach(listItem -> {
            MapValue mapValue = (MapValue) listItem;
            InputPosition inputPosition = InputPosition.empty;

            if (mapValue.containsKey("position")) {
                var positionMap = (MapValue) mapValue.get("position");
                inputPosition = new InputPosition(
                        ((IntValue) positionMap.get("offset")).value(),
                        ((IntValue) positionMap.get("line")).value(),
                        ((IntValue) positionMap.get("column")).value());
            }

            notifications.add(new HttpNotification(
                    ((TextValue) mapValue.get("code")).stringValue(),
                    ((TextValue) mapValue.get("title")).stringValue(),
                    ((TextValue) mapValue.get("description")).stringValue(),
                    ((TextValue) mapValue.get("severity")).stringValue(),
                    inputPosition,
                    mapValue.containsKey("category")
                            ? ((TextValue) mapValue.get("category")).stringValue()
                            : NotificationCategory.UNKNOWN.name()));
        });

        return notifications;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getTitle() {
        return title;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public SeverityLevel getSeverity() {
        return severity;
    }

    @Override
    public InputPosition getPosition() {
        return inputPosition;
    }

    @Override
    public NotificationCategory getCategory() {
        return category;
    }
}
