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
package org.neo4j.bolt.testing.messages.factory;

import java.util.Collection;
import org.neo4j.bolt.protocol.v52.BoltProtocolV52;
import org.neo4j.bolt.protocol.v56.BoltProtocolV56;
import org.neo4j.kernel.impl.query.NotificationConfiguration;

@SuppressWarnings("unchecked")
public interface NotificationsMessageBuilder<T extends NotificationsMessageBuilder<T>> extends WireMessageBuilder<T> {
    default T withDisabledNotifications() {
        if (getProtocolVersion().compareTo(BoltProtocolV52.VERSION) >= 0) {
            getMeta().put("notifications_minimum_severity", "OFF");
        }
        return (T) this;
    }

    default T withSeverity(NotificationConfiguration.Severity severity) {
        return withUnknownSeverity(severity.toString());
    }

    default T withUnknownSeverity(String severity) {
        if (getProtocolVersion().compareTo(BoltProtocolV52.VERSION) >= 0) {
            getMeta().put("notifications_minimum_severity", severity);
        }
        return (T) this;
    }

    default T withDisabledCategories(Collection<NotificationConfiguration.Category> categories) {
        return withUnknownDisabledCategories(
                categories.stream().map(Enum::toString).toList());
    }

    default T withUnknownDisabledCategories(Collection<String> categories) {
        if (getProtocolVersion().compareTo(BoltProtocolV56.VERSION) >= 0) {
            getMeta()
                    .put(
                            "notifications_disabled_classifications",
                            categories.stream().toList());
        } else if (getProtocolVersion().compareTo(BoltProtocolV52.VERSION) >= 0) {
            getMeta()
                    .put(
                            "notifications_disabled_categories",
                            categories.stream().toList());
        }
        return (T) this;
    }
}
