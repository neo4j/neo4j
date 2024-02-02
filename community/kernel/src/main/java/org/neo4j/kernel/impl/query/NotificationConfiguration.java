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
package org.neo4j.kernel.impl.query;

import java.util.Collections;
import java.util.Set;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.NotificationCategory;
import org.neo4j.graphdb.SeverityLevel;

/**
 * Notifications are created during planning or execution of a query.
 * This configuration decides which notifications should be returned, given by a minimum severity level and disabled categories.
 *
 * Example:
 * NotificationConfiguration(NONE, Set()) - should not return any notifications
 * NotificationConfiguration(WARNING, Set()) - should return all notifications with severityLevel WARNING
 * NotificationConfiguration(INFORMATION, Set(PERFORMANCE)) - should return notifications with severity INFORMATION or WARNING, but should exclude notifications with category PERFORMANCE.
 */
public record NotificationConfiguration(Severity severityLevel, Set<Category> disabledCategories) {
    public enum Severity {
        INFORMATION,
        WARNING,
        NONE
    }

    public enum Category {
        HINT,
        UNRECOGNIZED,
        UNSUPPORTED,
        PERFORMANCE,
        DEPRECATION,
        GENERIC,
        SECURITY,
        TOPOLOGY,
        SCHEMA
    }

    public static final NotificationConfiguration DEFAULT_FILTER = all();
    public static final NotificationConfiguration NONE = none();

    public static NotificationConfiguration all() {
        return new NotificationConfiguration(Severity.INFORMATION, Collections.emptySet());
    }

    public static NotificationConfiguration none() {
        return new NotificationConfiguration(Severity.NONE, Collections.emptySet());
    }

    public boolean includes(Notification notification) {
        return includesSeverityLevel(notification.getSeverity())
                && !disabledNotificationCategory(notification.getCategory());
    }

    private boolean disabledNotificationCategory(NotificationCategory notificationCategory) {
        var category =
                switch (notificationCategory) {
                    case PERFORMANCE -> Category.PERFORMANCE;
                    case DEPRECATION -> Category.DEPRECATION;
                    case UNRECOGNIZED -> Category.UNRECOGNIZED;
                    case HINT -> Category.HINT;
                    case GENERIC -> Category.GENERIC;
                    case UNSUPPORTED -> Category.UNSUPPORTED;
                    case SECURITY -> Category.SECURITY;
                    case TOPOLOGY -> Category.TOPOLOGY;
                    case SCHEMA -> Category.SCHEMA;
                    case UNKNOWN -> null;
                };

        return category != null && disabledCategories.contains(category);
    }

    private boolean includesSeverityLevel(SeverityLevel severityLevel) {
        return switch (this.severityLevel) {
            case INFORMATION -> true;
            case NONE -> false;
            case WARNING -> severityLevel.equals(SeverityLevel.WARNING);
        };
    }
}
