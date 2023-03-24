/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb.impl.notification;

import java.util.Objects;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.NotificationCategory;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.api.exceptions.Status;

public final class NotificationImplementation implements Notification {
    private final Status.Code statusCode;
    private final String title;
    private final String description;
    private final SeverityLevel severity;
    private final NotificationCategory category;
    private final InputPosition position;

    NotificationImplementation(
            NotificationCodeWithDescription notificationCodeWithDescription,
            InputPosition position,
            String title,
            String description) {

        this.statusCode = notificationCodeWithDescription.getStatus().code();

        if (statusCode instanceof Status.NotificationCode) {
            this.severity = mapSeverity(((Status.NotificationCode) statusCode).getSeverity());
            this.category = mapCategory(((Status.NotificationCode) statusCode).getNotificationCategory());
        } else {
            throw new IllegalStateException("'" + statusCode + "' is not a notification code.");
        }

        this.position = position;
        this.title = title;
        this.description = description;
    }

    public static class NotificationBuilder {
        private final NotificationCodeWithDescription notificationCodeWithDescription;
        private String title;
        private String description;
        private InputPosition position;

        public NotificationBuilder(NotificationCodeWithDescription notificationCodeWithDescription) {
            this.notificationCodeWithDescription = notificationCodeWithDescription;
            this.description = notificationCodeWithDescription.getDescription();
            this.title = notificationCodeWithDescription.getStatus().code().description();
            this.position = InputPosition.empty;
        }

        public NotificationBuilder setPosition(InputPosition position) {
            this.position = position;
            return this;
        }

        public NotificationBuilder setTitleDetails(String... details) {
            if (details.length > 0) {
                this.title = String.format(title, (Object[]) details);
            }
            return this;
        }

        public NotificationBuilder setNotificationDetails(String... details) {
            if (details.length > 0) {
                this.description = String.format(description, (Object[]) details);
            }
            return this;
        }

        public NotificationImplementation build() {
            return new NotificationImplementation(notificationCodeWithDescription, position, title, description);
        }
    }

    @Override
    public String getCode() {
        return statusCode.serialize();
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
    public InputPosition getPosition() {
        return position;
    }

    @Override
    public NotificationCategory getCategory() {
        return category;
    }

    @Override
    public SeverityLevel getSeverity() {
        return severity;
    }

    @Override
    public String toString() {
        return "Notification{" + "position=" + position + ", description='" + description + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NotificationImplementation that = (NotificationImplementation) o;
        return Objects.equals(position, that.position) && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, description);
    }

    private SeverityLevel mapSeverity(String severityLevel) {
        return SeverityLevel.valueOf(severityLevel);
    }

    private NotificationCategory mapCategory(String category) {
        return NotificationCategory.valueOf(category);
    }
}
