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
package org.neo4j.notifications;

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
    private final String message;

    NotificationImplementation(
            NotificationCodeWithDescription notificationCodeWithDescription,
            InputPosition position,
            String title,
            String description,
            String message) {

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
        this.message = message;
    }

    public static class NotificationBuilder {
        private final NotificationCodeWithDescription notificationCodeWithDescription;
        private String title;
        private final String description;
        private InputPosition position;
        private final String message;
        private String[] messageParameters;
        private String[] notificationDetails;

        public NotificationBuilder(NotificationCodeWithDescription notificationCodeWithDescription) {
            this.notificationCodeWithDescription = notificationCodeWithDescription;
            this.description = notificationCodeWithDescription.getDescription();
            this.title = notificationCodeWithDescription.getStatus().code().description();
            this.position = InputPosition.empty;
            this.message = notificationCodeWithDescription.getMessage();
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
            this.notificationDetails = details;
            return this;
        }

        public NotificationBuilder setMessageParameters(String... parameters) {
            this.messageParameters = parameters;
            return this;
        }

        public NotificationImplementation build() {
            var detailedDescription = description;
            if (notificationDetails != null) {
                detailedDescription = String.format(description, (Object[]) notificationDetails);
            }
            var detailedMessage = message;
            if (messageParameters != null) {
                detailedMessage = String.format(message, (Object[]) messageParameters);
            }

            return new NotificationImplementation(
                    notificationCodeWithDescription, position, title, detailedDescription, detailedMessage);
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
        return Objects.equals(position, that.position)
                && Objects.equals(description, that.description)
                && Objects.equals(title, that.title)
                && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, description, title, message);
    }

    private SeverityLevel mapSeverity(String severityLevel) {
        return SeverityLevel.valueOf(severityLevel);
    }

    private NotificationCategory mapCategory(String category) {
        return NotificationCategory.valueOf(category);
    }

    // Note: this should not be in the public interface until we have decided what name this field/function should have.
    public String getMessage() {
        return message;
    }
}
