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

import java.util.Map;
import java.util.Objects;
import org.neo4j.gqlstatus.Condition;
import org.neo4j.gqlstatus.DiagnosticRecord;
import org.neo4j.gqlstatus.GqlStatus;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.NotificationCategory;
import org.neo4j.graphdb.NotificationClassification;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.api.exceptions.Status;

public final class NotificationImplementation implements Notification {
    private final Status.Code statusCode;
    private final GqlStatus gqlStatus;
    private final DiagnosticRecord diagnosticRecord;
    private final String title;
    private final String description;
    private final InputPosition position;
    private final String message;
    private final String severity;
    private final String subCondition;
    private final Condition condition;

    NotificationImplementation(
            NotificationCodeWithDescription notificationCodeWithDescription,
            GqlStatus gqlStatus,
            InputPosition position,
            String title,
            String description,
            String message,
            String subCondition,
            Condition condition) {

        this.statusCode = notificationCodeWithDescription.getStatus().code();

        String classification;
        this.subCondition = subCondition;
        this.condition = condition;

        if (statusCode instanceof Status.NotificationCode) {
            this.severity = ((Status.NotificationCode) statusCode).getSeverity();
            classification = ((Status.NotificationCode) statusCode).getNotificationCategory();
        } else {
            throw new IllegalStateException("'" + statusCode + "' is not a notification code.");
        }

        this.diagnosticRecord = new DiagnosticRecord(
                this.severity,
                classification,
                position.getOffset(),
                position.getLine(),
                position.getColumn(),
                Map.of());
        this.gqlStatus = gqlStatus;
        this.position = position;
        this.title = title;
        this.description = description;
        this.message = message;
    }

    public static class NotificationBuilder {
        private final NotificationCodeWithDescription notificationCodeWithDescription;
        private final GqlStatus gqlStatus;
        private String title;
        private final String description;
        private InputPosition position;
        private final String message;
        private final String subCondition;
        private final Condition condition;
        private String[] messageParameters;
        private String[] notificationDetails;

        public NotificationBuilder(NotificationCodeWithDescription notificationCodeWithDescription) {
            this.notificationCodeWithDescription = notificationCodeWithDescription;
            this.gqlStatus = notificationCodeWithDescription.getGqlStatus();
            this.description = notificationCodeWithDescription.getDescription();
            this.title = notificationCodeWithDescription.getStatus().code().description();
            this.position = InputPosition.empty;
            this.message = notificationCodeWithDescription.getMessage();
            this.subCondition = notificationCodeWithDescription.getSubCondition();
            this.condition = notificationCodeWithDescription.getCondition();
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
                    notificationCodeWithDescription,
                    gqlStatus,
                    position,
                    title,
                    detailedDescription,
                    detailedMessage,
                    subCondition,
                    condition);
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
        return mapCategory(diagnosticRecord.asMap().get("_classification").toString());
    }

    public NotificationClassification getClassification() {
        return mapClassification(diagnosticRecord.asMap().get("_classification").toString());
    }

    @Override
    public SeverityLevel getSeverity() {
        return mapSeverity(diagnosticRecord.asMap().get("_severity").toString());
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
                && Objects.equals(message, that.message)
                && Objects.equals(gqlStatus, that.gqlStatus)
                && Objects.equals(diagnosticRecord, that.diagnosticRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, description, title, message, gqlStatus, diagnosticRecord);
    }

    private SeverityLevel mapSeverity(String severityLevel) {
        return SeverityLevel.valueOf(severityLevel);
    }

    private NotificationCategory mapCategory(String category) {
        return NotificationCategory.valueOf(category);
    }

    private NotificationClassification mapClassification(String classification) {
        return NotificationClassification.valueOf(classification);
    }

    // Note: this should not be in the public interface until we have decided what name this field/function should have.
    public String getMessage() {
        return message;
    }

    public String getGqlStatus() {
        return gqlStatus.gqlStatusString();
    }

    public Map<String, Object> getDiagnosticRecord() {
        return diagnosticRecord.asMap();
    }

    public String getStatusDescription() {

        return (conditionToDescription(this.condition) + this.subCondition + ". " + this.message);
    }

    private String conditionToDescription(Condition condition) {
        return switch (condition) {
            case WARNING -> "warn: ";
            case INFORMATION -> "info: ";
            case SUCCESSFUL_COMPLETION -> "note: successful completion - ";
        };
    }
}
