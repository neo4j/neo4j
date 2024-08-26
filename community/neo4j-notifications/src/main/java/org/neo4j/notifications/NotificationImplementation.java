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
import org.neo4j.gqlstatus.CommonGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.DiagnosticRecord;
import org.neo4j.gqlstatus.GqlStatusInfo;
import org.neo4j.gqlstatus.NotificationClassification;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.NotificationCategory;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.api.exceptions.Status;

public final class NotificationImplementation extends CommonGqlStatusObjectImplementation
        implements Notification, GqlStatusObject {
    private final Status neo4jStatus;
    private final String title;
    private final String descriptionWithParameters;
    private final InputPosition position;

    NotificationImplementation(
            Status neo4jStatus,
            GqlStatusInfo gqlStatusInfo,
            DiagnosticRecord diagnosticRecord,
            InputPosition position,
            String title,
            String descriptionWithParameters,
            Object[] messageParameterValues) {

        super(gqlStatusInfo, diagnosticRecord, messageParameterValues);

        // Legacy parts of the notification API
        this.neo4jStatus = neo4jStatus;
        this.position = position;
        this.title = title;
        this.descriptionWithParameters = descriptionWithParameters;
    }

    public static class NotificationBuilder {
        private final NotificationCodeWithDescription notificationCodeWithDescription;
        private final GqlStatusInfo gqlStatusInfo;
        private Object[] messageParameterValues = new String[] {};
        private String title;
        private InputPosition position;
        private String[] notificationDetails;

        public NotificationBuilder(NotificationCodeWithDescription notificationCodeWithDescription) {
            this.notificationCodeWithDescription = notificationCodeWithDescription;
            this.gqlStatusInfo = notificationCodeWithDescription.getGqlStatusInfo();
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
            this.notificationDetails = details;
            return this;
        }

        public NotificationBuilder setMessageParameters(Object[] messageParameterValues) {
            this.messageParameterValues = messageParameterValues;
            return this;
        }

        public NotificationImplementation build() {
            final var detailedDescription = notificationCodeWithDescription.getDescription(notificationDetails);

            final String severity;
            final NotificationClassification classification;

            var statusCode = notificationCodeWithDescription.getStatus().code();
            if (statusCode instanceof Status.NotificationCode notificationCode) {
                severity = notificationCode.getSeverity();
                classification = NotificationClassification.valueOf(notificationCode.getNotificationCategory());
            } else {
                throw new IllegalStateException("'" + statusCode + "' is not a notification code.");
            }

            var diagnosticRecord = new DiagnosticRecord(
                    severity, classification, position.getOffset(), position.getLine(), position.getColumn());

            return new NotificationImplementation(
                    notificationCodeWithDescription.getStatus(),
                    gqlStatusInfo,
                    diagnosticRecord,
                    position,
                    title,
                    detailedDescription,
                    messageParameterValues);
        }
    }

    public Status getNeo4jStatus() {
        return neo4jStatus;
    }

    @Override
    public String getCode() {
        return neo4jStatus.code().serialize();
    }

    @Override
    public String getTitle() {
        return title;
    }

    @Override
    public String getDescription() {
        return descriptionWithParameters;
    }

    @Override
    public InputPosition getPosition() {
        return position;
    }

    @Override
    public NotificationCategory getCategory() {
        return mapCategory(diagnosticRecord.asMap().get("_classification").toString());
    }

    @Override
    public NotificationClassification getClassification() {
        return mapClassification(diagnosticRecord.asMap().get("_classification").toString());
    }

    @Override
    public SeverityLevel getSeverity() {
        return mapSeverity(diagnosticRecord.asMap().get("_severity").toString());
    }

    @Override
    public String toString() {
        return "Notification{" + "position=" + position + ", description='" + descriptionWithParameters + '\'' + '}';
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
                && Objects.equals(descriptionWithParameters, that.descriptionWithParameters)
                && Objects.equals(gqlStatusInfo, that.gqlStatusInfo)
                && Objects.equals(title, that.title)
                && Objects.equals(diagnosticRecord, that.diagnosticRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, descriptionWithParameters, gqlStatusInfo, title, diagnosticRecord);
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
}
