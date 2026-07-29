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
package org.neo4j.queryapi.testclient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.server.queryapi.request.AccessMode;

public record QueryRequest(
        String statement,
        Map<String, Object> parameters,
        Boolean includeCounters,
        AccessMode accessMode,
        Integer maxExecutionTime,
        List<String> bookmarks,
        String impersonatedUser,
        String txType,
        Map<String, Object> txMetadata,
        NotificationsFilter notificationsFilter) {

    public static class Builder {
        private String statement;
        private Map<String, Object> parameters = new HashMap<>();
        private Boolean includeCounters;
        private AccessMode accessMode;
        private Integer maxExecutionTime;
        private List<String> bookmarks;
        private String impersonatedUser;
        String txType;
        private Map<String, Object> txMetadata;
        private NotificationsFilter notificationsFilter;

        public Builder statement(String statement) {
            this.statement = statement;
            return this;
        }

        public Builder parameters(Map<String, Object> parameters) {
            this.parameters = parameters;
            return this;
        }

        public Builder includeCounters() {
            this.includeCounters = true;
            return this;
        }

        public Builder withoutCounters() {
            this.includeCounters = false;
            return this;
        }

        public Builder accessMode(AccessMode accessMode) {
            this.accessMode = accessMode;
            return this;
        }

        public Builder maxExecutionTime(int maxExecutionTime) {
            this.maxExecutionTime = maxExecutionTime;
            return this;
        }

        public Builder bookmarks(List<String> bookmarks) {
            this.bookmarks = bookmarks;
            return this;
        }

        public Builder impersonatedUser(String impersonatedUser) {
            this.impersonatedUser = impersonatedUser;
            return this;
        }

        public Builder txType(String txType) {
            this.txType = txType;
            return this;
        }

        public Builder txMetadata(Map<String, Object> txMetadata) {
            this.txMetadata = txMetadata;
            return this;
        }

        public Builder notificationsFilter(NotificationsFilter notificationsFilter) {
            this.notificationsFilter = notificationsFilter;
            return this;
        }

        public QueryRequest build() {
            return new QueryRequest(
                    statement,
                    parameters,
                    includeCounters,
                    accessMode,
                    maxExecutionTime,
                    bookmarks,
                    impersonatedUser,
                    txType,
                    txMetadata,
                    notificationsFilter);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static QueryRequest returnOne() {
        return newBuilder().statement("RETURN 1").build();
    }

    public record NotificationsFilter(String minimumSeverityLevel, String[] disabledCategories) {}

    public static class NotificationsFilterBuilder {
        private String minimumSeverityLevel;
        private String[] disabledCategories;

        public NotificationsFilterBuilder minimumSeverityLevel(String minimumSeverityLevel) {
            this.minimumSeverityLevel = minimumSeverityLevel;
            return this;
        }

        public NotificationsFilterBuilder disabledCategories(String[] disabledCategories) {
            this.disabledCategories = disabledCategories;
            return this;
        }

        public static NotificationsFilterBuilder newBuilder() {
            return new NotificationsFilterBuilder();
        }

        public static NotificationsFilterBuilder newBuilderWithMinimumSeverityLevel(String minimumSeverityLevel) {
            return newBuilder().minimumSeverityLevel(minimumSeverityLevel);
        }

        public static NotificationsFilterBuilder newBuilderWithDisabledCategories(String... disabledCategories) {
            return newBuilder().disabledCategories(disabledCategories);
        }

        public NotificationsFilter build() {
            return new NotificationsFilter(minimumSeverityLevel, disabledCategories);
        }
    }
}
