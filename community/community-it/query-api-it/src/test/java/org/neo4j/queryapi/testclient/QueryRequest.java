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
        boolean includeCounters,
        AccessMode accessMode,
        int maxExecutionTime,
        List<String> bookmarks,
        String impersonatedUser,
        String txType) {

    public static class Builder {
        private String statement;
        private Map<String, Object> parameters = new HashMap<>();
        private boolean includeCounters;
        private AccessMode accessMode = AccessMode.WRITE;
        private int maxExecutionTime;
        private List<String> bookmarks = List.of();
        private String impersonatedUser;
        String txType;

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

        public QueryRequest build() {
            return new QueryRequest(
                    statement,
                    parameters,
                    includeCounters,
                    accessMode,
                    maxExecutionTime,
                    bookmarks,
                    impersonatedUser,
                    txType);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static QueryRequest returnOne() {
        return newBuilder().statement("RETURN 1").build();
    }
}
