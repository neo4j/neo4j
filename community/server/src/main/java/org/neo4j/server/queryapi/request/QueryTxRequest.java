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
package org.neo4j.server.queryapi.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;

/**
 * Represents a request to run query while starting a transaction.
 * <p/>
 * This is used by {@link org.neo4j.server.queryapi.QueryResource#beginTransaction(String, QueryRequest, HttpServletRequest, HttpHeaders)}
 * and {@link org.neo4j.server.queryapi.QueryResource#execute(String, QueryRequest, HttpServletRequest, HttpHeaders)}.
 */
public class QueryTxRequest extends QueryRequest {
    private final AccessMode accessMode;
    private final int maxExecutionTime;
    private final List<String> bookmarks;
    private final String impersonatedUser;
    private final String txType;
    private final QueryRequestCypherValues txMetadata;
    private final QueryRequestNotificationsFilter notificationsFilter;

    @JsonCreator
    public QueryTxRequest(
            @JsonProperty("statement") String statement,
            @JsonProperty("parameters") QueryRequestCypherValues parameters,
            @JsonProperty("includeCounters") boolean includeCounters,
            @JsonProperty("accessMode") AccessMode accessMode,
            @JsonProperty("maxExecutionTime") int maxExecutionTime,
            @JsonProperty("bookmarks") List<String> bookmarks,
            @JsonProperty("impersonatedUser") String impersonatedUser,
            @JsonProperty("txType") String txType,
            @JsonProperty("txMetadata") QueryRequestCypherValues txMetadata,
            @JsonProperty("notificationsFilter") QueryRequestNotificationsFilter notificationsFilter) {
        super(statement, parameters, includeCounters);
        this.accessMode = accessMode;
        this.maxExecutionTime = maxExecutionTime;
        this.bookmarks = bookmarks;
        this.impersonatedUser = impersonatedUser;
        this.txType = txType;
        this.txMetadata = txMetadata;
        this.notificationsFilter = notificationsFilter;
    }

    public QueryTxRequest() {
        this(null, null, false, AccessMode.WRITE, 0, List.of(), null, null, null, null);
    }

    public AccessMode accessMode() {
        return accessMode;
    }

    public int maxExecutionTime() {
        return maxExecutionTime;
    }

    public List<String> bookmarks() {
        return bookmarks;
    }

    public String impersonatedUser() {
        return impersonatedUser;
    }

    public String txType() {
        return txType;
    }

    public QueryRequestCypherValues txMetadata() {
        return txMetadata;
    }

    public QueryRequestNotificationsFilter notificationsFilter() {
        return notificationsFilter;
    }

    public Optional<Map<String, Object>> maybeTxMetadata() {
        return Optional.ofNullable(txMetadata).map(QueryRequestCypherValues::values);
    }

    public Optional<QueryRequestNotificationsFilter> maybeNotificationsFilter() {
        return Optional.ofNullable(notificationsFilter);
    }
}
