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
package org.neo4j.server.queryapi.response.format;

import static org.neo4j.server.queryapi.response.format.Fieldnames.BOOKMARKS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.COUNTERS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_BODY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_EVENT;
import static org.neo4j.server.queryapi.response.format.Fieldnames.DATA_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.ERRORS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.FIELDS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.NOTIFICATIONS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.PROFILE_CHILDREN_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.PROFILE_DB_HITS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.PROFILE_HAS_PAGE_CACHE_STATS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.PROFILE_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.PROFILE_PAGE_CACHE_HITS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.PROFILE_PAGE_CACHE_MISSES_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.PROFILE_PAGE_CACHE_RATION_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.PROFILE_ROWS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.PROFILE_TIME_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.QUERY_PLAN_ARGUMENTS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.QUERY_PLAN_CHILDREN_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.QUERY_PLAN_IDENTIFIERS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.QUERY_PLAN_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.QUERY_PLAN_OPERATOR_TYPE_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.QUERY_TYPE_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.RESULT_AVAILABLE_AFTER_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.RESULT_CONSUMED_AFTER_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.TRANSACTION_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.TX_EXPIRY_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.TX_ID_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.QueryProfile;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.server.queryapi.exception.ExceptionsUnwrapper;
import org.neo4j.server.queryapi.exception.QueryApiException;
import org.neo4j.server.queryapi.response.error.HttpErrorResponse;

class DriverResultSerializer {

    private final JsonGenerator jsonGenerator;
    private final OutputStream outputStream;

    public DriverResultSerializer(JsonGenerator jsonGenerator, OutputStream outputStream) {
        this.jsonGenerator = jsonGenerator;
        this.outputStream = outputStream;
    }

    public boolean write(RunnableSerialization runnable) throws IOException {
        try {
            HttpErrorResponse errorResponse;
            try {
                jsonGenerator.writeStartObject();
                runnable.run();
                return true;
            } catch (IOException ex) {
                errorResponse = ExceptionsUnwrapper.transformNeo4jAndQueryApiExceptions(
                        HttpErrorResponse::fromDriverException, HttpErrorResponse::fromQueryApiException, ex);
            } catch (Neo4jException neo4jException) {
                errorResponse = HttpErrorResponse.fromDriverException(neo4jException);
            } catch (QueryApiException queryApiException) {
                errorResponse = HttpErrorResponse.fromQueryApiException(queryApiException);
            }
            if (errorResponse != null) {
                jsonGenerator.writeFieldName(ERRORS_KEY);
                writeError(errorResponse);
            }
            return false;
        } finally {
            jsonGenerator.writeEndObject();
            jsonGenerator.flush();
        }
    }

    public boolean writeEvents(RunnableSerialization runnable) throws IOException {
        HttpErrorResponse errorResponse;
        try {
            runnable.run();
            return true;
        } catch (IOException ex) {
            errorResponse = ExceptionsUnwrapper.transformNeo4jAndQueryApiExceptions(
                    HttpErrorResponse::fromDriverException, HttpErrorResponse::fromQueryApiException, ex);
        } catch (Neo4jException neo4jException) {
            errorResponse = HttpErrorResponse.fromDriverException(neo4jException);
        } catch (QueryApiException queryApiException) {
            errorResponse = HttpErrorResponse.fromQueryApiException(queryApiException);
        }
        if (errorResponse != null) {
            QueryBodyFormatter.JsonLinesFormatter.error(this, errorResponse);
        }
        return false;
    }

    public void writeEvent(String event, RunnableSerialization runnable) throws IOException {
        try {
            object(() -> {
                jsonGenerator.writeStringField(CYPHER_EVENT, event);
                jsonGenerator.writeFieldName(CYPHER_BODY);
                runnable.run();
            });
        } finally {
            jsonGenerator.flush();
            outputStream.write("\n".getBytes());
            outputStream.flush();
        }
    }

    public void writeData(Result result) throws IOException {
        if (result != null) {
            object(DATA_KEY, () -> {
                writeFieldNames(result.keys());
                writeValues(result);
            });
        }
    }

    public void writeFieldNames(List<String> keys) throws IOException {
        if (keys == null) {
            return;
        }
        array(FIELDS_KEY, () -> {
            for (String key : keys) {
                jsonGenerator.writeString(key);
            }
        });
    }

    public void writeValues(Result result) throws IOException {
        array(VALUES_KEY, () -> {
            while (result.hasNext()) {
                writeValue(result.next());
            }
        });
    }

    public void writeValue(Record record) throws IOException {
        array(() -> jsonGenerator.writeObject(record));
    }

    public void writeNotifications(List<Notification> notifications) throws IOException {
        if (!notifications.isEmpty()) {
            jsonGenerator.writeFieldName(NOTIFICATIONS_KEY);
            jsonGenerator.writeObject(notifications);
        }
    }

    public void writeTxInfo(String txId, Instant timeout) throws IOException {
        if (txId != null && timeout != null) {
            jsonGenerator.writeObjectFieldStart(TRANSACTION_KEY);
            jsonGenerator.writeStringField(TX_ID_KEY, txId);
            jsonGenerator.writeStringField(TX_EXPIRY_KEY, timeout.toString());
            jsonGenerator.writeEndObject();
        }
    }

    public void writeMetadata(
            ResultSummary resultSummary,
            Long resultAvailableAfter,
            Long resultConsumedAfter,
            Set<Bookmark> bookmarks,
            String txId,
            Instant timeout,
            boolean requireCounters)
            throws IOException {
        if (resultSummary != null) {
            writeNotifications(resultSummary.notifications());
            writeCounters(resultSummary, requireCounters);
            writeProfile(resultSummary);
            writeQueryPlan(resultSummary);
            writeQueryType(resultSummary);
        }
        writeResultAvailableAfter(resultAvailableAfter);
        writeResultConsumeAfter(resultConsumedAfter);
        writeBookmarks(bookmarks);
        writeTxInfo(txId, timeout);
    }

    public void writeResultConsumeAfter(Long resultConsumedAfter) throws IOException {
        if (resultConsumedAfter != null) {
            jsonGenerator.writeFieldName(RESULT_CONSUMED_AFTER_KEY);
            jsonGenerator.writeNumber(resultConsumedAfter);
        }
    }

    public void writeResultAvailableAfter(Long resultAvailableAfter) throws IOException {
        if (resultAvailableAfter != null) {
            jsonGenerator.writeFieldName(RESULT_AVAILABLE_AFTER_KEY);
            jsonGenerator.writeNumber(resultAvailableAfter);
        }
    }

    public void writeQueryType(ResultSummary resultSummary) throws IOException {
        var queryType =
                switch (resultSummary.queryType()) {
                    case QueryType.READ_ONLY -> "r";
                    case QueryType.READ_WRITE -> "rw";
                    case QueryType.WRITE_ONLY -> "w";
                    case QueryType.SCHEMA_WRITE -> "s";
                };

        jsonGenerator.writeFieldName(QUERY_TYPE_KEY);
        jsonGenerator.writeString(queryType);
    }

    public void writeCounters(ResultSummary resultSummary, boolean requireCounters) throws IOException {
        if (requireCounters) {
            jsonGenerator.writeFieldName(COUNTERS_KEY);
            jsonGenerator.writeObject(resultSummary.counters());
        }
    }

    public void writeProfile(ResultSummary resultSummary) throws IOException {
        if (resultSummary.queryProfile().isPresent()) {
            jsonGenerator.writeFieldName(PROFILE_KEY);
            writeProfile(resultSummary.queryProfile().get());
        }
    }

    private void writeProfile(QueryProfile queryProfile) throws IOException {
        object(() -> {
            writeQueryPlanFieldsWithoutChildren(queryProfile);

            if (queryProfile.dbHits().isPresent()) {
                jsonGenerator.writeFieldName(PROFILE_DB_HITS_KEY);
                jsonGenerator.writeNumber(queryProfile.dbHits().getAsLong());
            }

            if (queryProfile.rows().isPresent()) {
                jsonGenerator.writeFieldName(PROFILE_ROWS_KEY);
                jsonGenerator.writeNumber(queryProfile.rows().getAsLong());
            }

            // Follows the same logic on the ProfiledPlan method on the JavaDriver.
            var hasPageCacheStats = false;

            if (queryProfile.pageCacheHits().isPresent()) {
                jsonGenerator.writeFieldName(PROFILE_PAGE_CACHE_HITS_KEY);
                jsonGenerator.writeNumber(queryProfile.pageCacheHits().getAsLong());
                hasPageCacheStats = queryProfile.pageCacheHits().getAsLong() > 0;
            }

            if (queryProfile.pageCacheMisses().isPresent()) {
                jsonGenerator.writeFieldName(PROFILE_PAGE_CACHE_MISSES_KEY);
                jsonGenerator.writeNumber(queryProfile.pageCacheMisses().getAsLong());
                hasPageCacheStats =
                        hasPageCacheStats || queryProfile.pageCacheMisses().getAsLong() > 0;
            }

            if (queryProfile.pageCacheHitRatio().isPresent()) {
                jsonGenerator.writeFieldName(PROFILE_PAGE_CACHE_RATION_KEY);
                jsonGenerator.writeNumber(queryProfile.pageCacheHitRatio().getAsDouble());
                hasPageCacheStats =
                        hasPageCacheStats || queryProfile.pageCacheHitRatio().getAsDouble() > 0;
            }

            jsonGenerator.writeFieldName(PROFILE_HAS_PAGE_CACHE_STATS_KEY);
            jsonGenerator.writeBoolean(hasPageCacheStats);

            if (queryProfile.time().isPresent()) {
                jsonGenerator.writeFieldName(PROFILE_TIME_KEY);
                jsonGenerator.writeNumber(queryProfile.time().get().toNanos());
            }

            array(PROFILE_CHILDREN_KEY, () -> {
                for (var child : queryProfile.children()) {
                    writeProfile(child);
                }
            });
        });
    }

    public void writeQueryPlan(ResultSummary resultSummary) throws IOException {
        if (resultSummary.queryProfile().isEmpty() && resultSummary.queryPlan().isPresent()) {
            jsonGenerator.writeFieldName(QUERY_PLAN_KEY);
            writeQueryPlan(resultSummary.queryPlan().get());
        }
    }

    private void writeQueryPlan(Plan queryPlan) throws IOException {
        object(() -> {
            writeQueryPlanFieldsWithoutChildren(queryPlan);
            array(QUERY_PLAN_CHILDREN_KEY, () -> {
                for (var child : queryPlan.children()) {
                    writeQueryPlan(child);
                }
            });
        });
    }

    private void writeQueryPlanFieldsWithoutChildren(Plan queryPlan) throws IOException {
        jsonGenerator.writeFieldName(QUERY_PLAN_OPERATOR_TYPE_KEY);
        jsonGenerator.writeString(queryPlan.operatorType());

        jsonGenerator.writeFieldName(QUERY_PLAN_ARGUMENTS_KEY);
        jsonGenerator.writeObject(queryPlan.arguments());

        jsonGenerator.writeFieldName(QUERY_PLAN_IDENTIFIERS_KEY);
        jsonGenerator.writeObject(queryPlan.identifiers());
    }

    public void writeBookmarks(Collection<Bookmark> bookmarks) throws IOException {
        if (bookmarks != null) {
            array(BOOKMARKS_KEY, () -> {
                for (Bookmark bookmark : bookmarks) {
                    jsonGenerator.writeString(bookmark.value());
                }
            });
        }
    }

    public void writeError(HttpErrorResponse errorResponse) throws IOException {
        array(() -> {
            for (var error : errorResponse.errors()) {
                jsonGenerator.writeObject(error);
            }
        });
    }

    public void object(RunnableSerialization runnable) throws IOException {
        try {
            jsonGenerator.writeStartObject();
            runnable.run();
        } finally {
            jsonGenerator.writeEndObject();
        }
    }

    private void object(String fieldName, RunnableSerialization runnable) throws IOException {
        try {
            jsonGenerator.writeObjectFieldStart(fieldName);
            runnable.run();
        } finally {
            jsonGenerator.writeEndObject();
        }
    }

    private void array(RunnableSerialization runnable) throws IOException {
        try {
            jsonGenerator.writeStartArray();
            runnable.run();
        } finally {
            jsonGenerator.writeEndArray();
        }
    }

    private void array(String fieldName, RunnableSerialization runnable) throws IOException {
        try {
            jsonGenerator.writeArrayFieldStart(fieldName);
            runnable.run();
        } finally {
            jsonGenerator.writeEndArray();
        }
    }

    public interface RunnableSerialization {
        void run() throws IOException;
    }
}
