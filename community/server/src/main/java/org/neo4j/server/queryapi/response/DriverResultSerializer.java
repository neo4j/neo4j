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
package org.neo4j.server.queryapi.response;

import static org.neo4j.server.queryapi.response.format.Fieldnames.BOOKMARKS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.COUNTERS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.DATA_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.ERRORS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.FIELDS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.NOTIFICATIONS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.PROFILE_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.QUERY_PLAN_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.TRANSACTION_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.TX_EXPIRY_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.TX_ID_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.ResultSummary;

public class DriverResultSerializer {

    private final JsonGenerator jsonGenerator;
    private State currentState = State.ROOT;

    public DriverResultSerializer(JsonGenerator jsonGenerator) {
        this.jsonGenerator = jsonGenerator;
    }

    public void writeRecords(Result result) throws IOException {
        jsonGenerator.writeStartObject();

        if (result != null) {
            writeFieldNames(result.keys());

            while (result.hasNext()) {
                writeValue(result.next());
            }

            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndObject();
        }
    }

    public void writeFieldNames(List<String> keys) throws IOException {
        jsonGenerator.writeFieldName(DATA_KEY);
        jsonGenerator.writeStartObject();
        jsonGenerator.writeArrayFieldStart(FIELDS_KEY);
        for (String key : keys) {
            jsonGenerator.writeString(key);
        }
        jsonGenerator.writeEndArray();
        jsonGenerator.writeArrayFieldStart(VALUES_KEY);
        currentState = State.IN_VALUES;
    }

    public void writeValue(Record record) throws IOException {
        jsonGenerator.writeStartArray();
        jsonGenerator.writeObject(record);
        jsonGenerator.writeEndArray();
    }

    public void writeError(Neo4jException neo4jException) throws IOException {
        ensureResultSetClosedForErrorsWriting();

        jsonGenerator.writeFieldName(ERRORS_KEY);
        jsonGenerator.writeStartArray();
        jsonGenerator.writeObject(neo4jException);
        jsonGenerator.writeEndArray();
        jsonGenerator.writeEndObject();
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

    public void finish(ResultSummary resultSummary, Set<Bookmark> bookmarks, boolean requireCounters)
            throws IOException {
        finish(resultSummary, bookmarks, null, null, requireCounters);
    }

    public void finish(
            ResultSummary resultSummary, Set<Bookmark> bookmarks, String txId, Instant timeout, boolean requireCounters)
            throws IOException {

        writeNotifications(resultSummary.notifications());

        if (requireCounters) {
            writeCounters(resultSummary);
        }

        writeProfile(resultSummary);
        writeQueryPlan(resultSummary);

        writeBookmarks(bookmarks);
        writeTxInfo(txId, timeout);

        jsonGenerator.writeEndObject();
        jsonGenerator.flush();
    }

    private void writeCounters(ResultSummary resultSummary) throws IOException {
        jsonGenerator.writeFieldName(COUNTERS_KEY);
        jsonGenerator.writeObject(resultSummary.counters());
    }

    private void writeProfile(ResultSummary resultSummary) throws IOException {
        if (resultSummary.hasPlan() && resultSummary.hasProfile()) {
            jsonGenerator.writeFieldName(PROFILE_KEY);
            jsonGenerator.writeObject(resultSummary.profile());
        }
    }

    private void writeQueryPlan(ResultSummary resultSummary) throws IOException {
        if (resultSummary.hasPlan() && !resultSummary.hasProfile()) {
            jsonGenerator.writeFieldName(QUERY_PLAN_KEY);
            jsonGenerator.writeObject(resultSummary.plan());
        }
    }

    private void writeBookmarks(Set<Bookmark> bookmarks) throws IOException {
        if (bookmarks != null) {
            jsonGenerator.writeArrayFieldStart(BOOKMARKS_KEY);
            for (Bookmark bookmark : bookmarks) {
                jsonGenerator.writeString(bookmark.value());
            }
            jsonGenerator.writeEndArray();
        }
    }

    private void ensureResultSetClosedForErrorsWriting() throws IOException {
        if (currentState == State.IN_VALUES) {
            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndObject();
        }
    }

    private enum State {
        ROOT,
        IN_VALUES
    }
}
