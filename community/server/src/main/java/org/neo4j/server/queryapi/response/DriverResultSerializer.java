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
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.server.queryapi.request.QueryRequest;

public class DriverResultSerializer {

    private final JsonGenerator jsonGenerator;
    private State currentState = State.ROOT;

    public DriverResultSerializer(JsonGenerator jsonGenerator) {
        this.jsonGenerator = jsonGenerator;
    }

    public void writeFieldNames(List<String> keys) throws IOException {
        jsonGenerator.writeStartObject();
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

    public void finish(ResultSummary resultSummary, Set<Bookmark> bookmarks, QueryRequest queryRequest)
            throws IOException {
        jsonGenerator.writeEndArray();
        jsonGenerator.writeEndObject();

        writeNotifications(resultSummary.notifications());

        if (queryRequest.includeCounters()) {
            writeCounters(resultSummary.counters());
        }

        if (resultSummary.hasPlan() && resultSummary.hasProfile()) {
            jsonGenerator.writeFieldName(PROFILE_KEY);
            jsonGenerator.writeObject(resultSummary.profile());
        }

        if (resultSummary.hasPlan() && !resultSummary.hasProfile()) {
            jsonGenerator.writeFieldName(QUERY_PLAN_KEY);
            jsonGenerator.writeObject(resultSummary.plan());
        }

        jsonGenerator.writeArrayFieldStart(BOOKMARKS_KEY);

        for (Bookmark bookmark : bookmarks) {
            jsonGenerator.writeString(bookmark.value());
        }

        jsonGenerator.writeEndArray();
        jsonGenerator.writeEndObject();
        jsonGenerator.flush();
    }

    private void writeCounters(SummaryCounters counters) throws IOException {
        jsonGenerator.writeFieldName(COUNTERS_KEY);
        jsonGenerator.writeObject(counters);
    }

    private void ensureResultSetClosedForErrorsWriting() throws IOException {
        if (currentState == State.IN_VALUES) {
            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndObject();
        }
    }

    private enum State {
        ROOT,
        IN_VALUES
    }
}
