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

import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_EVENT_ERROR;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_EVENT_HEADER;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_EVENT_RECORD;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_EVENT_SUMMARY;

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
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.server.queryapi.response.error.HttpErrorResponse;

public class QueryBodyFormatter {
    private final DriverResultSerializer serializer;
    private State state;

    public QueryBodyFormatter(JsonGenerator jsonGenerator, OutputStream outputStream) {
        this.serializer = new DriverResultSerializer(jsonGenerator, outputStream);
        this.state = State.CREATED;
    }

    public boolean json(FormatterConsumer<JsonBodyFormatter> consumer) throws IOException {
        accessState();
        var jsonBodyFormatter = new JsonBodyFormatter(this.serializer);
        return this.serializer.write(() -> consumer.accept(jsonBodyFormatter));
    }

    public boolean jsonl(FormatterConsumer<JsonLinesFormatter> consumer) throws IOException {
        accessState();
        var jsonBodyFormatter = new JsonLinesFormatter(this.serializer);
        return this.serializer.writeEvents(() -> consumer.accept(jsonBodyFormatter));
    }

    private void accessState() {
        if (this.state != State.CREATED) {
            throw new IllegalStateException(
                    "This method is called only once and can't be used in combination with others");
        }
        state = State.USED;
    }

    public static class JsonBodyFormatter {
        private final DriverResultSerializer serializer;

        private JsonBodyFormatter(DriverResultSerializer serializer) {
            this.serializer = serializer;
        }

        public void data(Result result) throws IOException {
            this.serializer.writeData(result);
        }

        public void metadata(
                ResultSummary resultSummary,
                Long resultAvailableAfter,
                Long resultConsumedAfter,
                Set<Bookmark> bookmarks,
                boolean requireCounters)
                throws IOException {
            metadata(resultSummary, resultAvailableAfter, resultConsumedAfter, bookmarks, null, null, requireCounters);
        }

        public void metadata(ResultSummary resultSummary, Set<Bookmark> bookmarks, boolean requireCounters)
                throws IOException {
            metadata(resultSummary, null, null, bookmarks, null, null, requireCounters);
        }

        public void metadata(
                ResultSummary resultSummary,
                Long resultAvailableAfter,
                Long resultConsumedAfter,
                Set<Bookmark> bookmarks,
                String txId,
                Instant timeout,
                boolean requireCounters)
                throws IOException {
            this.serializer.writeMetadata(
                    resultSummary,
                    resultAvailableAfter,
                    resultConsumedAfter,
                    bookmarks,
                    txId,
                    timeout,
                    requireCounters);
        }
    }

    public static class JsonLinesFormatter {
        private final DriverResultSerializer serializer;

        private JsonLinesFormatter(DriverResultSerializer serializer) {
            this.serializer = serializer;
        }

        public void header() throws IOException {
            header(null);
        }

        public void header(List<String> keys) throws IOException {
            this.serializer.writeEvent(CYPHER_EVENT_HEADER, () -> {
                this.serializer.object(() -> {
                    this.serializer.writeFieldNames(keys);
                });
            });
        }

        public void record(Record record) throws IOException {
            this.serializer.writeEvent(CYPHER_EVENT_RECORD, () -> {
                this.serializer.writeValue(record);
            });
        }

        public void summary(
                ResultSummary resultSummary,
                Long resultAvailableAfter,
                Long resultConsumedAfter,
                Set<Bookmark> bookmarks,
                boolean requireCounters)
                throws IOException {
            this.summary(
                    resultSummary, resultAvailableAfter, resultConsumedAfter, bookmarks, null, null, requireCounters);
        }

        public void summary(
                ResultSummary resultSummary,
                Long resultAvailableAfter,
                Long resultConsumedAfter,
                Set<Bookmark> bookmarks,
                String txId,
                Instant timeout,
                boolean requireCounters)
                throws IOException {
            this.serializer.writeEvent(CYPHER_EVENT_SUMMARY, () -> {
                this.serializer.object(() -> {
                    this.serializer.writeNotifications(resultSummary.notifications());
                    this.serializer.writeCounters(resultSummary, requireCounters);
                    this.serializer.writeProfile(resultSummary);
                    this.serializer.writeQueryPlan(resultSummary);
                    this.serializer.writeBookmarks(bookmarks);
                    this.serializer.writeQueryType(resultSummary);
                    this.serializer.writeResultAvailableAfter(resultAvailableAfter);
                    this.serializer.writeResultConsumeAfter(resultConsumedAfter);
                    this.serializer.writeTxInfo(txId, timeout);
                });
            });
        }

        public void summary(Collection<Bookmark> bookmarks) throws IOException {
            this.summary(bookmarks, null, null);
        }

        public void summary(String txId, Instant timeout) throws IOException {
            this.summary(null, txId, timeout);
        }

        public void summary(Collection<Bookmark> bookmarks, String txId, Instant timeout) throws IOException {
            this.serializer.writeEvent(CYPHER_EVENT_SUMMARY, () -> {
                this.serializer.object(() -> {
                    this.serializer.writeBookmarks(bookmarks);
                    this.serializer.writeTxInfo(txId, timeout);
                });
            });
        }

        public void error(HttpErrorResponse error) throws IOException {
            error(serializer, error);
        }

        static void error(DriverResultSerializer serializer, HttpErrorResponse error) throws IOException {
            serializer.writeEvent(CYPHER_EVENT_ERROR, () -> {
                serializer.writeError(error);
            });
        }
    }

    public interface FormatterConsumer<T> {
        void accept(T formatter) throws IOException;
    }

    private enum State {
        CREATED,
        USED,
    }
}
