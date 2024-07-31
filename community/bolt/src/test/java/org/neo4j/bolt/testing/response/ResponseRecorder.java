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
package org.neo4j.bolt.testing.response;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.neo4j.bolt.protocol.common.fsm.response.AbstractMetadataAwareResponseHandler;
import org.neo4j.bolt.protocol.common.fsm.response.RecordHandler;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.MetadataHandler;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.response.FailureMessage;
import org.neo4j.bolt.protocol.common.message.response.IgnoredMessage;
import org.neo4j.bolt.protocol.common.message.response.SuccessMessage;
import org.neo4j.bolt.protocol.v44.fsm.response.metadata.MetadataHandlerV44;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValueBuilder;

public class ResponseRecorder extends AbstractMetadataAwareResponseHandler {
    private BlockingQueue<RecordedMessage> messages;

    private MapValueBuilder metadataBuilder;
    private RecordingRecordHandler recordHandler;

    public ResponseRecorder(MetadataHandler metadataHandler) {
        super(metadataHandler);

        reset();
    }

    public ResponseRecorder() {
        this(MetadataHandlerV44.getInstance());
    }

    public void reset() {
        this.messages = new LinkedBlockingQueue<>();
        this.resetState();
    }

    private void resetState() {
        this.metadataBuilder = new MapValueBuilder();
        this.recordHandler = null;
    }

    public RecordedMessage next() throws InterruptedException {
        var response = messages.poll(3, SECONDS);
        assertNotNull(response, "No message arrived after 3s");

        return response;
    }

    @Override
    public RecordHandler onBeginStreaming(List<String> fieldNames) {
        return this.recordHandler = new RecordingRecordHandler(fieldNames.size());
    }

    @Override
    public void onCompleteStreaming(boolean hasRemaining) {
        super.onCompleteStreaming(hasRemaining);

        this.messages.addAll(this.recordHandler.records);
    }

    @Override
    public void onFailure(Error error) {
        messages.add(new RecordedResponseMessage(
                new FailureMessage(error.status(), error.message(), error.isFatal()), new IllegalStateException()));

        this.resetState();
    }

    @Override
    public void onIgnored() {
        messages.add(new RecordedResponseMessage(IgnoredMessage.INSTANCE, new IllegalStateException()));
        this.resetState();
    }

    @Override
    public void onSuccess() {
        messages.add(new RecordedResponseMessage(
                new SuccessMessage(this.metadataBuilder.build()), new IllegalStateException()));
        this.resetState();
    }

    @Override
    public void onMetadata(String key, AnyValue value) {
        this.metadataBuilder.add(key, value);
    }

    public int remaining() {
        return messages.size();
    }

    private static class RecordingRecordHandler implements RecordHandler {
        private final List<RecordedRecordMessage> records = new ArrayList<>();

        private AnyValue[] fields;
        private int fieldIndex;

        public RecordingRecordHandler(int numberOfFields) {
            this.fields = new AnyValue[numberOfFields];
        }

        @Override
        public void onField(AnyValue value) {
            this.fields[this.fieldIndex++] = value;
        }

        @Override
        public void onCompleted() {
            this.records.add(new RecordedRecordMessage(this.fields, new IllegalStateException()));

            this.fields = new AnyValue[this.fields.length];
            this.fieldIndex = 0;
        }

        @Override
        public void onFailure() {}
    }
}
