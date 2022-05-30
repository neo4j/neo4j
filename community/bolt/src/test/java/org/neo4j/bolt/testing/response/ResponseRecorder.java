/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.testing.response;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.response.FailureMessage;
import org.neo4j.bolt.protocol.common.message.response.IgnoredMessage;
import org.neo4j.bolt.protocol.common.message.response.ResponseMessage;
import org.neo4j.bolt.protocol.common.message.response.SuccessMessage;
import org.neo4j.bolt.protocol.common.message.result.BoltResult;
import org.neo4j.bolt.protocol.common.message.result.ResponseHandler;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.virtual.MapValueBuilder;

public class ResponseRecorder implements ResponseHandler {
    private BlockingQueue<RecordedMessage> messages;

    private MapValueBuilder metadataBuilder;
    private ResponseMessage currentResponse;

    public ResponseRecorder() {
        reset();
    }

    public void reset() {
        this.messages = new LinkedBlockingQueue<>();
        this.resetState();
    }

    private void resetState() {
        this.metadataBuilder = new MapValueBuilder();
        this.currentResponse = null;
    }

    public RecordedMessage next() throws InterruptedException {
        var response = messages.poll(3, SECONDS);
        assertNotNull(response, "No message arrived after 3s");

        return response;
    }

    @Override
    public boolean onPullRecords(BoltResult result, long size) throws Throwable {
        return hasMore(result.handleRecords(new RecordingBoltResultRecordConsumer(), size));
    }

    @Override
    public boolean onDiscardRecords(BoltResult result, long size) throws Throwable {
        return hasMore(result.handleRecords(new DiscardingBoltResultVisitor(), size));
    }

    @Override
    public void onMetadata(String key, AnyValue value) {
        this.metadataBuilder.add(key, value);
    }

    @Override
    public void markIgnored() {
        this.currentResponse = IgnoredMessage.INSTANCE;
    }

    @Override
    public void markFailed(Error error) {
        this.currentResponse = new FailureMessage(error.status(), error.message(), error.isFatal());
    }

    @Override
    public void onFinish() {
        var response = this.currentResponse;
        if (this.currentResponse == null) {
            response = new SuccessMessage(this.metadataBuilder.build());
        }

        this.messages.add(new RecordedResponseMessage(response));
        this.resetState();
    }

    public int remaining() {
        return messages.size();
    }

    private boolean hasMore(boolean hasMore) {
        if (hasMore) {
            onMetadata("has_more", BooleanValue.TRUE);
        }
        return hasMore;
    }

    private class DiscardingBoltResultVisitor extends BoltResult.DiscardingRecordConsumer {
        @Override
        public void addMetadata(String key, AnyValue value) {
            metadataBuilder.add(key, value);
        }
    }

    private class RecordingBoltResultRecordConsumer implements BoltResult.RecordConsumer {
        private AnyValue[] anyValues;
        private int currentOffset = -1;

        @Override
        public void addMetadata(String key, AnyValue value) {
            metadataBuilder.add(key, value);
        }

        @Override
        public void beginRecord(int numberOfFields) {
            currentOffset = 0;
            anyValues = new AnyValue[numberOfFields];
        }

        @Override
        public void consumeField(AnyValue value) {
            anyValues[currentOffset++] = value;
        }

        @Override
        public void endRecord() {
            currentOffset = -1;
            messages.add(new RecordedRecordMessage(anyValues));
        }

        @Override
        public void onError() {
            // IGNORE
        }
    }
}
