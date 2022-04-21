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
package org.neo4j.bolt.v3.messaging;

import static java.lang.String.format;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.messaging.ResponseMessage;
import org.neo4j.bolt.messaging.ResponseMessageEncoder;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.PackOutput;
import org.neo4j.bolt.packstream.PackProvider;
import org.neo4j.bolt.v3.messaging.encoder.FailureMessageEncoder;
import org.neo4j.bolt.v3.messaging.encoder.IgnoredMessageEncoder;
import org.neo4j.bolt.v3.messaging.encoder.RecordMessageEncoder;
import org.neo4j.bolt.v3.messaging.encoder.SuccessMessageEncoder;
import org.neo4j.bolt.v3.messaging.response.FailureMessage;
import org.neo4j.bolt.v3.messaging.response.FatalFailureMessage;
import org.neo4j.bolt.v3.messaging.response.IgnoredMessage;
import org.neo4j.bolt.v3.messaging.response.RecordMessage;
import org.neo4j.bolt.v3.messaging.response.SuccessMessage;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.values.AnyValue;

/**
 * Writer for Bolt request messages to be sent to a {@link Neo4jPack.Packer}.
 */
public class BoltResponseMessageWriterV3 implements BoltResponseMessageWriter {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(BoltResponseMessageWriterV3.class);
    private static final int MAX_LOG_COMPONENT_LENGTH = 4096;

    private final PackOutput output;
    private final Neo4jPack.Packer packer;
    private final InternalLog log;
    private final Map<Byte, ResponseMessageEncoder<ResponseMessage>> encoders;
    private RecordMessageEncoder recordMessageEncoder = new RecordMessageEncoder();

    public BoltResponseMessageWriterV3(PackProvider packerProvider, PackOutput output, LogService logService) {
        this.output = output;
        this.packer = packerProvider.newPacker(output);
        this.log = logService.getInternalLog(getClass());
        this.encoders = registerEncoders();
    }

    private Map<Byte, ResponseMessageEncoder<ResponseMessage>> registerEncoders() {
        Map<Byte, ResponseMessageEncoder<?>> encoders = new HashMap<>();
        encoders.put(SuccessMessage.SIGNATURE, new SuccessMessageEncoder());
        encoders.put(RecordMessage.SIGNATURE, recordMessageEncoder);
        encoders.put(IgnoredMessage.SIGNATURE, new IgnoredMessageEncoder());
        encoders.put(FailureMessage.SIGNATURE, new FailureMessageEncoder(log));
        return (Map) encoders;
    }

    @Override
    public void write(ResponseMessage message) throws IOException {
        packCompleteMessageOrFail(message);
        if (message instanceof FatalFailureMessage) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException {
        output.flush();
    }

    public PackOutput output() {
        return this.output;
    }

    public InternalLog log() {
        return this.log;
    }

    private void packCompleteMessageOrFail(ResponseMessage message) throws IOException {
        boolean packingFailed = true;
        output.beginMessage();
        try {
            ResponseMessageEncoder<ResponseMessage> encoder = encoders.get(message.signature());
            if (encoder == null) {
                throw new BoltIOException(
                        Status.Request.InvalidFormat,
                        format("Message %s is not supported in this protocol version.", message));
            }
            encoder.encode(packer, message);
            packingFailed = false;
            output.messageSucceeded();
        } catch (Throwable error) {
            if (packingFailed) {
                // packing failed, there might be some half-written data in the output buffer right now
                // notify output about the failure so that it cleans up the buffer
                output.messageFailed();
                log.error("Failed to write full %s message because: %s", message, error.getMessage());
            }
            throw error;
        }
    }

    @Override
    public void beginRecord(int numberOfFields) throws IOException {
        output.beginMessage();
        try {
            recordMessageEncoder.beginRecord(packer, numberOfFields);
        } catch (Throwable error) {
            // packing failed, there might be some half-written data in the output buffer right now
            // notify output about the failure so that it cleans up the buffer
            output.messageFailed();
            log.error("Failed to write new record because: %s", error.getMessage());
            throw error;
        }
    }

    @Override
    public void consumeField(AnyValue value) throws IOException {
        try {
            recordMessageEncoder.onField(packer, value);
        } catch (Throwable error) {
            log.error("Failed to write value %s because: %s", formatValue(value), error.getMessage());
            onError();
            throw error;
        }
    }

    private String formatValue(AnyValue value) {
        var encoded = value.toString();

        if (encoded.length() < MAX_LOG_COMPONENT_LENGTH) {
            return encoded;
        }

        return encoded.substring(0, MAX_LOG_COMPONENT_LENGTH) + "...";
    }

    @Override
    public void endRecord() throws IOException {
        output.messageSucceeded();
    }

    @Override
    public void onError() {
        // packing failed, there might be some half-written data in the output buffer right now
        // notify output about the failure so that it cleans up the buffer
        output.messageReset();
    }

    @Override
    public void close() throws IOException {
        output.close();
    }
}
