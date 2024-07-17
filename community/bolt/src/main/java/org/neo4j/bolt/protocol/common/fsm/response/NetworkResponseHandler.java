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
package org.neo4j.bolt.protocol.common.fsm.response;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.listener.ConnectionListener;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.MetadataHandler;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.response.FailureMessage;
import org.neo4j.bolt.protocol.common.message.response.IgnoredMessage;
import org.neo4j.bolt.protocol.common.message.response.SuccessMessage;
import org.neo4j.bolt.protocol.error.streaming.BoltStreamingWriteException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

public class NetworkResponseHandler extends AbstractMetadataAwareResponseHandler {
    private static final Set<Status> CLIENT_MID_OP_DISCONNECT_ERRORS =
            new HashSet<>(Arrays.asList(Status.Transaction.Terminated, Status.Transaction.LockClientStopped));

    private final Connection connection;
    private final int bufferSize;
    private final int flushThreshold;
    private final Log log;

    private MapValueBuilder metadataBuilder;
    private NetworkRecordHandler recordHandler;

    public NetworkResponseHandler(
            Connection connection,
            MetadataHandler metadataHandler,
            int bufferSize,
            int flushThreshold,
            LogService logging) {
        super(metadataHandler);

        this.connection = connection;
        this.bufferSize = bufferSize;
        this.flushThreshold = flushThreshold;
        this.log = logging.getInternalLog(NetworkResponseHandler.class);
    }

    @Override
    public void onMetadata(String key, AnyValue value) {
        if (this.metadataBuilder == null) {
            this.metadataBuilder = new MapValueBuilder();
        }

        this.metadataBuilder.add(key, value);
    }

    @Override
    public RecordHandler onBeginStreaming(List<String> fieldNames) {
        return this.recordHandler =
                new NetworkRecordHandler(this.connection, fieldNames.size(), this.bufferSize, this.flushThreshold);
    }

    @Override
    public void onFailure(Error error) {
        // ensure that any remaining resources are released back to the pool
        if (this.recordHandler != null) {
            this.recordHandler.close();
            this.recordHandler = null;
        }

        // discard any metadata accumulated until now as we no longer consider any of it valid
        this.metadataBuilder = null;

        if (error.isFatal()) {
            this.log.debug("Publishing fatal error: %s", error);
        }

        var remoteAddress = this.connection.clientAddress();
        connection
                .writeAndFlush(new FailureMessage(error.status(), error.message(), error.isFatal()))
                .addListener(f -> {
                    if (f.isSuccess()) {
                        return;
                    }

                    // TODO: Re-Evaluate after StateMachine refactor (Should be handled upstream)

                    // Can't write error to the client, because the connection is closed.
                    // Very likely our error is related to the connection being closed.

                    // If the error is that the transaction was terminated, then the error is a side-effect of
                    // us cleaning up stuff that was running when the client disconnected. Log a warning without
                    // stack trace to highlight clients are disconnecting while stuff is running:
                    if (CLIENT_MID_OP_DISCONNECT_ERRORS.contains(error.status())) {
                        this.log.warn(
                                "Client %s disconnected while query was running. Session has been cleaned up. "
                                        + "This can be caused by temporary network problems, but if you see this often, "
                                        + "ensure your applications are properly waiting for operations to complete before exiting.",
                                remoteAddress);
                        return;
                    }

                    // If the error isn't that the tx was terminated, log it to the console for debugging. It's likely
                    // there are other "ok" errors that we can whitelist into the conditional above over time.
                    var ex = f.cause();
                    ex.addSuppressed(error.cause());

                    this.log.warn("Unable to send error back to the client. " + ex.getMessage(), ex);
                });

        this.connection.notifyListenersSafely("requestResultFailure", listener -> listener.onResponseFailed(error));
    }

    @Override
    public void onIgnored() {
        try {
            this.connection.writeAndFlush(IgnoredMessage.INSTANCE).sync();
            this.connection.notifyListenersSafely("requestResultIgnored", ConnectionListener::onResponseIgnored);
        } catch (Throwable ex) {
            throw new BoltStreamingWriteException("Failed to transmit operation result: Response write failure", ex);
        }
    }

    @Override
    public void onSuccess() {
        // ensure that any remaining resources are released back to the pool
        if (this.recordHandler != null) {
            this.recordHandler.close();
            this.recordHandler = null;
        }

        MapValue metadata;
        if (this.metadataBuilder != null) {
            metadata = this.metadataBuilder.build();
            this.metadataBuilder = null;
        } else {
            metadata = MapValue.EMPTY;
        }

        try {
            this.connection.writeAndFlush(new SuccessMessage(metadata)).sync();

            this.connection.notifyListenersSafely(
                    "requestResultSuccess", listener -> listener.onResponseSuccess(metadata));
        } catch (Throwable ex) {
            throw new BoltStreamingWriteException("Failed to transmit operation result: Response write failure", ex);
        }
    }
}
