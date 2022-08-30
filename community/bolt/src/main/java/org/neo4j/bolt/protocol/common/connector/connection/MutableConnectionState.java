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
package org.neo4j.bolt.protocol.common.connector.connection;

import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.result.BoltResult;
import org.neo4j.bolt.protocol.common.message.result.ResponseHandler;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;

/**
 * Keeps state of the connection and bolt state machine.
 */
public class MutableConnectionState implements ResponseHandler {
    private Error pendingError;
    private boolean pendingIgnore;
    private volatile boolean terminated;
    private boolean closed;
    private volatile String currentTransactionId;

    /**
     * Callback poised to receive the next response.
     */
    private ResponseHandler responseHandler;

    /**
     * This will be set if the previous transaction (rolled back already) has left some error.
     */
    private Status pendingTerminationNotice;

    @Override
    public boolean onPullRecords(BoltResult result, long size) throws Throwable {
        if (responseHandler != null) {
            return responseHandler.onPullRecords(result, size);
        } else {
            return false;
        }
    }

    @Override
    public boolean onDiscardRecords(BoltResult result, long size) throws Throwable {
        if (responseHandler != null) {
            return responseHandler.onDiscardRecords(result, size);
        } else {
            return false;
        }
    }

    @Override
    public void onMetadata(String key, AnyValue value) {
        if (responseHandler != null) {
            responseHandler.onMetadata(key, value);
        }
    }

    @Override
    public void markIgnored() {
        if (responseHandler != null) {
            responseHandler.markIgnored();
        } else {
            pendingIgnore = true;
        }
    }

    @Override
    public void markFailed(Error error) {
        if (responseHandler != null) {
            responseHandler.markFailed(error);
        } else {
            pendingError = error;
        }
    }

    @Override
    public void onFinish() {
        if (responseHandler != null) {
            responseHandler.onFinish();
        }
    }

    public Error getPendingError() {
        return pendingError;
    }

    public boolean hasPendingIgnore() {
        return pendingIgnore;
    }

    public void resetPendingFailedAndIgnored() {
        pendingError = null;
        pendingIgnore = false;
        pendingTerminationNotice = null;
    }

    public boolean canProcessMessage() {
        return !closed && pendingError == null && !pendingIgnore;
    }

    public ResponseHandler getResponseHandler() {
        return responseHandler;
    }

    public void setResponseHandler(ResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
    }

    public void setPendingTerminationNotice(Status terminationNotice) {
        this.pendingTerminationNotice = terminationNotice;
    }

    public boolean isTerminated() {
        return terminated;
    }

    public void markTerminated() {
        terminated = true;
    }

    public boolean isClosed() {
        return closed;
    }

    public void markClosed() {
        closed = true;
    }

    public void ensureNoPendingTerminationNotice() {
        if (pendingTerminationNotice != null) {
            Status status = pendingTerminationNotice;

            pendingTerminationNotice = null;

            throw new TransactionTerminatedException(status);
        }
    }

    public void setCurrentTransactionId(String transactionId) throws BoltProtocolBreachFatality {
        if (this.currentTransactionId != null) {
            throw new BoltProtocolBreachFatality(
                    "Cannot assign new transaction id without clearing the old id: " + currentTransactionId);
        }
        this.currentTransactionId = transactionId;
    }

    public void clearCurrentTransactionId() {
        this.currentTransactionId = null;
    }

    public String getCurrentTransactionId() {
        return currentTransactionId;
    }
}
