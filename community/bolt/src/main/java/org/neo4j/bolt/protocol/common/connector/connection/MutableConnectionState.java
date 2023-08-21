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
package org.neo4j.bolt.protocol.common.connector.connection;

import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;

/**
 * Keeps state of the connection and bolt state machine.
 */
public class MutableConnectionState {
    private boolean responded;

    private Error pendingError;
    private boolean pendingIgnore;

    /**
     * Callback poised to receive the next response.
     */
    private ResponseHandler responseHandler;

    /**
     * This will be set if the previous transaction (rolled back already) has left some error.
     */
    private Status pendingTerminationNotice;

    public void onMetadata(String key, AnyValue value) {
        if (responseHandler != null) {
            responseHandler.onMetadata(key, value);
        }
    }

    public void markIgnored() {
        pendingIgnore = true;
    }

    public void markFailed(Error error) {
        pendingError = error;
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
        return pendingError == null && !pendingIgnore;
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

    public void ensureNoPendingTerminationNotice() {
        if (pendingTerminationNotice != null) {
            Status status = pendingTerminationNotice;

            pendingTerminationNotice = null;

            throw new TransactionTerminatedException(status);
        }
    }
}
