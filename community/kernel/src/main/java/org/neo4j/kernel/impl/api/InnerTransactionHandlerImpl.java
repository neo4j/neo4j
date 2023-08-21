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
package org.neo4j.kernel.impl.api;

import org.eclipse.collections.api.map.primitive.ImmutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.kernel.api.InnerTransactionHandler;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * The public methods of this class are synchronized as we expect it to be accessed from both the executing thread and possibly from a different thread which
 * marks the transaction to be terminated.
 */
class InnerTransactionHandlerImpl implements InnerTransactionHandler {
    /**
     * The transaction for this handler has been closed/terminated.
     */
    private boolean closed;
    /**
     * The reason for terminating this handler's transaction. Is not null once marked for termination.
     */
    private Status terminationReason;

    private MutableLongSet innerTransactionIds;
    private final KernelTransactions kernelTransactions;

    InnerTransactionHandlerImpl(KernelTransactions kernelTransactions) {
        this.kernelTransactions = kernelTransactions;
    }

    /**
     * {@inheritDoc}
     * <p>
     * We assume this method to only be called from the executing thread.
     */
    @Override
    public synchronized void registerInnerTransaction(long innerTransactionId) {
        if (closed) {
            throw new IllegalStateException("The inner transaction handler is already closed.");
        } else if (terminationReason != null) {
            terminateInnerTransaction(terminationReason, getTransactionHandlesById(), innerTransactionId);
        } else {
            if (innerTransactionIds == null) {
                innerTransactionIds = LongSets.mutable.empty();
            }
            innerTransactionIds.add(innerTransactionId);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * We assume this method to only be called from the executing thread.
     */
    @Override
    public synchronized void removeInnerTransaction(long innerTransactionId) {
        if (innerTransactionIds != null) {
            innerTransactionIds.remove(innerTransactionId);
        }
    }

    /**
     * @return {@code true} if any open inner transaction is currently connected to this transaction.
     */
    synchronized boolean hasInnerTransaction() {
        return innerTransactionIds != null && !innerTransactionIds.isEmpty();
    }

    /**
     * Terminates all the inner transactions contained in this handler and all handlers subsequently registered with this handler.
     * <p>
     * This method may be called from a thread different from the executing thread.
     */
    synchronized void terminateInnerTransactions(Status reason) {
        terminationReason = reason;
        var handlesById = getTransactionHandlesById();
        if (innerTransactionIds != null) {
            innerTransactionIds.forEach(
                    innerTransactionId -> terminateInnerTransaction(reason, handlesById, innerTransactionId));
            innerTransactionIds.clear();
            innerTransactionIds = null;
        }
    }

    private ImmutableLongObjectMap<KernelTransactionHandle> getTransactionHandlesById() {
        return LongObjectMaps.immutable.from(
                kernelTransactions.executingTransactions(),
                KernelTransactionHandle::getTransactionSequenceNumber,
                a -> a);
    }

    private void terminateInnerTransaction(
            Status reason, ImmutableLongObjectMap<KernelTransactionHandle> handlesById, long innerTransactionId) {
        KernelTransactionHandle kernelTransactionHandle = handlesById.get(innerTransactionId);
        if (kernelTransactionHandle != null) {
            kernelTransactionHandle.markForTermination(reason);
        }
    }

    /**
     * Marks this handler as closed, once the transaction that this handler belongs to is closed.
     * <p>
     * We assume this method to only be called from the executing thread and to have no further interaction with this handler from that thread.
     */
    synchronized void close() {
        this.closed = true;
        if (this.innerTransactionIds != null) {
            this.innerTransactionIds.clear();
        }
        this.innerTransactionIds = null;
    }
}
