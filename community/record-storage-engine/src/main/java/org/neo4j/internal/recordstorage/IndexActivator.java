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
package org.neo4j.internal.recordstorage;

import java.util.HashSet;
import java.util.Set;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.InternalKernelRuntimeException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.storageengine.api.IndexUpdateListener;

/**
 * Delayed activation of indexes. At the point in time when a transaction that creates a uniqueness constraint
 * commits it may be that some low-level locks are held on nodes/relationships, locks which prevents the backing index from being
 * fully populated. Those locks are released after the appliers have been closed. This activator acts as a register for indexes
 * that wants to be activated inside an applier, to be activated right after the low-level locks have been released for the batch
 * of transactions currently applying.
 */
public class IndexActivator implements AutoCloseable {
    private final IndexUpdateListener listener;
    private Set<IndexDescriptor> indexesToActivate;

    IndexActivator(IndexUpdateListener listener) {
        this.listener = listener;
    }

    /**
     * Activates any index that needs activation, i.e. have been added with {@link #activateIndex(IndexDescriptor)}.
     */
    @Override
    public void close() {
        if (indexesToActivate != null) {
            for (IndexDescriptor index : indexesToActivate) {
                try {
                    listener.activateIndex(index);
                } catch (KernelException | InternalKernelRuntimeException e) {
                    throw new IllegalStateException("Unable to enable constraint, backing index is not online.", e);
                }
            }
        }
    }

    /**
     * Makes a note to activate index after batch of transaction have been applied, i.e. in {@link #close()}.
     * @param index index.
     */
    void activateIndex(IndexDescriptor index) {
        if (indexesToActivate == null) {
            indexesToActivate = new HashSet<>();
        }
        indexesToActivate.add(index);
    }

    /**
     * Called when an index is dropped, so that a previously noted index to activate is removed from this internal list.
     * @param index index.
     */
    void indexDropped(IndexDescriptor index) {
        if (indexesToActivate != null) {
            indexesToActivate.remove(index);
        }
    }
}
