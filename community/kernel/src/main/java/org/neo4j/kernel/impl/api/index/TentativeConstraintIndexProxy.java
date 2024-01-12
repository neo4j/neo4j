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
package org.neo4j.kernel.impl.api.index;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CopyOnWriteArrayList;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.schema.IncompleteConstraintValidationException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.DeferredConflictCheckingIndexUpdater;
import org.neo4j.storageengine.api.IndexEntryUpdate;

/**
 * What is a tentative constraint index proxy? Well, the way we build uniqueness constraints is as follows:
 * <ol>
 * <li>Begin a transaction T, which will be the "parent" transaction in this process</li>
 * <li>Execute a mini transaction Tt which will create the index rule to start the index population</li>
 * <li>In T: Sit and wait for the index to be built</li>
 * <li>In T: Create the constraint rule and connect the two</li>
 * </ol>
 *
 * The fully populated index flips to a tentative index. The reason for that is to guard for incoming transactions
 * that gets applied.
 * Such incoming transactions have potentially been verified on another instance with a slightly dated view
 * of the schema and has furthermore made it through some additional checks on this instance since transaction T
 * hasn't yet fully committed. Transaction data gets applied to the neo store first and the index second, so at
 * the point where the applying transaction sees that it violates the constraint it has already modified the store and
 * cannot back out. However, the constraint transaction T can. So a violated constraint while
 * in tentative mode does not fail the transaction violating the constraint, but keeps the failure around and will
 * eventually fail T instead.
 */
public class TentativeConstraintIndexProxy extends AbstractDelegatingIndexProxy {
    private final FlippableIndexProxy flipper;
    private final OnlineIndexProxy target;
    private final Collection<IndexEntryConflictException> failures = new CopyOnWriteArrayList<>();

    TentativeConstraintIndexProxy(FlippableIndexProxy flipper, OnlineIndexProxy target) {
        this.flipper = flipper;
        this.target = target;
    }

    @Override
    public IndexUpdater newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
        return switch (mode) {
            case ONLINE, RECOVERY -> new DeferredConflictCheckingIndexUpdater(
                    target.accessor.newUpdater(mode, cursorContext, parallel),
                    target::newValueReader,
                    target.getDescriptor(),
                    cursorContext) {
                @Override
                public void process(IndexEntryUpdate<?> update) {
                    try {
                        super.process(update);
                    } catch (IndexEntryConflictException conflict) {
                        failures.add(conflict);
                    }
                }

                @Override
                public void close() {
                    try {
                        super.close();
                    } catch (IndexEntryConflictException conflict) {
                        failures.add(conflict);
                    }
                }
            };
            default -> throw new IllegalArgumentException("Unsupported update mode: " + mode);
        };
    }

    @Override
    public InternalIndexState getState() {
        return failures.isEmpty() ? InternalIndexState.POPULATING : InternalIndexState.FAILED;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[target:" + target + "]";
    }

    @Override
    public ValueIndexReader newValueReader() throws IndexNotFoundKernelException {
        throw new IndexNotFoundKernelException(getDescriptor() + " is still populating");
    }

    @Override
    public TokenIndexReader newTokenReader() {
        throw new UnsupportedOperationException("Not supported for value indexes");
    }

    @Override
    public IndexProxy getDelegate() {
        return target;
    }

    @Override
    public void validate() throws IncompleteConstraintValidationException {
        if (!failures.isEmpty()) {
            throw new IncompleteConstraintValidationException(
                    ConstraintValidationException.Phase.VERIFICATION, new HashSet<>(failures));
        }
    }

    @Override
    public void activate() {
        if (failures.isEmpty()) {
            flipper.flipTo(target);
        } else {
            throw new IllegalStateException(
                    "Trying to activate failed index, should have checked the failures earlier...");
        }
    }
}
