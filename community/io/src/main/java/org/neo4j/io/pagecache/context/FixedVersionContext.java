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
package org.neo4j.io.pagecache.context;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;

public class FixedVersionContext implements VersionContext {
    private static final long INVALID_TRANSACTION_ID = 0;
    public static final VersionContext EMPTY_VERSION_CONTEXT = new FixedVersionContext(INVALID_TRANSACTION_ID);

    private final long committingTransactionId;

    public FixedVersionContext(long committingTransactionId) {
        this.committingTransactionId = committingTransactionId;
    }

    @Override
    public void initRead() {}

    @Override
    public void initWrite(long committingTransactionId) {}

    @Override
    public long committingTransactionId() {
        return committingTransactionId;
    }

    @Override
    public long lastClosedTransactionId() {
        return Long.MAX_VALUE;
    }

    @Override
    public long highestClosed() {
        return Long.MAX_VALUE;
    }

    @Override
    public void markAsDirty() {}

    @Override
    public boolean isDirty() {
        return false;
    }

    @Override
    public long[] notVisibleTransactionIds() {
        return EMPTY_LONG_ARRAY;
    }

    @Override
    public long oldestVisibleTransactionNumber() {
        return INVALID_TRANSACTION_ID;
    }

    @Override
    public void refreshVisibilityBoundaries() {}

    @Override
    public void observedChainHead(long headVersion) {}

    @Override
    public boolean invisibleHeadObserved() {
        return false;
    }

    @Override
    public void resetObsoleteHeadState() {}

    @Override
    public void markHeadInvisible() {}

    @Override
    public long chainHeadVersion() {
        return Long.MIN_VALUE;
    }

    @Override
    public boolean initializedForWrite() {
        return committingTransactionId != INVALID_TRANSACTION_ID;
    }
}
