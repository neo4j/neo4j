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
package org.neo4j.storageengine;

import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.storageengine.api.LogVersionRepository;

public class ReadOnlyLogVersionRepository implements LogVersionRepository {
    private final FixedLogVersion logVersion;
    private final FixedLogVersion checkpointLogVersion;

    public ReadOnlyLogVersionRepository(LogTailMetadata logTailMetadata) {
        this.logVersion = new FixedLogVersion(logTailMetadata.getLogVersion());
        this.checkpointLogVersion = new FixedLogVersion(logTailMetadata.getCheckpointLogVersion());
    }

    @Override
    public long getCurrentLogVersion() {
        return getCurrentVersion(logVersion);
    }

    @Override
    public void setCurrentLogVersion(long version) {
        setCurrentVersionAttempt();
    }

    @Override
    public long incrementAndGetVersion() {
        return incrementAndGetVersion(logVersion);
    }

    @Override
    public long getCheckpointLogVersion() {
        return getCurrentVersion(checkpointLogVersion);
    }

    @Override
    public void setCheckpointLogVersion(long version) {
        setCurrentVersionAttempt();
    }

    @Override
    public long incrementAndGetCheckpointLogVersion() {
        return incrementAndGetVersion(checkpointLogVersion);
    }

    private static long getCurrentVersion(FixedLogVersion version) {
        // We can expect a call to this during shutting down, if we have a LogFile using us.
        // So it's sort of OK.
        if (version.isIncrementAttempted()) {
            throw new IllegalStateException("Read-only log version repository has observed a call to "
                    + "incrementVersion, which indicates that it's been shut down");
        }
        return version.getValue();
    }

    private static void setCurrentVersionAttempt() {
        throw new UnsupportedOperationException("Can't set current log version in read only version repository.");
    }

    private static long incrementAndGetVersion(
            FixedLogVersion
                    version) { // We can expect a call to this during shutting down, if we have a LogFile using us.
        // So it's sort of OK.
        if (version.isIncrementAttempted()) {
            throw new IllegalStateException(
                    "Read-only log version repository only allows " + "to call incrementVersion once, during shutdown");
        }
        version.setIncrementAttempt();
        return version.getValue();
    }

    private static class FixedLogVersion {
        private boolean incrementAttempt;
        private final long value;

        FixedLogVersion(long value) {
            this.value = value;
        }

        boolean isIncrementAttempted() {
            return incrementAttempt;
        }

        long getValue() {
            return value;
        }

        void setIncrementAttempt() {
            this.incrementAttempt = true;
        }
    }
}
