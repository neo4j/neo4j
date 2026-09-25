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
package org.neo4j.wal.enveloped;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.wal.entry.LogFormat;
import org.neo4j.wal.entry.LogHeader;

public class BaseLogHeaderFactory implements LogHeaderFactory {
    // Once the store identifier is set or used for a log, then it cannot be changed.
    private final AtomicBoolean storeIdentifierFinalized = new AtomicBoolean(false);
    private final DatabaseConfig config;
    private volatile KernelVersion currentAppendedDatabaseVersion;
    private volatile StoreIdentifier storeIdentifier;
    private final Clock clock;
    private static final LogFormat MIN_VERSION = LogFormat.V11;

    public BaseLogHeaderFactory(
            DatabaseConfig config,
            KernelVersion currentAppendedDatabaseVersion,
            StoreIdentifier storeIdentifier,
            Clock clock) {
        this.config = config;
        this.currentAppendedDatabaseVersion = currentAppendedDatabaseVersion;
        this.storeIdentifier = storeIdentifier;
        this.clock = clock;
    }

    public BaseLogHeaderFactory(
            DatabaseConfig config, KernelVersion currentAppendedDatabaseVersion, StoreId storeId, Clock clock) {
        this.config = config;
        this.currentAppendedDatabaseVersion = currentAppendedDatabaseVersion;
        this.storeIdentifier = StoreIdentifier.newStoreIdentifier(storeId);
        this.clock = clock;
    }

    @Override
    public LogHeader createLogHeader(
            long newFileVersion, long lastAppendIndex, int lastChecksum, int segmentSize, long preFileTerm) {
        storeIdentifierFinalized.set(true);
        KernelVersion version = getCurrentDatabaseVersion();
        LogFormat logFormat = LogFormat.fromConfigAndKernelVersion(config, version);
        if (logFormat.getVersionByte() < MIN_VERSION.getVersionByte()) {
            logFormat = MIN_VERSION;
        }
        return logFormat.newHeader(
                newFileVersion,
                lastAppendIndex,
                preFileTerm,
                storeIdentifier,
                segmentSize,
                lastChecksum,
                version,
                clock.millis());
    }

    @Override
    public void setVersion(KernelVersion databaseVersion) {
        this.currentAppendedDatabaseVersion = databaseVersion;
    }

    @Override
    public void setStoreIdentifier(StoreIdentifier storeIdentifier) {
        if (storeIdentifierFinalized.compareAndSet(false, true)) {
            this.storeIdentifier = storeIdentifier;
        } else if (!storeIdentifier.equals(this.storeIdentifier)) {
            throw new IllegalStateException(
                    "Store identifier can not be changed current:" + this.storeIdentifier + " new:" + storeIdentifier);
        }
    }

    @Override
    public void allowStoreIdentifierChange() {
        storeIdentifierFinalized.set(false);
    }

    public KernelVersion getCurrentDatabaseVersion() {
        if (currentAppendedDatabaseVersion == null) {
            throw new IllegalStateException("No version has been set for the current log");
        }
        return currentAppendedDatabaseVersion;
    }
}
