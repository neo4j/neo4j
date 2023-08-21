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
package org.neo4j.kernel.impl.transaction.log.files;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.dynamic_read_only_failover;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.configuration.Config;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeCallResult;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public class LogFileChannelNativeAccessor implements ChannelNativeAccessor {
    private final FileSystemAbstraction fileSystem;
    private final NativeAccess nativeAccess;
    private final InternalLog log;
    private final AtomicLong rotationThreshold;
    private final Config config;
    private final String databaseName;

    public LogFileChannelNativeAccessor(FileSystemAbstraction fileSystem, TransactionLogFilesContext context) {
        this(
                fileSystem,
                context.getNativeAccess(),
                context.getLogProvider(),
                context.getRotationThreshold(),
                context.getConfig(),
                context.getDatabaseName());
    }

    public LogFileChannelNativeAccessor(
            FileSystemAbstraction fileSystem,
            NativeAccess nativeAccess,
            InternalLogProvider logProvider,
            AtomicLong rotationThreshold,
            Config config,
            String databaseName) {
        this.fileSystem = fileSystem;
        this.nativeAccess = nativeAccess;
        this.log = logProvider.getLog(getClass());
        this.rotationThreshold = rotationThreshold;
        this.config = config;
        this.databaseName = databaseName;
    }

    @Override
    public void adviseSequentialAccessAndKeepInCache(StoreChannel channel, long version) {
        if (channel.isOpen()) {
            final int fileDescriptor = fileSystem.getFileDescriptor(channel);
            var sequentialResult = nativeAccess.tryAdviseSequentialAccess(fileDescriptor);
            if (sequentialResult.isError()) {
                log.warn("Unable to advise sequential access for transaction log version: " + version + ". Error: "
                        + sequentialResult);
            }
            var cacheResult = nativeAccess.tryAdviseToKeepInCache(fileDescriptor);
            if (cacheResult.isError()) {
                log.warn("Unable to advise preserve data in cache for transaction log version: " + version + ". Error: "
                        + cacheResult);
            }
        }
    }

    @Override
    public void evictFromSystemCache(StoreChannel channel, long version) {
        if (channel.isOpen()) {
            var result = nativeAccess.tryEvictFromCache(fileSystem.getFileDescriptor(channel));
            if (result.isError()) {
                log.warn("Unable to evict transaction log from cache with version: " + version + ". Error: " + result);
            }
        }
    }

    @Override
    public void preallocateSpace(StoreChannel storeChannel, long version) {
        int fileDescriptor = fileSystem.getFileDescriptor(storeChannel);
        var result = nativeAccess.tryPreallocateSpace(fileDescriptor, rotationThreshold.get());
        if (result.isError()) {
            if (nativeAccess.errorTranslator().isOutOfDiskSpace(result)) {
                handleOutOfDiskSpaceError(result);
            } else {
                log.warn("Error on attempt to preallocate log file version: " + version + ". Error: " + result);
            }
        }
    }

    private void handleOutOfDiskSpaceError(NativeCallResult result) {
        log.error(
                "Warning! System is running out of disk space. Failed to preallocate log file since disk does not have enough space left. "
                        + "Please provision more space to avoid that. Allocation failure details: " + result);
        if (config.get(dynamic_read_only_failover)) {
            log.error("Switching database to read only mode.");
            markDatabaseReadOnly();
        } else {
            log.error(
                    "Dynamic switchover to read-only mode is disabled. The database will continue execution in the current mode.");
        }
    }

    private void markDatabaseReadOnly() {
        Set<String> readOnlyDatabases = new HashSet<>(config.get(read_only_databases));
        readOnlyDatabases.add(databaseName);
        config.setDynamic(read_only_databases, readOnlyDatabases, "Dynamic failover to read-only mode.");
    }
}
