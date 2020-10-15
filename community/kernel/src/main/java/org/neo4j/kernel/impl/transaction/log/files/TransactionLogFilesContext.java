/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log.files;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.TransactionLogVersionProvider;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;

public class TransactionLogFilesContext
{
    private final AtomicLong rotationThreshold;
    private final AtomicBoolean tryPreallocateTransactionLogs;
    private final LogEntryReader logEntryReader;
    private final LongSupplier lastCommittedTransactionIdSupplier;
    private final LongSupplier committingTransactionIdSupplier;
    private final Supplier<LogPosition> lastClosedPositionSupplier;
    private final Supplier<LogVersionRepository> logVersionRepositorySupplier;
    private final FileSystemAbstraction fileSystem;
    private final LogProvider logProvider;
    private final DatabaseTracers databaseTracers;
    private final NativeAccess nativeAccess;
    private final MemoryTracker memoryTracker;
    private final Monitors monitors;
    private final boolean failOnCorruptedLogFiles;
    private final Supplier<StoreId> storeId;
    private final DatabaseHealth databaseHealth;
    private final TransactionLogVersionProvider transactionLogVersionProvider;
    private final Clock clock;
    private final Config config;

    public TransactionLogFilesContext( AtomicLong rotationThreshold, AtomicBoolean tryPreallocateTransactionLogs, LogEntryReader logEntryReader,
            LongSupplier lastCommittedTransactionIdSupplier, LongSupplier committingTransactionIdSupplier, Supplier<LogPosition> lastClosedPositionSupplier,
            Supplier<LogVersionRepository> logVersionRepositorySupplier,FileSystemAbstraction fileSystem, LogProvider logProvider,
            DatabaseTracers databaseTracers, Supplier<StoreId> storeId, NativeAccess nativeAccess,
            MemoryTracker memoryTracker, Monitors monitors, boolean failOnCorruptedLogFiles, DatabaseHealth databaseHealth,
            TransactionLogVersionProvider transactionLogVersionProvider, Clock clock, Config config )
    {
        this.rotationThreshold = rotationThreshold;
        this.tryPreallocateTransactionLogs = tryPreallocateTransactionLogs;
        this.logEntryReader = logEntryReader;
        this.lastCommittedTransactionIdSupplier = lastCommittedTransactionIdSupplier;
        this.committingTransactionIdSupplier = committingTransactionIdSupplier;
        this.lastClosedPositionSupplier = lastClosedPositionSupplier;
        this.logVersionRepositorySupplier = logVersionRepositorySupplier;
        this.fileSystem = fileSystem;
        this.logProvider = logProvider;
        this.databaseTracers = databaseTracers;
        this.storeId = storeId;
        this.nativeAccess = nativeAccess;
        this.memoryTracker = memoryTracker;
        this.monitors = monitors;
        this.failOnCorruptedLogFiles = failOnCorruptedLogFiles;
        this.databaseHealth = databaseHealth;
        this.transactionLogVersionProvider = transactionLogVersionProvider;
        this.clock = clock;
        this.config = config;
    }

    AtomicLong getRotationThreshold()
    {
        return rotationThreshold;
    }

    public LogEntryReader getLogEntryReader()
    {
        return logEntryReader;
    }

    public LogVersionRepository getLogVersionRepository()
    {
        return logVersionRepositorySupplier.get();
    }

    public long getLastCommittedTransactionId()
    {
        return lastCommittedTransactionIdSupplier.getAsLong();
    }

    public long committingTransactionId()
    {
        return committingTransactionIdSupplier.getAsLong();
    }

    LogPosition getLastClosedTransactionPosition()
    {
        return lastClosedPositionSupplier.get();
    }

    public FileSystemAbstraction getFileSystem()
    {
        return fileSystem;
    }

    public LogProvider getLogProvider()
    {
        return logProvider;
    }

    AtomicBoolean getTryPreallocateTransactionLogs()
    {
        return tryPreallocateTransactionLogs;
    }

    NativeAccess getNativeAccess()
    {
        return nativeAccess;
    }

    public DatabaseTracers getDatabaseTracers()
    {
        return databaseTracers;
    }

    public StoreId getStoreId()
    {
        return storeId.get();
    }

    public MemoryTracker getMemoryTracker()
    {
        return memoryTracker;
    }

    public Monitors getMonitors()
    {
        return monitors;
    }

    public boolean isFailOnCorruptedLogFiles()
    {
        return failOnCorruptedLogFiles;
    }

    public DatabaseHealth getDatabaseHealth()
    {
        return databaseHealth;
    }

    public TransactionLogVersionProvider getTransactionLogVersionProvider()
    {
        return transactionLogVersionProvider;
    }

    public Clock getClock()
    {
        return clock;
    }

    public Config getConfig()
    {
        return config;
    }
}
