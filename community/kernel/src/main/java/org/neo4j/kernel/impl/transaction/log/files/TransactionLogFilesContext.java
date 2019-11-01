/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;

class TransactionLogFilesContext
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
    private final DatabaseTracer databaseTracer;
    private final Supplier<StoreId> storeId;
    private final NativeAccess nativeAccess;

    TransactionLogFilesContext( AtomicLong rotationThreshold, AtomicBoolean tryPreallocateTransactionLogs, LogEntryReader logEntryReader,
            LongSupplier lastCommittedTransactionIdSupplier, LongSupplier committingTransactionIdSupplier, Supplier<LogPosition> lastClosedPositionSupplier,
            Supplier<LogVersionRepository> logVersionRepositorySupplier, FileSystemAbstraction fileSystem,
            LogProvider logProvider, DatabaseTracer databaseTracer, Supplier<StoreId> storeId, NativeAccess nativeAccess )
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
        this.databaseTracer = databaseTracer;
        this.storeId = storeId;
        this.nativeAccess = nativeAccess;
    }

    AtomicLong getRotationThreshold()
    {
        return rotationThreshold;
    }

    LogEntryReader getLogEntryReader()
    {
        return logEntryReader;
    }

    LogVersionRepository getLogVersionRepository()
    {
        return logVersionRepositorySupplier.get();
    }

    long getLastCommittedTransactionId()
    {
        return lastCommittedTransactionIdSupplier.getAsLong();
    }

    long committingTransactionId()
    {
        return committingTransactionIdSupplier.getAsLong();
    }

    LogPosition getLastClosedTransactionPosition()
    {
        return lastClosedPositionSupplier.get();
    }

    FileSystemAbstraction getFileSystem()
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

    DatabaseTracer getDatabaseTracer()
    {
        return databaseTracer;
    }

    public StoreId getStoreId()
    {
        return storeId.get();
    }
}
