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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.LogVersionRepository;

class TransactionLogFilesContext
{
    private final AtomicLong rotationThreshold;
    private final LogEntryReader logEntryReader;
    private final LongSupplier lastCommittedTransactionIdSupplier;
    private final LongSupplier committingTransactionIdSupplier;
    private final Supplier<LogVersionRepository> logVersionRepositorySupplier;
    private final LogFileCreationMonitor logFileCreationMonitor;
    private final FileSystemAbstraction fileSystem;
    private final LogProvider logProvider;

    TransactionLogFilesContext( AtomicLong rotationThreshold, LogEntryReader logEntryReader,
            LongSupplier lastCommittedTransactionIdSupplier, LongSupplier committingTransactionIdSupplier,
            LogFileCreationMonitor logFileCreationMonitor, Supplier<LogVersionRepository> logVersionRepositorySupplier,
            FileSystemAbstraction fileSystem, LogProvider logProvider )
    {
        this.rotationThreshold = rotationThreshold;
        this.logEntryReader = logEntryReader;
        this.lastCommittedTransactionIdSupplier = lastCommittedTransactionIdSupplier;
        this.committingTransactionIdSupplier = committingTransactionIdSupplier;
        this.logVersionRepositorySupplier = logVersionRepositorySupplier;
        this.logFileCreationMonitor = logFileCreationMonitor;
        this.fileSystem = fileSystem;
        this.logProvider = logProvider;
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

    LogFileCreationMonitor getLogFileCreationMonitor()
    {
        return logFileCreationMonitor;
    }

    FileSystemAbstraction getFileSystem()
    {
        return fileSystem;
    }

    public LogProvider getLogProvider()
    {
        return logProvider;
    }

    public NativeAccess getNativeAccess()
    {
        return NativeAccessProvider.getNativeAccess();
    }
}
