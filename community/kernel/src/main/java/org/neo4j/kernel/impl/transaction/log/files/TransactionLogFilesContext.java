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

import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.LogFormatVersionProvider;
import org.neo4j.kernel.impl.transaction.log.RecoveryOutcome;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StoreId;

public record TransactionLogFilesContext(
        AtomicLong rotationThreshold,
        long checkpointRotationThreshold,
        AtomicBoolean tryPreallocateTransactionLogs,
        CommandReaderFactory commandReaderFactory,
        LogFileVersionTracker versionTracker,
        FileSystemAbstraction fileSystem,
        InternalLogProvider logProvider,
        DatabaseTracers databaseTracers,
        Supplier<StoreId> storeId,
        NativeAccess nativeAccess,
        MemoryTracker memoryTracker,
        Monitors monitors,
        boolean failOnCorruptedLogFiles,
        DatabaseHealth databaseHealth,
        KernelVersionProvider emptyDbKernelVersionProvider,
        LogFormatVersionProvider emptyDbLogFormatVersionProvider,
        Clock clock,
        String databaseName,
        Config config,
        BinarySupportedKernelVersions binarySupportedKernelVersions,
        boolean readOnly,
        int envelopeSegmentBlockSizeBytes,
        int bufferSizeBytes,
        RecoveryOutcome recoveryOutcome) {}
