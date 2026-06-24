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
package org.neo4j.kernel.impl.transaction.log.entry;

import static org.neo4j.io.fs.ReadableChannel.UNSPECIFIED_CONTENT_TYPE;
import static org.neo4j.kernel.impl.transaction.log.distributed.ReplicatedTransactionHelper.skipDistributedHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.EMPTY_TX;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.KERNEL_CONTENT_TYPE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.REPLICATED_TX_CONTENT_TYPE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.VERSION_UPGRADE_CONTENT_TYPE;
import static org.neo4j.kernel.impl.transaction.log.entry.TailUtils.checkSmallChunkOfTail;
import static org.neo4j.kernel.impl.transaction.log.entry.TailUtils.checkTail;

import java.io.IOException;
import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.enveloped.IncompleteEnvelopeReadException;
import org.neo4j.kernel.impl.transaction.log.enveloped.InconsistentLogFilesException;
import org.neo4j.kernel.impl.transaction.log.enveloped.InvalidLogEnvelopeReadException;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandReaderFactory;

/**
 * Reads {@link LogEntry log entries} off of a channel. Supported versions can be read intermixed.
 */
public class VersionAwareLogEntryReader implements LogEntryReader {
    private final CommandReaderFactory commandReaderFactory;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;
    private final LogPositionMarker positionMarker;
    private final MemoryTracker memoryTracker;
    private final boolean treatBrokenLastEntryAsCorruption;
    private boolean brokenLastEntry;
    private LogEntrySerializationSet parserSet;

    public VersionAwareLogEntryReader(
            CommandReaderFactory commandReaderFactory,
            BinarySupportedKernelVersions binarySupportedKernelVersions,
            MemoryTracker memoryTracker) {
        this(commandReaderFactory, binarySupportedKernelVersions, memoryTracker, false);
    }

    public VersionAwareLogEntryReader(
            CommandReaderFactory commandReaderFactory,
            BinarySupportedKernelVersions binarySupportedKernelVersions,
            MemoryTracker memoryTracker,
            boolean treatBrokenLastEntryAsCorruption) {
        this.commandReaderFactory = commandReaderFactory;
        this.positionMarker = new LogPositionMarker();
        this.binarySupportedKernelVersions = binarySupportedKernelVersions;
        this.memoryTracker = memoryTracker;
        this.treatBrokenLastEntryAsCorruption = treatBrokenLastEntryAsCorruption;
    }

    @Override
    public LogEntry readLogEntry(ReadableLogPositionAwareChannel channel) throws IOException {
        boolean insideEntry = false;
        try {
            while (true) {
                insideEntry = false;
                byte versionCode = channel.markAndGetVersion(positionMarker);
                if (versionCode == 0) {
                    // We reached the end of available records,
                    // but in pre-allocated file still have space available.
                    // We reset channel position to restore last read byte in case someone would like to re-read
                    // or check it again if possible,
                    // and we report that we reach end of record stream from our point of view.
                    // Let's double-check that it isn't a corrupt byte by checking part of the tail first
                    checkSmallChunkOfTail(channel, channel.getCurrentLogPosition());
                    channel.position(positionMarker.getByteOffset());
                    return null;
                }
                insideEntry = true;

                updateParserSet(channel, versionCode);
                // Capture position in case we are at end of file and getContentType() reads off the end
                byte contentType = channel.getContentType();
                byte typeCode =
                        switch (contentType) {
                            case KERNEL_CONTENT_TYPE, UNSPECIFIED_CONTENT_TYPE -> channel.get();

                            case REPLICATED_TX_CONTENT_TYPE, VERSION_UPGRADE_CONTENT_TYPE -> {
                                skipDistributedHeader(channel);

                                yield channel.get();
                            }

                            default -> EMPTY_TX;
                        };

                LogEntry logEntry = readEntry(channel, versionCode, typeCode, memoryTracker);
                if (logEntry != LogEntry.SKIP) {
                    return logEntry;
                }
            }
        } catch (ReadPastEndException e) {
            if (insideEntry && treatBrokenLastEntryAsCorruption) {
                throw new InconsistentLogFilesException(
                        "Log file has a broken last entry in a state where it is not allowed - read past end");
            }
            return null;
        } catch (UnsupportedLogVersionException
                | IllegalStateException
                | InvalidLogEnvelopeReadException
                | ChecksumMismatchException e) {
            throw e;
        } catch (IncompleteEnvelopeReadException e) {
            // This exception signals that the tail is already checked and the last entry is broken.
            return brokenLastEntry(e);
        } catch (IOException | RuntimeException e) {
            LogPosition currentLogPosition = channel.getCurrentLogPosition();
            // check if error was in the last command or is there anything else after that
            checkTail(channel, currentLogPosition, e);
            return brokenLastEntry(e);
        }
    }

    private LogEntry brokenLastEntry(Throwable cause) throws IOException {
        if (treatBrokenLastEntryAsCorruption) {
            throw new InconsistentLogFilesException(
                    "Log file has a broken last entry in a state where it is not allowed", cause);
        }
        brokenLastEntry = true;
        return null;
    }

    @Override
    public boolean hasBrokenLastEntry() {
        return brokenLastEntry;
    }

    private void updateParserSet(ReadableLogPositionAwareChannel channel, byte versionCode) throws IOException {
        if (parserSet != null && parserSet.getIntroductionVersion().version() == versionCode) {
            return; // We already have the correct parser set
        }
        try {
            KernelVersion kernelVersion = KernelVersion.getForVersion(versionCode);
            parserSet = LogEntrySerializationSets.serializationSet(kernelVersion, binarySupportedKernelVersions);

            if (channel.rewindAfterMarkAndGetVersion()) {
                // Since checksum is calculated over the whole entry we need to rewind and begin
                // a new checksum segment if we change version parser.
                rewindOneByte(channel);
                channel.beginChecksum();
                channel.get();
            }
        } catch (IllegalArgumentException e) {
            throw UnsupportedLogVersionException.unsupported(binarySupportedKernelVersions, versionCode);
        }
    }

    private void rewindOneByte(ReadableLogPositionAwareChannel channel) throws IOException {
        channel.position(channel.position() - 1);
        channel.getCurrentLogPosition(positionMarker);
    }

    private LogEntry readEntry(
            ReadableLogPositionAwareChannel channel, byte versionCode, byte typeCode, MemoryTracker memoryTracker)
            throws IOException {
        try {
            return parserSet
                    .select(typeCode)
                    .parse(
                            parserSet.getIntroductionVersion(),
                            parserSet.wrap(channel),
                            positionMarker,
                            commandReaderFactory,
                            memoryTracker);
        } catch (ReadPastEndException | IncompleteEnvelopeReadException | InvalidLogEnvelopeReadException e) {
            // Make these exceptions slip by straight out to the outer handler
            throw e;
        } catch (Exception e) { // Tag all other exceptions with log position and other useful information
            LogPosition position = positionMarker.newPosition();
            var message = e.getMessage() + ". At position " + position + " and entry version " + versionCode;
            if (e instanceof UnsupportedLogVersionException) {
                throw new UnsupportedLogVersionException(versionCode, message, e);
            }
            throw new IOException(message, e);
        }
    }

    @Override
    public LogPosition lastPosition() {
        return positionMarker.newPosition();
    }
}
