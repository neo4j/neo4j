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

import static org.neo4j.kernel.KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_INTRODUCED;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

import java.io.IOException;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryRollback;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.util.FeatureToggles;

/**
 * Reads {@link LogEntry log entries} off of a channel. Supported versions can be read intermixed.
 */
public class VersionAwareLogEntryReader implements LogEntryReader {
    private static final boolean VERIFY_CHECKSUM_CHAIN =
            FeatureToggles.flag(LogEntryReader.class, "verifyChecksumChain", false);
    private final CommandReaderFactory commandReaderFactory;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;
    private final LogPositionMarker positionMarker;
    private final boolean verifyChecksumChain;
    private LogEntrySerializationSet parserSet;
    private int lastTxChecksum = BASE_TX_CHECKSUM;

    public VersionAwareLogEntryReader(
            CommandReaderFactory commandReaderFactory, BinarySupportedKernelVersions binarySupportedKernelVersions) {
        this(commandReaderFactory, true, binarySupportedKernelVersions);
    }

    public VersionAwareLogEntryReader(
            CommandReaderFactory commandReaderFactory,
            boolean verifyChecksumChain,
            BinarySupportedKernelVersions binarySupportedKernelVersions) {
        this.commandReaderFactory = commandReaderFactory;
        this.positionMarker = new LogPositionMarker();
        this.verifyChecksumChain = verifyChecksumChain;
        this.binarySupportedKernelVersions = binarySupportedKernelVersions;
    }

    @Override
    public LogEntry readLogEntry(ReadableLogPositionAwareChannel channel) throws IOException {
        try {
            byte versionCode = channel.markAndGetVersion(positionMarker);
            if (versionCode == 0) {
                // we reached the end of available records but still have space available in pre-allocated file
                // we reset channel position to restore last read byte in case someone would like to re-read or check it
                // again if possible
                // and we report that we reach end of record stream from our point of view
                rewindOneByte(channel);
                return null;
            }
            updateParserSet(channel, versionCode);

            byte typeCode = channel.get();
            LogEntry entry = readEntry(channel, versionCode, typeCode);
            verifyChecksumChain(entry);
            return entry;
        } catch (ReadPastEndException e) {
            return null;
        }
    }

    private void updateParserSet(ReadableLogPositionAwareChannel channel, byte versionCode) throws IOException {
        if (parserSet != null && parserSet.getIntroductionVersion().version() == versionCode) {
            return; // We already have the correct parser set
        }
        try {
            KernelVersion kernelVersion = KernelVersion.getForVersion(versionCode);
            parserSet = LogEntrySerializationSets.serializationSet(kernelVersion, binarySupportedKernelVersions);

            if (kernelVersion.isLessThan(VERSION_ENVELOPED_TRANSACTION_LOGS_INTRODUCED)) {
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

    private LogEntry readEntry(ReadableLogPositionAwareChannel channel, byte versionCode, byte typeCode)
            throws IOException {
        try {
            return parserSet
                    .select(typeCode)
                    .parse(
                            parserSet.getIntroductionVersion(),
                            parserSet.wrap(channel),
                            positionMarker,
                            commandReaderFactory);
        } catch (ReadPastEndException e) { // Make these exceptions slip by straight out to the outer handler
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

    private void verifyChecksumChain(LogEntry e) {
        if (VERIFY_CHECKSUM_CHAIN && verifyChecksumChain) {
            if (e instanceof LogEntryStart logEntryStart) {
                int previousChecksum = logEntryStart.getPreviousChecksum();
                if (lastTxChecksum != BASE_TX_CHECKSUM) {
                    if (previousChecksum != lastTxChecksum) {
                        throw new IllegalStateException("The checksum chain is broken. " + positionMarker);
                    }
                }
            } else if (e instanceof LogEntryCommit logEntryCommit) {
                lastTxChecksum = logEntryCommit.getChecksum();
            } else if (e instanceof LogEntryRollback rollback) {
                lastTxChecksum = rollback.getChecksum();
            }
        }
    }

    private void rewindOneByte(ReadableLogPositionAwareChannel channel) throws IOException {
        // take current position
        channel.position(channel.position() - 1);
        // refresh with reset position
        channel.getCurrentLogPosition(positionMarker);
    }

    @Override
    public LogPosition lastPosition() {
        return positionMarker.newPosition();
    }
}
