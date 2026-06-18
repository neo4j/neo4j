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
package org.neo4j.kernel.impl.transaction.log.enveloped;

import static java.lang.Math.max;
import static org.neo4j.internal.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.UNSPECIFIED_INDEX;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.UNSPECIFIED_TERM;
import static org.neo4j.kernel.impl.transaction.log.entry.TailUtils.checkTail;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.entry.TailUtils;
import org.neo4j.kernel.impl.transaction.log.entry.UnknownLogFormatException;
import org.neo4j.kernel.impl.transaction.log.entry.UnsupportedLogVersionException;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StoreIdentifier;

public class EnvelopedLogTailChecker {
    private final LogsRepository logsRepository;
    private final EnvelopedReadChannelAllocator readChannelAllocator;
    private final MemoryTracker memoryTracker;
    private final InternalLog log;

    @FunctionalInterface
    public interface EnvelopedReadChannelAllocator {
        EnvelopeReadChannel envelopedReadChannel(long logFileVersion) throws IOException;
    }

    public EnvelopedLogTailChecker(
            LogsRepository logsRepository,
            EnvelopedReadChannelAllocator readChannelAllocator,
            MemoryTracker memoryTracker,
            InternalLogProvider logProvider) {
        this.logsRepository = logsRepository;
        this.readChannelAllocator = readChannelAllocator;
        this.memoryTracker = memoryTracker;
        this.log = logProvider.getLog(EnvelopedLogTailChecker.class);
    }

    public record EnvelopedLogTailInfo(
            StoreIdentifier storeIdentifier,
            LogPosition lastValidatedPosition,
            long lastValidAppendIndex,
            long lastValidTerm,
            int lastValidChecksum,
            int segmentOffset,
            boolean createInitial,
            boolean brokenLastEntry) {

        public static EnvelopedLogTailInfo empty(StoreIdentifier identifier) {
            return new EnvelopedLogTailInfo(
                    identifier,
                    new LogPosition(0L, 0L),
                    UNSPECIFIED_INDEX,
                    UNSPECIFIED_TERM,
                    BASE_TX_CHECKSUM,
                    0,
                    true,
                    false);
        }
    }

    public EnvelopedLogTailInfo checkEnvelopedLogTail(long fromVersion) throws IOException {
        var versionsRange = logsRepository.logVersionsRange();
        if (versionsRange.isEmpty()) {
            return EnvelopedLogTailInfo.empty(StoreIdentifier.UNKNOWN);
        }
        versionsRange = LongRange.range(max(fromVersion, versionsRange.from()), versionsRange.to());
        var storeIdentifier = checkEnvelopedLogVersionSequence(versionsRange);
        // update log range again, previous check may have deleted the last file
        versionsRange = logsRepository.logVersionsRange();
        if (versionsRange.isEmpty()) {
            // Some logs exist, but not in range, so grab a single header for the store identifier.
            try (var metadata = new LogFilesMetadata(logsRepository)) {
                if (metadata.next()) {
                    return EnvelopedLogTailInfo.empty(metadata.get().logHeader().getStoreIdentifier());
                }
                return EnvelopedLogTailInfo.empty(StoreIdentifier.UNKNOWN);
            }
        }
        // reversed iteration
        for (long version = versionsRange.to(); version >= versionsRange.from(); version--) {
            try (EnvelopeReadChannel readChannel = readChannelAllocator.envelopedReadChannel(version)) {
                LogHeader logHeader = readChannel.logHeader();
                long lastValidAppendIndex = logHeader.getLastAppendIndex();
                long lastValidTerm = logHeader.getLastTerm();
                int lastValidChecksum = logHeader.getPreviousLogFileChecksum();
                LogPosition lastGoodPosition = null;
                boolean brokenLastEntry = false;
                try {
                    while (true) {
                        lastGoodPosition = readChannel.goToEndOfEntry();
                        lastValidAppendIndex = readChannel.entryIndex();
                        lastValidTerm = readChannel.currentTerm();
                        lastValidChecksum = readChannel.getChecksum();
                    }
                } catch (ReadPastEndException e) {
                    // reached end
                } catch (UnsupportedLogVersionException | IllegalStateException | InvalidLogEnvelopeReadException e) {
                    throw e;
                } catch (IncompleteEnvelopeReadException e) {
                    // This exception signals that the tail is already checked and the last entry is broken.
                    brokenLastEntry = true;
                } catch (IOException | RuntimeException e) {
                    LogPosition currentLogPosition = readChannel.getCurrentLogPosition();
                    // check if error was in the last entry, or is there anything else after that
                    checkTail(readChannel, currentLogPosition, e);
                    brokenLastEntry = true;
                }

                if (lastGoodPosition == null) { // haven't read a valid entry
                    if (version > versionsRange.from()) {
                        // can go back further
                        continue;
                    }
                    // first entry we have is incomplete, so just preserve header values and rebuild
                    return new EnvelopedLogTailInfo(
                            storeIdentifier,
                            new LogPosition(
                                    version, logHeader.getStartPosition().getByteOffset()),
                            logHeader.getLastAppendIndex(),
                            logHeader.getLastTerm(),
                            logHeader.getPreviousLogFileChecksum(),
                            0,
                            true,
                            false);
                }

                return new EnvelopedLogTailInfo(
                        storeIdentifier,
                        lastGoodPosition,
                        lastValidAppendIndex,
                        lastValidTerm,
                        lastValidChecksum,
                        readChannel.getPadAdjustedSegmentOffset(lastGoodPosition.getByteOffset()),
                        false,
                        brokenLastEntry);
            }
        }
        return EnvelopedLogTailInfo.empty(storeIdentifier);
    }

    private StoreIdentifier checkEnvelopedLogVersionSequence(LongRange versionRange) throws IOException {
        long expectedLogVersion = versionRange.from();
        long lastHeaderTerm = -1L;
        long lastHeaderAppendIndex = -1L;
        StoreIdentifier storeIdentifier = null;
        LogFormat lastLogFormat = LogFormat.V10;

        for (long version = versionRange.from(); version <= versionRange.to(); version++) {
            try (LogChannelContext<StoreChannel> channel = logsRepository.openReadChannel(version)) {
                LogHeader logHeader;
                try {
                    logHeader = LogHeaderReader.readLogHeader(channel.channel(), true, channel.path(), memoryTracker);
                } catch (UnknownLogFormatException e) {
                    throw new IllegalStateException(
                            "Log File: " + logsRepository.pathFor(version) + " doesn't have a recognised LogFormat", e);
                } catch (IOException e) {
                    logHeader = null;
                }
                if (logHeader == null) {
                    // preallocated file, or corrupt?
                    long thisVersion = version;
                    TailUtils.checkNonZerosAfterOffset(
                            LogFormat.BIGGEST_HEADER,
                            channel.channel(),
                            memoryTracker,
                            safeCastLongToInt(kibiBytes(64)),
                            true,
                            (offset, data) -> {
                                throw new IllegalStateException("Log File: " + logsRepository.pathFor(thisVersion)
                                        + " doesn't have a valid LogHeader, but also contains non-zero data"
                                        + " and may be corrupted");
                            });
                    if (version != versionRange.to()) {
                        // only last file can have a partial header
                        throw new InconsistentLogFilesException("Log File: " + logsRepository.pathFor(version)
                                + " has incomplete header, or is preallocated, but is not last in the log file"
                                + " sequence ending with version "
                                + versionRange.to());
                    }

                    log.info("Removing last partial header/preallocated log file: " + logsRepository.pathFor(version));
                    // truncate away the additional file to stop it causing problems later
                    logsRepository.deleteLogFilesFrom(version);
                    return storeIdentifier;
                }

                // must be enveloped
                if (!logHeader.getLogFormatVersion().usesSegments()) {
                    throw new InconsistentLogFilesException(
                            "Log File: " + logsRepository.pathFor(version) + " is not using Envelopes as required");
                }

                // No format downgrades allowed
                if (logHeader.getLogFormatVersion().getVersionByte() < lastLogFormat.getVersionByte()) {
                    throw new InconsistentLogFilesException("Log File: " + logsRepository.pathFor(version)
                            + " uses LogFormat: " + logHeader.getLogFormatVersion()
                            + " but previous file used higher LogFormat: " + lastLogFormat);
                }
                lastLogFormat = logHeader.getLogFormatVersion();

                // Enveloped files must contain at least the header segment
                if (channel.channel().size() < logHeader.getSegmentBlockSize()) {
                    throw new IllegalStateException("Log File: " + logsRepository.pathFor(version)
                            + " does not contain a complete initial segment");
                }

                // we require matching header log version
                if (logHeader.getLogVersion() != expectedLogVersion) {
                    throw new InconsistentLogFilesException(
                            "Log File: " + logsRepository.pathFor(version) + " contains header " + logHeader
                                    + " with mismatched logVersion. Expected: " + expectedLogVersion);
                }

                // appendIndex must be increasing
                if (logHeader.getLastAppendIndex() < lastHeaderAppendIndex) {
                    throw new InconsistentLogFilesException("Log File: " + logsRepository.pathFor(version)
                            + " has lower previousAppendIndex: " + logHeader.getLastAppendIndex()
                            + " than previous file with header appendIndex: " + lastHeaderAppendIndex);
                }
                lastHeaderAppendIndex = logHeader.getLastAppendIndex();

                // term must be increasing
                if (logHeader.getLastTerm() < lastHeaderTerm) {
                    throw new InconsistentLogFilesException("Log File: " + logsRepository.pathFor(version)
                            + " has lower previousTerm: " + logHeader.getLastTerm()
                            + " than previous file with header previousTerm: " + lastHeaderTerm);
                }
                lastHeaderTerm = logHeader.getLastTerm();

                if (logHeader.getStoreIdentifier() == null
                        || (storeIdentifier != null && !storeIdentifier.equals(logHeader.getStoreIdentifier()))) {
                    throw new InconsistentLogFilesException("Log File: " + logsRepository.pathFor(version)
                            + " has inconsistent storeIdentifier: " + logHeader.getStoreIdentifier()
                            + " than previous file with storeIdentifier: " + storeIdentifier);
                }
                storeIdentifier = logHeader.getStoreIdentifier();
            } catch (NoSuchFileException e) {
                throw new InconsistentLogFilesException("Missing log file: "
                        + logsRepository.pathFor(expectedLogVersion) + " expected for version " + expectedLogVersion);
            }
            ++expectedLogVersion;
        }
        return storeIdentifier;
    }
}
