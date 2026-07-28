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

import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.log.LogFormatVersionProvider;
import org.neo4j.kernel.impl.transaction.log.LogTermProvider;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.LogVersionRepository;

public class TransactionLogFilesProviders {
    private final LogMetadataProvider logMetadataProvider;
    private final TransactionLogFilesOverrides overrides;
    private final AppendIndexProvider lastAppendIndexProvider;
    private final LastCommittedChecksumProvider lastCommittedChecksumProvider;
    private final LogVersionRepository logVersionRepository;
    private final LastClosedPositionProvider lastClosedPositionProvider;

    public TransactionLogFilesProviders(
            LogMetadataProvider logMetadataProvider, TransactionLogFilesOverrides overrides) {
        this.logMetadataProvider = logMetadataProvider;
        this.overrides = overrides;
        logVersionRepository = setupLogVersionRepository();
        lastAppendIndexProvider = lastAppendIndexProvider();
        lastClosedPositionProvider = closePositionProvider();
        lastCommittedChecksumProvider = lastCommittedChecksumProvider();
    }

    private LastCommittedChecksumProvider lastCommittedChecksumProvider() {
        if (overrides.lastCommittedChecksumProvider() != null) {
            return () -> overrides.lastCommittedChecksumProvider().getAsInt();
        }
        if (overrides.transactionIdStore() != null) {
            return () ->
                    overrides.transactionIdStore().getLastCommittedTransaction().checksum();
        }

        return () -> logMetadataProvider.getLastCommittedTransaction().checksum();
    }

    private LogVersionRepository setupLogVersionRepository() {
        if (overrides.logVersionRepository() != null) {
            return overrides.logVersionRepository();
        }
        return logMetadataProvider;
    }

    public LogVersionRepository getLogVersionRepository() {
        return logVersionRepository;
    }

    public long appendIndex() {
        return lastAppendIndexProvider.getLastAppendIndex();
    }

    LastClosedPositionProvider getLastClosedTransactionPositionProvider() {
        return lastClosedPositionProvider;
    }

    public LastCommittedChecksumProvider getLastCommittedChecksumProvider() {
        return lastCommittedChecksumProvider;
    }

    public KernelVersionProvider getKernelVersionProvider() {
        if (overrides.kernelVersionProvider() != null) {
            return overrides.kernelVersionProvider();
        }
        return logMetadataProvider;
    }

    public LogFormatVersionProvider getLogFormatVersionProvider() {
        if (overrides.logFormatVersionProvider() != null) {
            return overrides.logFormatVersionProvider();
        }
        return logMetadataProvider;
    }

    public LogTermProvider getLogTermProvider() {
        return logMetadataProvider;
    }

    private AppendIndexProvider lastAppendIndexProvider() {
        if (overrides.appendIndexProvider() != null) {
            return overrides.appendIndexProvider();
        }
        return logMetadataProvider;
    }

    private LastClosedPositionProvider closePositionProvider() {
        if (overrides.transactionIdStore() != null) {
            return () -> overrides
                    .transactionIdStore()
                    .getHighestGapFreeClosedTransaction()
                    .logPosition();
        }
        return () -> logMetadataProvider.getHighestGapFreeClosedTransaction().logPosition();
    }
}
