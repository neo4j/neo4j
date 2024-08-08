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
package org.neo4j.kernel.impl.transaction.log;

import static java.lang.String.format;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.neo4j.common.Subject;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.StorageCommand;

public class CompleteCommandBatch implements CommandBatch {
    private final List<StorageCommand> commands;
    private final long timeStarted;
    private final long latestCommittedTxWhenStarted;
    private final long timeCommitted;
    private final KernelVersion kernelVersion;
    private final Subject subject;
    /**
     * This is a bit of a smell since it's only used for coordinating transactions in a cluster.
     * We may want to refactor this design later on.
     */
    private final int leaseId;

    private long consensusIndex;
    private long appendIndex = UNKNOWN_APPEND_INDEX;

    public CompleteCommandBatch(
            List<StorageCommand> commands,
            long consensusIndex,
            long timeStarted,
            long latestCommittedTxWhenStarted,
            long timeCommitted,
            int leaseId,
            KernelVersion kernelVersion,
            Subject subject) {
        this.commands = commands;
        this.consensusIndex = consensusIndex;
        this.timeStarted = timeStarted;
        this.latestCommittedTxWhenStarted = latestCommittedTxWhenStarted;
        this.timeCommitted = timeCommitted;
        this.leaseId = leaseId;
        this.kernelVersion = kernelVersion;
        this.subject = subject;
    }

    @Override
    public void setConsensusIndex(long consensusIndex) {
        this.consensusIndex = consensusIndex;
    }

    @Override
    public boolean accept(Visitor<StorageCommand, IOException> visitor) throws IOException {
        for (StorageCommand command : commands) {
            if (visitor.visit(command)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public long consensusIndex() {
        return consensusIndex;
    }

    @Override
    public long getTimeStarted() {
        return timeStarted;
    }

    @Override
    public long getLatestCommittedTxWhenStarted() {
        return latestCommittedTxWhenStarted;
    }

    @Override
    public long getTimeCommitted() {
        return timeCommitted;
    }

    @Override
    public int getLeaseId() {
        return leaseId;
    }

    @Override
    public Subject subject() {
        return subject;
    }

    @Override
    public KernelVersion kernelVersion() {
        return kernelVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompleteCommandBatch that = (CompleteCommandBatch) o;
        return latestCommittedTxWhenStarted == that.latestCommittedTxWhenStarted
                && timeStarted == that.timeStarted
                && consensusIndex == that.consensusIndex
                && appendIndex == that.appendIndex
                && commands.equals(that.commands);
    }

    @Override
    public int hashCode() {
        int result = commands.hashCode();
        result = 31 * result + Long.hashCode(consensusIndex);
        result = 31 * result + Long.hashCode(appendIndex);
        result = 31 * result + Long.hashCode(timeStarted);
        result = 31 * result + Long.hashCode(latestCommittedTxWhenStarted);
        return result;
    }

    @Override
    public String toString() {
        return toString(false);
    }

    @Override
    public String toString(boolean includeCommands) {
        String basic = format(
                "%s[timeStarted:%d, latestCommittedTxWhenStarted:%d, timeCommitted:%d, lease:%d, consensusIndex:%d, commands.length:%d",
                getClass().getSimpleName(),
                timeStarted,
                latestCommittedTxWhenStarted,
                timeCommitted,
                leaseId,
                consensusIndex,
                commands.size());
        if (!includeCommands) {
            return basic;
        }

        StringBuilder builder = new StringBuilder(basic);
        for (StorageCommand command : commands) {
            builder.append(format("%n%s", command.toString()));
        }
        return builder.toString();
    }

    @Override
    public boolean isLast() {
        return true;
    }

    @Override
    public boolean isFirst() {
        return true;
    }

    @Override
    public boolean isRollback() {
        return false;
    }

    @Override
    public Iterator<StorageCommand> iterator() {
        return commands.iterator();
    }

    @Override
    public int commandCount() {
        return commands.size();
    }

    @Override
    public long appendIndex() {
        if (appendIndex == UNKNOWN_APPEND_INDEX) {
            throw new IllegalStateException("Append index was not generated for the batch yet.");
        }
        return appendIndex;
    }

    @Override
    public void setAppendIndex(long appendIndex) {
        this.appendIndex = appendIndex;
    }
}
