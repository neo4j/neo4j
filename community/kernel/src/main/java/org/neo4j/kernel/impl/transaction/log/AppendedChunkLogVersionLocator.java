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

import java.util.Optional;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogHeaderVisitor;

public class AppendedChunkLogVersionLocator implements LogHeaderVisitor {
    private final long appendIndex;
    private LogPosition foundPosition;

    public AppendedChunkLogVersionLocator(long appendIndex) {
        this.appendIndex = appendIndex;
    }

    @Override
    public boolean visit(LogHeader logHeader, LogPosition position, long lowApplyIndex, long lastApplyIndex) {
        boolean foundIt = appendIndex >= lowApplyIndex && appendIndex <= lastApplyIndex;
        if (foundIt) {
            foundPosition = position;
        }
        return !foundIt; // continue as long we don't find it
    }

    public LogPosition getLogPositionOrThrow() throws NoSuchLogEntryException {
        if (foundPosition == null) {
            throw new NoSuchLogEntryException(appendIndex);
        }
        return foundPosition;
    }

    public Optional<LogPosition> getOptionalLogPosition() {
        return Optional.ofNullable(foundPosition);
    }
}
