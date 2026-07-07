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

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.UNSPECIFIED_CREATION_TIME;

import java.nio.file.Path;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;

/**
 * Adapts a {@link LogFileMetadata} entry into the {@link LogFileInformation} view consumed by threshold
 * predicates. Mtime captured at the point of construction can substitute for the legacy "first start record timestamp"
 * when using log versions that do not carry a timestamp in their header.
 */
record EnvelopedLogFileInformation(LogFileMetadata metadata, long lastModifiedMillis) implements LogFileInformation {

    @Override
    public long version() {
        return metadata.version();
    }

    @Override
    public Path path() {
        return metadata.path();
    }

    @Override
    public long getPreviousAppendIndexFromHeader() {
        return metadata.logHeader().getLastAppendIndex();
    }

    @Override
    public long getFirstStartRecordTimestamp() {
        if (metadata.logHeader().getCreationTime() == UNSPECIFIED_CREATION_TIME) {
            if (metadata.logHeader().getLogVersion() >= 11) {
                throw new IllegalStateException("Creation time should be specified from V11 logs onwards, but wasn't");
            }
            return lastModifiedMillis;
        }
        return metadata.logHeader().getCreationTime();
    }
}
