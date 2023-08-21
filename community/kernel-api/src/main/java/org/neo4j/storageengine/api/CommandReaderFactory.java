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
package org.neo4j.storageengine.api;

import org.neo4j.kernel.KernelVersion;

/**
 * Provides {@link CommandReader} instances for specific versions.
 */
public interface CommandReaderFactory {
    /**
     * Returns a {@link CommandReader} able to read commands of {@code logEntryVersion}. Previously the log entry version was coupled
     * with the command version, and to keep backwards compatibility this log entry version is still passed in here.
     * Command writers/readers may choose to use log entry version for command versioning or (should) introduce its own versioning.
     *
     * In the future when there's no longer any storage engine and version that makes use of this logEntryVersion for reading its
     * commands then this {@link CommandReaderFactory} interface could be removed entirely.
     *
     * @param version kernel version.
     * @return {@link CommandReader} for reading commands of that version.
     * @throws IllegalArgumentException on invalid or unrecognized version.
     */
    CommandReader get(KernelVersion version);

    CommandReaderFactory NO_COMMANDS = version -> {
        throw new IllegalArgumentException("No commands supported");
    };
}
