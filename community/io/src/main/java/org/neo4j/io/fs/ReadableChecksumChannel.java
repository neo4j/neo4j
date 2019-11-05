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
package org.neo4j.io.fs;

import java.io.IOException;

/**
 * Extends the {@link ReadableChannel} with a way to validate checksum over parts. The checksum will be calculated over all of the {@code .get*()} methods.
 */
public interface ReadableChecksumChannel extends ReadableChannel
{
    /**
     * Mark position from where checksum should be calculated from.
     */
    void beginChecksum();

    /**
     * Mark the end for checksum calculations. This method will go through a number of stages:
     * <ul>
     *     <li>Calculate the checksum over the stream since the call to {@link #beginChecksum()}.
     *     <li>Read the checksum at the current position.
     *     <li>Validate the checksum. Throws {@link ChecksumMismatchException} if not.
     *
     * @return the checksum that was validated.
     * @throws IOException I/O error from channel.
     * @throws ChecksumMismatchException if the checksum do not match.
     */
    int endChecksumAndValidate() throws IOException;
}
