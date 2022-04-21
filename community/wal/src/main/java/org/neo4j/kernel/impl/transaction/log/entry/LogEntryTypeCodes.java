/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.log.entry;

public class LogEntryTypeCodes {
    private LogEntryTypeCodes() {
        throw new AssertionError(); // no instances are allowed
    }

    public static final byte TX_START = (byte) 1;
    public static final byte COMMAND = (byte) 3;
    public static final byte TX_COMMIT = (byte) 5;
    // type code 7 was used before 4.2 for inlined checkpoints log entries and got removed in 5.0
    // Detached check point log entries lives in a separate file
    public static final byte DETACHED_CHECK_POINT = (byte) 8;

    // Checkpoint that contains transaction info (tx id, checksum, commit timestamp)
    public static final byte DETACHED_CHECK_POINT_V5_0 = (byte) 9;
}
