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

public class LogEntryTypeCodes {
    private LogEntryTypeCodes() {
        throw new AssertionError(); // no instances are allowed
    }

    public static final byte TX_START = (byte) 1;
    public static final byte COMMAND = (byte) 3;
    public static final byte TX_COMMIT = (byte) 5;
    // Legacy inline checkpoint that can be encountered during migration of legacy databases
    public static final byte LEGACY_CHECK_POINT = (byte) 7;
    // Detached check point log entries lives in a separate file
    public static final byte DETACHED_CHECK_POINT = (byte) 8;

    // Checkpoint that contains transaction info
    public static final byte DETACHED_CHECK_POINT_V5_0 = (byte) 9;

    // chunked transactions entry codes
    public static final byte CHUNK_START = 10;
    public static final byte CHUNK_END = 11;

    // transaction roll back entry code
    public static final byte TX_ROLLBACK = 12;
}
