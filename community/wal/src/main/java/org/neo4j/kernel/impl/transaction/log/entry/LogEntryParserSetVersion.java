/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

public enum LogEntryParserSetVersion
{
    LogEntryV2_3( (byte) -10 ), // 2.3 to 3.5.
    LogEntryV4_0( (byte) 1 ), // 4.0 to 4.1. Added checksums to the log files.
    LogEntryV4_2( (byte) 2 ), // 4.2+. Removed checkpoint entries.

    CheckpointEntryV4_2( (byte) 3 ); // 4.2+. Checkpoint entries in separate file.

    private final byte version;

    LogEntryParserSetVersion( byte version )
    {
        this.version = version;
    }

    public byte getVersionByte()
    {
        return version;
    }
}
