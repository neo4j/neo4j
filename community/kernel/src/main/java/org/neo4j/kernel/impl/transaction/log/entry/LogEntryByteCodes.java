/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

public class LogEntryByteCodes
{
    private LogEntryByteCodes()
    {
        // no instances are allowed
    }

    // empty record due to memory mapped file
    public static final byte EMPTY = (byte) 0;

    // Real entries
    public static final byte TX_START = (byte) 1;
    public static final byte TX_PREPARE = (byte) 2;
    public static final byte COMMAND = (byte) 3;
    public static final byte DONE = (byte) 4;
    public static final byte TX_1P_COMMIT = (byte) 5;
    public static final byte TX_2P_COMMIT = (byte) 6;
    public static final byte CHECK_POINT = (byte) 7;
}
