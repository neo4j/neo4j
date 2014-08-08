/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework.log.entry;

import java.io.IOException;
import java.util.TimeZone;

import org.neo4j.helpers.Format;
import org.neo4j.kernel.impl.nioneo.xa.command.LogHandler;

public abstract class LogEntry
{
    public static final int MASTER_ID_REPRESENTING_NO_MASTER = -1;

    /* version 1 as of 2011-02-22
     * version 2 as of 2011-10-17
     * version 3 as of 2013-02-09: neo4j 2.0 Labels & Indexing
     * version 4 as of 2014-02-06: neo4j 2.1 Dense nodes, split by type/direction into groups
     * version 5 as of 2014-05-23: neo4j 2.2 Removal of JTA / unified data source
     */
    public static final byte CURRENT_LOG_VERSION = (byte) 5;

    /*
     * version 0 for Neo4j versions < 2.1
     * version -1 for Neo4j 2.1
     */
    public static final byte CURRENT_LOG_ENTRY_VERSION = (byte) -1;

    // empty record due to memory mapped file
    public static final byte EMPTY = (byte) 0;

    // Real entries
    public static final byte TX_START = (byte) 1;
    public static final byte COMMAND = (byte) 3;
    public static final byte TX_1P_COMMIT = (byte) 5;

    private final byte type;
    private final byte version;

    LogEntry( byte type, byte version )
    {
        this.type = type;
        this.version = version;
    }

    public abstract void accept( LogHandler handler ) throws IOException;

    public byte getType()
    {
        return type;
    }

    public byte getVersion()
    {
        return version;
    }

    public String toString( TimeZone timeZone )
    {
        return toString();
    }

    public String timestamp( long timeWritten, TimeZone timeZone )
    {
        return Format.date( timeWritten, timeZone ) + "/" + timeWritten;
    }
}
