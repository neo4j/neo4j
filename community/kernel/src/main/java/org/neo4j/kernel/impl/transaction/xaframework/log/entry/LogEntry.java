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
