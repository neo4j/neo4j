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

import java.util.TimeZone;

/*
 * This class is used when reading legacy log entries and it will preserve their identifiers.
 * Identifiers will be use for reordering the log entries when migrating from older neo4j versions.
 */
public class IdentifiableLogEntry implements LogEntry
{
    private final LogEntry entry;
    private final int identifier;

    public IdentifiableLogEntry( LogEntry entry, int identifier )
    {
        this.entry = entry;
        this.identifier = identifier;
    }

    @Override
    public byte getType()
    {
        return entry.getType();
    }

    @Override
    public LogEntryVersion getVersion()
    {
        return entry.getVersion();
    }

    @Override
    public String toString( TimeZone timeZone )
    {
        return entry.toString( timeZone );
    }

    @Override
    public String timestamp( long timeWritten, TimeZone timeZone )
    {
        return entry.timestamp( timeWritten, timeZone );
    }

    @Override
    public <T extends LogEntry> T as()
    {
        return (T) entry;
    }

    public int getIdentifier()
    {
        return identifier;
    }

    public LogEntry getEntry()
    {
        return entry;
    }

    @Override
    public String toString()
    {
        return "IdentifiableLogEntry{" +
                "identifier=" + identifier +
                ", entry=" + entry +
                '}';
    }
}
