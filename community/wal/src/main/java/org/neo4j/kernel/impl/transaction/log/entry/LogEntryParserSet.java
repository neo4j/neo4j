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

import org.eclipse.collections.impl.map.mutable.primitive.ByteObjectHashMap;

import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.util.Preconditions;

/**
 * Set of log entry parsers for a specific version/layout of these entries. Typically such a set contains parsers for log entries such as:
 * START, COMMAND, COMMIT and CHECKPOINT (see {@link LogEntryTypeCodes}, where each such type maps to a specific parser.
 * Versioning of commands (see {@link CommandReaderFactory} should be detached from versions of this set.
 */
public abstract class LogEntryParserSet
{
    private final LogEntryParserSetVersion version;
    private final ByteObjectHashMap<LogEntryParser> parsers = new ByteObjectHashMap<>();

    LogEntryParserSet( LogEntryParserSetVersion version )
    {
        this.version = version;
    }

    /**
     * Selects the correct log entry parser for the specific type, for type codes see {@link LogEntryTypeCodes}.
     * @param type type code for the log entry to parse.
     * @return parser able to read and parse log entry of this type.
     */
    public LogEntryParser select( byte type )
    {
        LogEntryParser parser = parsers.get( type );
        if ( parser == null )
        {
            throw new IllegalArgumentException( "Unknown entry type " + type + " for version " + version.getVersionByte() );
        }
        return parser;
    }

    protected void register( LogEntryParser parser )
    {
        byte type = parser.type();
        Preconditions.checkState( !parsers.containsKey( type ), "Already registered parser for type " + type );
        parsers.put( type, parser );
    }

    /**
     * @return the version of this log entry parser set.
     */
    public byte versionByte()
    {
        return version.getVersionByte();
    }

    public LogEntryParserSetVersion version()
    {
        return version;
    }
}
