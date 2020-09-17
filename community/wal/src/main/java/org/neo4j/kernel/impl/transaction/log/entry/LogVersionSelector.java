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

import org.neo4j.util.Preconditions;

public abstract class LogVersionSelector
{
    private final ByteObjectHashMap<LogEntryParserSet> sets;
    private final byte latestVersion;

    protected LogVersionSelector( byte latestVersion )
    {
        this.latestVersion = latestVersion;
        sets = new ByteObjectHashMap<>();
    }

    protected void register( LogEntryParserSet set )
    {
        byte version = set.versionByte();
        Preconditions.checkState( !sets.containsKey( version ), "Conflicting version %d", version );
        sets.put( version, set );
    }

    public LogEntryParserSet select( byte version )
    {
        LogEntryParserSet set = sets.get( version );
        if ( set != null )
        {
            return set;
        }

        if ( version > latestVersion )
        {
            throw new UnsupportedLogVersionException( String.format(
                    "Log file contains entries with prefix %d, and the highest supported prefix is %d. This " +
                            "indicates that the log files originates from a newer version of neo4j.",
                    version, latestVersion ) );
        }
        throw new UnsupportedLogVersionException( String.format(
                "Log file contains entries with prefix %d, and the lowest supported prefix is %d. This " +
                        "indicates that the log files originates from an older version of neo4j, which we don't support " +
                        "migrations from.",
                version, sets.keySet().min() ) );
    }

    public LogEntryParserSet select( LogEntryParserSetVersion parserSetVersion )
    {
        return select( parserSetVersion.getVersionByte() );
    }

    /**
     * Check if a more recent version of the log entry format exists and can be handled.
     *
     * @param version to compare against latest version
     * @return {@code true} if a more recent log entry version exists
     */
    public boolean moreRecentVersionExists( byte version )
    {
        return version < latestVersion;
    }
}
