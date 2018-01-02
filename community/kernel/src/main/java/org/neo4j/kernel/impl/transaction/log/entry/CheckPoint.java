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

import org.neo4j.kernel.impl.transaction.log.LogPosition;

public class CheckPoint extends AbstractLogEntry
{
    private final LogPosition logPosition;

    public CheckPoint( LogPosition logPosition )
    {
        this( LogEntryVersion.CURRENT, logPosition );
    }

    public CheckPoint( LogEntryVersion version, LogPosition logPosition )
    {
        super( version, LogEntryByteCodes.CHECK_POINT );
        this.logPosition = logPosition;
    }

    @Override
    public <T extends LogEntry> T as()
    {
        return (T) this;
    }


    public LogPosition getLogPosition()
    {
        return logPosition;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        CheckPoint that = (CheckPoint) o;

        return !(logPosition != null ? !logPosition.equals( that.logPosition ) : that.logPosition != null);
    }

    @Override
    public int hashCode()
    {
        return logPosition != null ? logPosition.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return "CheckPoint[" +
               "position=" + logPosition +
               ']';
    }
}
