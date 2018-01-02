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
package org.neo4j.kernel.impl.transaction.log;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

public class LogPosition implements Comparable<LogPosition>
{
    public static final LogPosition UNSPECIFIED = new LogPosition( -1, -1 )
    {
        @Override
        public long getLogVersion()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getByteOffset()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "UNSPECIFIED";
        }
    };

    public static LogPosition start( long logVersion )
    {
        return new LogPosition( logVersion, LOG_HEADER_SIZE );
    }

    private final long logVersion;
    private final long byteOffset;

    public LogPosition( long logVersion, long byteOffset )
    {
        this.logVersion = logVersion;
        this.byteOffset = byteOffset;
    }

    public long getLogVersion()
    {
        return logVersion;
    }

    public long getByteOffset()
    {
        return byteOffset;
    }

    @Override
    public String toString()
    {
        return "LogPosition{" +
                "logVersion=" + logVersion +
                ", byteOffset=" + byteOffset +
                '}';
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

        LogPosition that = (LogPosition) o;

        if ( byteOffset != that.byteOffset )
        {
            return false;
        }
        if ( logVersion != that.logVersion )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (logVersion ^ (logVersion >>> 32));
        result = 31 * result + (int) (byteOffset ^ (byteOffset >>> 32));
        return result;
    }

    @Override
    public int compareTo( LogPosition o )
    {
        if ( logVersion != o.logVersion )
        {
            return (int) (logVersion - o.logVersion);
        }

        return (int) (byteOffset - o.byteOffset);
    }
}
