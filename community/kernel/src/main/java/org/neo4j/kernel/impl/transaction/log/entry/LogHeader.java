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

public class LogHeader
{
    public static final int LOG_HEADER_SIZE = 16;

    public final byte logFormatVersion;
    public final long logVersion;
    public final long lastCommittedTxId;

    public LogHeader( byte logFormatVersion, long logVersion, long lastCommittedTxId )
    {
        this.logFormatVersion = logFormatVersion;
        this.logVersion = logVersion;
        this.lastCommittedTxId = lastCommittedTxId;
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

        LogHeader logHeader = (LogHeader) o;
        return lastCommittedTxId == logHeader.lastCommittedTxId && logFormatVersion == logHeader.logFormatVersion &&
               logVersion == logHeader.logVersion;
    }

    @Override
    public int hashCode()
    {
        int result = (int) logFormatVersion;
        result = 31 * result + (int) (logVersion ^ (logVersion >>> 32));
        result = 31 * result + (int) (lastCommittedTxId ^ (lastCommittedTxId >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "LogHeader{" +
                "logFormatVersion=" + logFormatVersion +
                ", logVersion=" + logVersion +
                ", lastCommittedTxId=" + lastCommittedTxId +
                '}';
    }
}
