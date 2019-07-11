/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log.monitor;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

public class DefaultLogAppenderMonitor implements LogAppenderMonitor
{
    private final AtomicLong appendedBytes = new AtomicLong();
    private long lastObservedVersion = -1;

    @Override
    public void appendToLogFile( LogPosition beforePosition, LogPosition afterPosition )
    {
        if ( afterPosition.getLogVersion() != beforePosition.getLogVersion() )
        {
            throw new IllegalStateException( "Appending transaction to several log files is not supported." );
        }
        appendedBytes.addAndGet( afterPosition.getByteOffset() - beforePosition.getByteOffset() );
        //TODO:
        if ( lastObservedVersion != afterPosition.getLogVersion() )
        {
            appendedBytes.addAndGet( LogHeader.LOG_HEADER_SIZE );
        }
        lastObservedVersion = afterPosition.getLogVersion();
    }

    @Override
    public long appendedBytes()
    {
        return appendedBytes.get();
    }
}
