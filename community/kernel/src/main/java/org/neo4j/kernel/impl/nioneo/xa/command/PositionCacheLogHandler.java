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
package org.neo4j.kernel.impl.nioneo.xa.command;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;

public class PositionCacheLogHandler extends LogHandler.Filter
{
    private LogEntry.Start startEntry;

    public interface SPI
    {
        public long getLogVersion();
    }

    private final LogExtractor.LogPositionCache positionCache;
    private final SPI spi;

    public PositionCacheLogHandler( XaLogicalLog.LogApplier applier, LogExtractor.LogPositionCache positionCache, SPI spi )
    {
        super( applier );
        this.positionCache = positionCache;
        this.spi = spi;
    }

    @Override
    public void startEntry( LogEntry.Start startEntry ) throws IOException
    {
        super.startEntry( startEntry );
        this.startEntry = startEntry;
    }

    @Override
    public void onePhaseCommitEntry( LogEntry.OnePhaseCommit onePhaseCommitEntry ) throws IOException
    {
        super.onePhaseCommitEntry( onePhaseCommitEntry );
        assert startEntry != null;
        positionCache.cacheStartPosition( onePhaseCommitEntry.getTxId(), startEntry, spi.getLogVersion() );
        startEntry = null;
    }

    @Override
    public void twoPhaseCommitEntry( LogEntry.TwoPhaseCommit twoPhaseCommitEntry ) throws IOException
    {
        super.twoPhaseCommitEntry( twoPhaseCommitEntry );
        assert startEntry != null;
        positionCache.cacheStartPosition( twoPhaseCommitEntry.getTxId(), startEntry, spi.getLogVersion() );
        startEntry = null;
    }
}
