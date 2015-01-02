/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.store.windowpool;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;

import org.neo4j.consistency.store.paging.PageLoadFailureException;
import org.neo4j.consistency.store.paging.PageReplacementStrategy;
import org.neo4j.kernel.impl.nioneo.store.OperationType;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindow;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPool;

public class ScanResistantWindowPool implements WindowPool,
        PageReplacementStrategy.Storage<PersistenceWindow, WindowPage>
{
    private final File storeFileName;
    private final FileMapper fileMapper;
    private final PageReplacementStrategy replacementStrategy;
    private final int bytesPerRecord;
    private final int recordsPerPage;
    private final int reportInterval;
    private final MappingStatisticsListener statisticsListener;

    private WindowPage[] pages = new WindowPage[0];
    private int acquireCount = 0;
    private int mapCount = 0;

    public ScanResistantWindowPool( File storeFileName, int bytesPerRecord, int targetBytesPerPage,
                                    FileMapper fileMapper, PageReplacementStrategy replacementStrategy,
                                    int reportInterval, MappingStatisticsListener statisticsListener )
            throws IOException
    {
        this.storeFileName = storeFileName;
        this.bytesPerRecord = bytesPerRecord;
        this.fileMapper = fileMapper;
        this.replacementStrategy = replacementStrategy;
        this.statisticsListener = statisticsListener;
        this.recordsPerPage = calculateNumberOfRecordsPerPage( bytesPerRecord, targetBytesPerPage );
        this.reportInterval = reportInterval;
        this.setupPages();
    }

    private static int calculateNumberOfRecordsPerPage( int bytesPerRecord, int targetBytesPerPage )
    {
        if ( bytesPerRecord <= 0 || bytesPerRecord > targetBytesPerPage )
        {
            throw new IllegalArgumentException( format( "number of bytes per record [%d] " +
                    "is not in the valid range [1-%d]", bytesPerRecord, targetBytesPerPage ) );
        }
        return targetBytesPerPage / bytesPerRecord;
    }

    private void setupPages() throws IOException
    {
        // pre-allocate pages that exist already
        page( fileMapper.fileSizeInBytes() / bytesPerRecord );
    }

    private int pageNumber( long position )
    {
        long pageNumber = position / recordsPerPage;
        if ( pageNumber + 1 > Integer.MAX_VALUE )
        {
            throw new IllegalArgumentException( format( "Position [record %d] with current page size [%d records/page]"
                    + " implies an impossible page number [%d].", position, recordsPerPage, pageNumber ) );
        }
        return (int) (position / recordsPerPage);
    }

    private WindowPage page( long position )
    {
        int pageNumber = pageNumber( position );
        if ( pageNumber >= pages.length )
        {
            WindowPage[] newPages = new WindowPage[pageNumber + 1];
            System.arraycopy( pages, 0, newPages, 0, pages.length );
            for ( int i = pages.length; i < newPages.length; i++ )
            {
                newPages[i] = new WindowPage( i * (long) recordsPerPage );
            }
            pages = newPages;
        }
        return pages[pageNumber];
    }

    @Override
    public PersistenceWindow acquire( long position, OperationType operationType )
    {
        if ( operationType != OperationType.READ )
        {
            throw new UnsupportedOperationException( "Only supports READ operations." );
        }
        try
        {
            acquireCount++;
            return replacementStrategy.acquire( page( position ), this );
        }
        catch ( PageLoadFailureException e )
        {
            throw new UnderlyingStorageException( "Unable to load position["
                    + position + "] @[" + position * bytesPerRecord + "]", e );
        }
        finally
        {
            reportStats();
        }
    }

    private int lastMapCount;
    private long lastReportTime = System.currentTimeMillis();

    private void reportStats()
    {
        if ( acquireCount % reportInterval == 0 )
        {
            int deltaMapCount = mapCount - lastMapCount;
            lastMapCount = mapCount;
            long currentTime = System.currentTimeMillis();
            long deltaTime = currentTime - lastReportTime;
            lastReportTime = currentTime;

            statisticsListener.onStatistics( storeFileName, reportInterval, deltaMapCount, deltaTime );
        }
    }

    @Override
    public void release( PersistenceWindow window )
    {
        // we're not using lockable windows, so no action required
    }

    @Override
    public void flushAll()
    {
        // current implementation is read-only, so no need to flush
    }

    @Override
    public void close()
    {
        for ( WindowPage page : pages )
        {
            replacementStrategy.forceEvict( page );
        }
    }

    @Override
    public WindowPoolStats getStats()
    {
        return new WindowPoolStats( storeFileName, 0, 0, pages.length,
                bytesPerRecord * recordsPerPage, acquireCount - mapCount, mapCount, 0, 0, 0, 0, 0 );
    }

    @Override
    public PersistenceWindow load( WindowPage page ) throws PageLoadFailureException
    {
        try
        {
            mapCount++;
            return fileMapper.mapWindow( page.firstRecord, recordsPerPage, bytesPerRecord );
        }
        catch ( IOException e )
        {
            throw new PageLoadFailureException( e );
        }
    }
}
