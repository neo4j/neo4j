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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.IOException;

import org.neo4j.io.pagecache.Page;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageSwapper;

final class MonitoredPageSwapper implements PageSwapper
{
    private final PageSwapper pageSwapper;
    private final PageCacheMonitor monitor;

    public MonitoredPageSwapper( PageSwapper pageSwapper, PageCacheMonitor monitor )
    {
        this.pageSwapper = pageSwapper;
        this.monitor = monitor;
    }

    @Override
    public void read( long filePageId, Page page ) throws IOException
    {
        pageSwapper.read( filePageId, page );
    }

    @Override
    public void write( long filePageId, Page page ) throws IOException
    {
        pageSwapper.write( filePageId, page );
        monitor.flushed(filePageId, pageSwapper);
    }

    @Override
    public void evicted( long pageId, Page page )
    {
        pageSwapper.evicted( pageId, page );
    }

    @Override
    public String fileName()
    {
        return pageSwapper.fileName();
    }

    @Override
    public void close() throws IOException
    {
        pageSwapper.close();
    }

    @Override
    public void force() throws IOException
    {
        pageSwapper.force();
    }

    @Override
    public long getLastPageId() throws IOException
    {
        return pageSwapper.getLastPageId();
    }

    @Override
    public String toString()
    {
        return pageSwapper.toString() + "[*Monitored]";
    }
}
