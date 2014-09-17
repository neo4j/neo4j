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
package org.neo4j.io.pagecache.impl;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.Page;
import org.neo4j.io.pagecache.PageEvictionCallback;

/**
 * This class exist because of a JDK bug in Java 7, that can lead to
 * threads deadlocking in FileChannelImpl.pread0():
 *
 * https://bugs.openjdk.java.net/browse/JDK-8012019
 *
 * TODO: This has been fixed in Java 8, so this workaround can be removed when we upgrade.
 */
public class WindowsSingleFilePageSwapper extends SingleFilePageSwapper
{
    public WindowsSingleFilePageSwapper(
            File file,
            StoreChannel channel,
            int filePageSize,
            PageEvictionCallback onEviction )
    {
        super( file, channel, filePageSize, onEviction );
    }

    @Override
    public synchronized void read( long filePageId, Page page ) throws IOException
    {
        super.read( filePageId, page );
    }

    @Override
    public synchronized void write( long filePageId, Page page ) throws IOException
    {
        super.write( filePageId, page );
    }

    @Override
    public synchronized void close() throws IOException
    {
        super.close();
    }

    @Override
    public synchronized void force() throws IOException
    {
        super.force();
    }

    @Override
    public synchronized long getLastPageId() throws IOException
    {
        return super.getLastPageId();
    }

    @Override
    public synchronized boolean equals( Object o )
    {
        return super.equals( o );
    }

    @Override
    public synchronized int hashCode()
    {
        return super.hashCode();
    }
}
