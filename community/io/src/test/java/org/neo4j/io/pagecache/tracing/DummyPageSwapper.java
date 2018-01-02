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
package org.neo4j.io.pagecache.tracing;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.Page;
import org.neo4j.io.pagecache.PageSwapper;

public class DummyPageSwapper implements PageSwapper
{
    private final String filename;

    public DummyPageSwapper( String filename )
    {
        this.filename = filename;
    }

    @Override
    public long read( long filePageId, Page page ) throws IOException
    {
        return 0;
    }

    @Override
    public long write( long filePageId, Page page ) throws IOException
    {
        return 0;
    }

    @Override
    public void evicted( long pageId, Page page )
    {
    }

    @Override
    public File file()
    {
        return new File( filename );
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public void force() throws IOException
    {
    }

    @Override
    public long getLastPageId() throws IOException
    {
        return 0;
    }

    @Override
    public void truncate() throws IOException
    {
    }

    @Override
    public long read( long startFilePageId, Page[] pages, int arrayOffset, int length ) throws IOException
    {
        return 0;
    }

    @Override
    public long write( long startFilePageId, Page[] pages, int arrayOffset, int length ) throws IOException
    {
        return 0;
    }
}
