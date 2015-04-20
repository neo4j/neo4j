/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.function.Factory;
import org.neo4j.io.pagecache.Page;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PageSwappingTest;

public class MuninnPageSwappingTest extends PageSwappingTest
{
    public MuninnPageSwappingTest( Factory<PageSwapperFactory> fixture )
    {
        super( fixture );
    }

    @Override
    protected Page createPage( int cachePageSize )
    {
        MemoryReleaser memoryReleaser = new MemoryReleaser( 1 );
        MuninnPage page = new MuninnPage( cachePageSize, memoryReleaser );
        long stamp = page.writeLock();
        try
        {
            page.initBuffer();
        }
        finally
        {
            page.unlockWrite( stamp );
        }
        return page;
    }

    @Override
    public long writeLock( Page page )
    {
        return ((MuninnPage) page).writeLock();
    }

    @Override
    public void unlockWrite( Page page, long stamp )
    {
        ((MuninnPage) page).unlockWrite( stamp );
    }

    @Override
    protected long getLong( Page page, int offset )
    {
        return ((MuninnPage) page).getLong( offset );
    }

    @Override
    protected int getInt( Page page, int offset )
    {
        return ((MuninnPage) page).getInt( offset );
    }

    @Override
    protected void putLong( Page page, long value, int offset )
    {
        ((MuninnPage) page).putLong( value, offset );
    }

    @Override
    protected void putInt( Page page, int value, int offset )
    {
        ((MuninnPage) page).putInt( value, offset );
    }
}
