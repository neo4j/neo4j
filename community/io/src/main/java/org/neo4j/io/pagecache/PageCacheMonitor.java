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
package org.neo4j.io.pagecache;

public interface PageCacheMonitor
{
    public static final PageCacheMonitor NULL = new PageCacheMonitor()
    {
        @Override
        public void pageFaulted(long filePageId, PageSwapper swapper)
        {
        }

        @Override
        public void evicted(long filePageId, PageSwapper swapper)
        {
        }

        @Override
        public void pinned(boolean exclusiveLock, long filePageId, PageSwapper swapper)
        {
        }

        @Override
        public void unpinned(boolean exclusiveLock, long filePageId, PageSwapper swapper)
        {
        }

        @Override
        public void flushed(long filePageId, PageSwapper swapper)
        {
        }
    };

    /** A page not in the cache was loaded */
    void pageFaulted(long filePageId, PageSwapper swapper);

    /** A page was evicted. */
    void evicted(long filePageId, PageSwapper swapper);

    /** A page is pinned */
    void pinned(boolean exclusiveLock, long filePageId, PageSwapper swapper);

    /** A page is unpinned */
    void unpinned(boolean exclusiveLock, long filePageId, PageSwapper swapper);

    /** A page is flushed to the mapped file */
    void flushed(long filePageId, PageSwapper swapper);
}
